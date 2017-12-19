package loamstream.model.jobs

import loamstream.util.ValueBox
import rx.lang.scala.Subject
import rx.lang.scala.subjects.ReplaySubject
import rx.lang.scala.Observable
import loamstream.util.Loggable
import loamstream.util.Observables
import rx.lang.scala.observables.ConnectableObservable
import rx.lang.scala.subjects.BehaviorSubject
import rx.lang.scala.subjects.SerializedSubject
import loamstream.model.jobs.log.JobLog

/**
 * @author oliverr
 *         clint
 *         kyuksel
 * Date (as LJob): Dec 23, 2015
 * Factored out: Nov 14, 2017
 * 
 * NB: Note liberal use of shareReplay (.share.cache) to (dramatically) reduce memory consumption.  Without 
 * this, RxJava/RxScala makes one "copy" of any observable that multicasts *per* derived observable.  
 * Since we resursively build Observable streams from trees of jobs, this led to pathological, exponential memory
 * use.
 */
trait JobNode extends Loggable {  
  def job: LJob
  
  /** Any jobs this job depends on */
  def inputs: Set[JobNode]
  
  import JobNode.ObservableOps
  
  private[this] val snapshotRef: ValueBox[JobSnapshot] = ValueBox(JobSnapshot(JobStatus.NotStarted, 0))

  /** This job's 'state' (status and run count) at this instant. */ 
  private def snapshot: JobSnapshot = snapshotRef()
  
  //NB: Needs to be a ReplaySubject for correct operation
  private[this] val runsEmitter: Subject[JobRun] = ReplaySubject()
  
  /** This job's current status */
  final def status: JobStatus = snapshotRef().status
  
  /** The number of times this job has transitioned to `Running` status */
  final def runCount: Int = snapshotRef().runCount
  
  /**
   * An Observable that emits JobRuns of this job.  These are (effectively) tuples of (job, status, runCount), 
   * which allows using .distinct this and derived Observables, solving the problem where multiple jobs with 
   * the same upstream dependency would cause the dependency to be run more than necessary under certain 
   * conditions.
   */
  //NB: Note use of .shareReplay which allows re-using this Observable, 
  //saving lots of memory when running complex pipelines
  private final lazy val runs: Observable[JobRun] = runsEmitter.shareReplay
  
  /**
   * An observable stream of statuses emitted by this job, each one reflecting a status this job transitioned to.
   */
  //NB: Note use of .shareReplay which allows re-using this Observable, 
  //saving lots of memory when running complex pipelines
  final lazy val statuses: Observable[JobStatus] = runs.map(_.status).shareReplay

  /**
   * The "terminal" status emitted by this job: the one that indicates the job is finished for any reason.
   * Will fire at most one time.
   */
  //NB: Note use of .shareReplay which allows re-using this Observable, 
  //saving lots of memory when running complex pipelines
  private[jobs] lazy val lastStatus: Observable[JobStatus] = statuses.filter(_.isTerminal).first.shareReplay

  /**
   * An observable that will emit a sequence containing all our dependencies' "terminal" statuses.
   * When this fires, our dependencies are finished.
   */
  //NB: Note use of .shareReplay which allows re-using this Observable, 
  //saving lots of memory when running complex pipelines
  private[jobs] lazy val finalInputStatuses: Observable[Seq[JobStatus]] = {
    Observables.sequence(inputs.toSeq.map(_.lastStatus)).shareReplay
  }
  
  /**
   * An observable that will emit this job when all this job's dependencies are finished, and then once for every
   * time this job fails with a non-Terminal status.
   * If the this job has no dependencies, this job is emitted immediately.  
   */
  private[jobs] lazy val selfRunnables: Observable[JobRun] = {
    def isNonTerminalFailure(jobRun: JobRun): Boolean = jobRun.status.isFailure && !jobRun.status.isTerminal
    
    //NB: We can just filter here, instead of using something like 
    //runs.filter(isFailure).takeUntil(isTerminal)
    //Since transitioning to a terminal status will shut down runsEmitter (aliased as "run") and observables
    //derived from it.
    lazy val nonTerminalFailures = runs.filter(isNonTerminalFailure)
    
    //NB: We run once, and then once per failure, until we transition to a terminal status. 
    //ChunkRunners/Executers are responsible for setting our status to FailedPermanently if they will no 
    //longer run us.
    def selfJobRun = jobRunFrom(snapshot)

    def logMsg(msg: String): String = s"selfRunnables '${job.id}': ${msg} ('${job.name}')" 
    
    def justUs: Observable[JobRun] = {
      trace(logMsg("justUs()"))
      
      Observable.just(selfJobRun) ++ nonTerminalFailures
    }

    //Return an observable that will ONLY produce a JobRun for this job with the status 'FailedPermanently'.
    //NB: As a side effect, transition this job to the state 'FailedPermanently'. :/
    def stopDueToDependencyFailure(): Observable[JobRun] = {
      trace(logMsg("stopDueToDependencyFailure()"))
      
      transitionTo(JobStatus.CouldNotStart)
      
      val couldNotStartSnapshot = snapshot.withStatus(JobStatus.CouldNotStart)
      
      Observable.just(jobRunFrom(couldNotStartSnapshot))
    }

    val result = {
      if(inputs.isEmpty) {
        trace(logMsg("no deps, just us"))
        
        justUs
      } else {
        for {
          inputStatuses <- finalInputStatuses
          _ = trace(logMsg(s"deps finished with statuses: $inputStatuses"))
          anyInputFailures = inputStatuses.exists(_.isFailure)
          runnable <- if(anyInputFailures) stopDueToDependencyFailure() else justUs
        } yield {
          runnable
        }
      }
    }
    //NB: Note use of .shareReplay which allows re-using this Observable, 
    //saving lots of memory when running complex pipelines
    result.shareReplay
  }

  /**
   * An Observable producing a all the runnable jobs among this job, its dependencies, their dependencies,
   * and so on, as soon as those jobs become runnable.  A job becomes runnable when all its dependencies are finished
   * - either successfully or with JobStatus.FailedPermanently - or if it fails (to facilitate restarting).  
   * If a job has no dependencies, it's runnable immediately. (See selfRunnables)
   */
  final lazy val runnables: Observable[JobRun] = {

    //Multiplex the streams of runnable jobs starting from each of our dependencies
    val dependencyRunnables = {
      if(inputs.isEmpty) { Observable.empty }
      //NB: Note the use of merge instead of ++; this ensures that we don't emit jobs from the sub-graph rooted at
      //one dependency before the other dependencies, but rather emit all the streams of runnable jobs "together".
      else { Observables.merge(inputs.toSeq.map(_.runnables)) }
    }

    //Emit the current job *after* all our dependencies
    //NB: Note use of .shareReplay which allows re-using this Observable, 
    //saving lots of memory when running complex pipelines
    (dependencyRunnables ++ selfRunnables).shareReplay
  }
  
  /**
   * Transitions this JobNode to a new status.  If the passed-in status is the same as this JobNode's current
   * status, this method has no effect.  If the passed-in status is different, this method updates this JobNode's
   * status of this job to be newStatus, and emits a JobRun with the new status to any observers.
   * 
   * Also bumps this job's run count if the current status *is not* `Running` and the new status *is* `Running`.
   * If `newStatus` is a terminal status, all Observables derived from this job will be shut down.
   *
   * @param newStatus the new status to set for this job
   */
  final def transitionTo(newStatus: JobStatus): Unit = snapshotRef.foreach { _ =>
    
    val (newSnapshot, isChanged) = snapshotRef.mutateAndGet(_.transitionTo(newStatus))
    
    val newRunCount = newSnapshot.runCount
    
    if(isChanged) {
      debug(s"Status change to $newStatus (run count $newRunCount) for job: $job")
      
      val isRunning = newSnapshot.status.isRunning
      
      val isCouldNotStart = newSnapshot.status.isCouldNotStart
      
      if(isRunning) {
        info(s"Now running: $job")
      }
      
      if(isCouldNotStart) {
        info(s"Could not start due to dependency failures: $job")
      }
      
      val jobRun = jobRunFrom(newSnapshot)

      JobLog.onStatusChange(jobRun)
      
      runsEmitter.onNext(jobRun)
      
      //Shut down all Observables derived from this job when we will no longer emit any events.
      if(newStatus.isTerminal) {
        trace(s"$newStatus is terminal; emitting no more JobRuns from job: $job")
       
        runsEmitter.onCompleted()
      }
    }
  }
  
  private def jobRunFrom(snapshot: JobSnapshot): JobRun = JobRun(this, snapshot.status, snapshot.runCount)
}

object JobNode {
  /** Extension method to allow convenient, memory-efficient-enough multicasting */ 
  private implicit final class ObservableOps[A](val o: Observable[A]) extends AnyVal { 
    def shareReplay: Observable[A] = o.share.cache 
  }
}
