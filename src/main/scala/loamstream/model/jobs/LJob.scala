package loamstream.model.jobs

import java.nio.file.Path

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.execute.ExecutionEnvironment
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.ValueBox
import rx.lang.scala.Observable
import rx.lang.scala.Subject
import rx.lang.scala.subjects.ReplaySubject

/**
 * @author oliverr
 *         clint
 *         kyuksel
 * date: Dec 23, 2015
 */
trait LJob extends Loggable {
  def executionEnvironment: ExecutionEnvironment
  
  def workDirOpt: Option[Path] = None
  
  def runCount: Int = runCountRef()
  
  private[this] val runCountRef: ValueBox[Int] = ValueBox(0)
  
  private def incrementRunCount(): Int = runCountRef.getAndUpdate { oldCount =>
    val incremented = oldCount + 1
    
    (incremented, incremented) 
  }
  
  def print(indent: Int = 0, doPrint: String => Unit = debug(_), header: Option[String] = None): Unit = {
    val indentString = s"${"-" * indent} >"

    header.foreach(doPrint)
    
    doPrint(s"$indentString ($status)${this}")

    inputs.foreach(_.print(indent + 2, doPrint))
  }

  def name: String = ""

  /** Any jobs this job depends on */
  def inputs: Set[LJob]

  /** Any outputs produced by this job */
  def outputs: Set[Output]

  /**
   * An observable producing a stream of all the runnable jobs among this job, its dependencies, their dependencies,
   * and so on, as soon as those jobs become runnable.  A job becomes runnable when all its dependencies are finished,
   * or if it has no dependencies, it's runnable immediately.  (See selfRunnable)
   */
  final lazy val runnables: Observable[LJob] = {

    //Multiplex the streams of runnable jobs starting from each of our dependencies
    val dependencyRunnables = {
      if(inputs.isEmpty) { Observable.empty }
      //NB: Note the use of merge instead of ++; this ensures that we don't emit jobs from the sub-graph rooted at
      //one dependency before the other dependencies, but rather emit all the streams of runnable jobs "together".
      else { Observables.merge(inputs.toSeq.map(_.runnables)) }
    }

    //Emit the current job *after* all our dependencies
    (dependencyRunnables ++ selfRunnable)
  }

  /**
   * An observable that will emit this job ONLY when all this job's dependencies are finished.
   * If the this job has no dependencies, this job is emitted immediately.  This will fire at most once.
   */
  private[jobs] lazy val selfRunnable: Observable[LJob] = {
    //lazy val failures = statuses.filter(_.isFailure).takeUntil(_.isTerminal)
    lazy val failures = statuses.filter(s => s.isFailure && !s.isTerminal)
    
    //def justUs = Observable.just(this)
    //NB: We run once, and then once per failure, until we fail permanently; ChunkRunners are responsible for
    //setting our status to PermanentFailure if they will no longer restart us.
    def justUs = Observable.just(this) ++ failures.map(_ => this)
    def noMore = Observable.empty

    if(inputs.isEmpty) {
      justUs
    } else {
      for {
        inputStatuses <- finalInputStatuses
        _ = debug(s"'$name'.selfRunnable: deps finished with statuses: $inputStatuses")
        anyInputFailures = inputStatuses.exists(_.isFailure)
        runnable <- if(anyInputFailures) noMore else justUs
      } yield {
        runnable
      }
    }
  }

  private[this] val statusRef: ValueBox[JobStatus] = ValueBox(JobStatus.NotStarted)

  /** This job's current status */
  final def status: JobStatus = statusRef.value

  /**
   * An observable stream of statuses emitted by this job, each one reflecting a status this job transitioned to.
   */
  //NB: Needs to be a ReplaySubject for correct operation
  private[this] val statusEmitter: Subject[JobStatus] = ReplaySubject[JobStatus]()

  lazy val statuses: Observable[JobStatus] = statusEmitter

  final protected def emitJobStatus(): Unit = statusEmitter.onNext(status)

  /**
   * The "terminal" status emitted by this job: the one that indicates the job is finished for any reason.
   * Will fire at most one time.
   */
  protected[jobs] lazy val lastStatus: Observable[JobStatus] = statuses.filter(_.isTerminal).first

  /**
   * An observable that will emit a sequence containing all our dependencies' "terminal" statuses.
   * When this fires, our dependencies are finished.
   */
  protected[jobs] lazy val finalInputStatuses: Observable[Seq[JobStatus]] = {
    Observables.sequence(inputs.toSeq.map(_.lastStatus))
  }
  
  /**
   * Sets the status of this job to be newStatus, and emits the new status to any observers.
   *
   * @param newStatus the new status to set for this job
   */
  //TODO: TEST
  final def transitionTo(newStatus: JobStatus): Unit = {
    debug(s"Status change to $newStatus for job: ${this}")
    
    val newRunCount = statusRef.mutate { oldStatus =>

      //TODO: BEWARE: Side effect, overlapping locks
      incrementRunCountIfNeeded(oldStatus, newStatus)
      
      newStatus
    }
    
    statusEmitter.onNext(newStatus)
    
    if(newStatus.isTerminal) {
      debug(s"$newStatus is terminal; emitting no more statuses from job: ${this}")
      
      statusEmitter.onCompleted()
    }
  }
  
  //TODO: TEST
  private[jobs] def incrementRunCountIfNeeded(oldStatus: JobStatus, newStatus: JobStatus): Int = {
    import JobStatus._
    
    runCountRef.getAndUpdate { oldRunCount =>
      val startedRunning = newStatus.isRunning && oldStatus.notRunning
      
      val newCount = if(startedRunning) oldRunCount + 1 else oldRunCount
      
      (newCount, newCount)
    }
  }

  /**
   * Implementions of this method will do any actual work to be performed by this job
   */
  def execute(implicit context: ExecutionContext): Future[Execution]

  protected def doWithInputs(newInputs: Set[LJob]): LJob

  final def withInputs(newInputs: Set[LJob]): LJob = {
    if (inputs eq newInputs) { this }
    else { doWithInputs(newInputs) }
  }
}
