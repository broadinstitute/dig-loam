package loamstream.uger

import java.io.File
import java.nio.file.Path
import java.util.UUID

import loamstream.conf.UgerConfig
import loamstream.model.execute.ChunkRunnerFor
import loamstream.model.execute.{ExecutionEnvironment => ExecEnv}
import loamstream.model.jobs.{JobResult, LJob, NoOpJob}
import loamstream.model.jobs.JobResult.Failed
import loamstream.model.jobs.JobResult.Running
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.uger.UgerStatus.toJobResult
import loamstream.util.Files
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.Terminable
import loamstream.util.TimeUtils.time
import rx.lang.scala.Observable


/**
 * @author clint
 *         date: Jul 1, 2016
 *
 *         A ChunkRunner that runs groups of command line jobs as UGER task arrays, via the provided DrmaaClient.
 *
 *         TODO: Make logging more fine-grained; right now, too much is at info level.
 */
final case class UgerChunkRunner(
    ugerConfig: UgerConfig,
    drmaaClient: DrmaaClient,
    jobMonitor: JobMonitor,
    pollingFrequencyInHz: Double = 1.0) extends ChunkRunnerFor(ExecEnv.Uger) with Terminable with Loggable {

  import UgerChunkRunner._

  override def stop(): Unit = jobMonitor.stop()
  
  override def maxNumJobs = ugerConfig.maxNumJobs

  override def run(leaves: Set[LJob]): Observable[Map[LJob, JobResult]] = {

    debug(s"Running: ")
    leaves.foreach(job => debug(s"  $job"))

    require(
      leaves.forall(isAcceptableJob),
      s"For now, we only know how to run ${classOf[CommandLineJob].getSimpleName}s on UGER")

    // Filter out NoOpJob's
    val commandLineJobs = leaves.toSeq.filterNot(isNoOpJob).collect { case clj: CommandLineJob => clj }

    if (commandLineJobs.nonEmpty) {
      val ugerScript = writeUgerScriptFile(commandLineJobs)

      //TODO: do we need this?  Should it be something better?
      val jobName: String = s"LoamStream-${UUID.randomUUID}"

      val submissionResult = drmaaClient.submitJob(ugerConfig, ugerScript, jobName, commandLineJobs.size)

      toJobStateStream(commandLineJobs, submissionResult)
    } else {
      // Handle NoOp case or a case when no jobs were presented for some reason
      Observable.just(Map.empty)
    }
  }
  
  private def toJobStateStream(
      commandLineJobs: Seq[CommandLineJob], 
      submissionResult: DrmaaClient.SubmissionResult): Observable[Map[LJob, JobResult]] = submissionResult match {

    case DrmaaClient.SubmissionSuccess(rawJobIds) => {
      commandLineJobs.foreach(_.updateAndEmitJobState(Running))

      val jobsById = rawJobIds.zip(commandLineJobs).toMap

      toResultMap(jobsById)
    }
    case DrmaaClient.SubmissionFailure(e) => {
      commandLineJobs.foreach(_.updateAndEmitJobState(Failed()))
      
      makeAllFailureMap(commandLineJobs, Some(e))
    }
  }
  
  private[uger] def toResultMap(jobsById: Map[String, CommandLineJob]): Observable[Map[LJob, JobResult]] = {

    def statuses(jobIds: Iterable[String]) = time(s"Calling Jobs.monitor(${jobIds.mkString(",")})", trace(_)) {
      jobMonitor.monitor(jobIds)
    }

    val jobsAndStatusesById = combine(jobsById, statuses(jobsById.keys))

    val jobsToResultObservables: Iterable[(LJob, Observable[JobResult])] = for {
      (jobId, (job, jobStatuses)) <- jobsAndStatusesById
      _ = jobStatuses.foreach(status => job.updateAndEmitJobState(toJobResult(status)))
      resultObs = jobStatuses.last.map(toJobResult)
    } yield {
      job -> resultObs
    }

    Observables.toMap(jobsToResultObservables)
  }
  
  private def writeUgerScriptFile(commandLineJobs: Seq[CommandLineJob]): Path = {
    val ugerWorkDir = ugerConfig.workDir.toFile
    
    val ugerScript = createScriptFile(ScriptBuilder.buildFrom(commandLineJobs), ugerWorkDir)
    
    info(s"Made script '$ugerScript' from $commandLineJobs")
    
    ugerScript
  }
}

object UgerChunkRunner extends Loggable {
  private[uger] def isCommandLineJob(job: LJob): Boolean = job match {
    case clj: CommandLineJob => true
    case _                   => false
  }

  private[uger] def isNoOpJob(job: LJob): Boolean = job match {
    case noj: NoOpJob => true
    case _            => false
  }

  private[uger] def isAcceptableJob(job: LJob): Boolean = isNoOpJob(job) || isCommandLineJob(job)

  private[uger] def makeAllFailureMap(jobs: Seq[LJob], cause: Option[Exception]): Observable[Map[LJob, JobResult]] = {
    val failure: JobResult = cause match {
      case Some(e) => JobResult.FailedWithException(e)
      case None    => JobResult.Failed()
    }

    cause.foreach(e => error(s"Couldn't submit jobs to UGER: ${e.getMessage}", e))

    import loamstream.util.Traversables.Implicits._

    Observable.just(jobs.mapTo(_ => failure))
  }

  private[uger] def createScriptFile(contents: String, file: Path): Path = {
    Files.writeTo(file)(contents)

    file
  }

  /**
   * Creates a script file in the *default temporary-file directory*, using
   * the given prefix and suffix to generate its name.
   */
  private[uger] def createScriptFile(contents: String): Path = createScriptFile(contents, Files.tempFile(".sh"))

  /**
   * Creates a script file in the *specified* directory, using
   * the given prefix and suffix to generate its name.
   */
  private[uger] def createScriptFile(contents: String, directory: File): Path = {
    createScriptFile(contents, Files.tempFile(".sh", directory))
  }

  private[uger] def combine[A, U, V](m1: Map[A, U], m2: Map[A, V]): Map[A, (U, V)] = {
    Map.empty[A, (U, V)] ++ (for {
      (a, u) <- m1.toIterable
      v <- m2.get(a)
    } yield {
      a -> (u -> v)
    })
  }
}
