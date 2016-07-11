package loamstream.uger

import java.nio.file.Path

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.execute.ChunkRunner
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.LJob.SimpleFailure
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.Futures
import loamstream.util.Loggable
import monix.execution.Scheduler
import loamstream.model.jobs.commandline.CommandLineBuilderJob
import loamstream.util.Files
import loamstream.conf.ImputationConfig
import loamstream.conf.UgerConfig
import java.util.UUID

/**
 * @author clint
 * date: Jul 1, 2016
 * 
 * A ChunkRunner that runs groups of command line jobs as UGER task arrays, via the provided DrmaaClient.
 * 
 * TODO: Make logging more fine-grained; right now, too much is at info level.
 */
final case class UgerChunkRunner(
    ugerConfig: UgerConfig,
    drmaaClient: DrmaaClient,
    pollingFrequencyInHz: Double = 1.0) extends ChunkRunner with Loggable {

  import UgerChunkRunner._

  override def run(leaves: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, Result]] = {

    require(
      leaves.forall(isCommandLineJob),
      s"For now, we only know how to run ${classOf[CommandLineJob].getSimpleName}s on UGER")

    val leafCommandLineJobs = leaves.toSeq.collect { case clj: CommandLineJob => clj }

    val ugerScript = createScriptFile(ScriptBuilder.buildFrom(leafCommandLineJobs))

    info(s"Made script '$ugerScript' from $leafCommandLineJobs")
    
    val ugerLogFile: Path = ugerConfig.ugerLogFile

    //TODO: do we need this?  Should it be something better?
    val jobName: String = s"LoamStream-${UUID.randomUUID}"

    val submissionResult = drmaaClient.submitJob(ugerScript, ugerLogFile, jobName)

    submissionResult match {
      case DrmaaClient.SubmissionSuccess(rawJobIds) => {
        import monix.execution.Scheduler.Implicits.global

        toResultMap(drmaaClient, leafCommandLineJobs, rawJobIds)
      }
      case DrmaaClient.SubmissionFailure(e) => makeAllFailureMap(leafCommandLineJobs, Some(e))
    }
  }

  private[uger] def toResultMap(
      drmaaClient: DrmaaClient, 
      jobs: Seq[LJob], 
      jobIds: Seq[String])(implicit scheduler: Scheduler): Future[Map[LJob, Result]] = {
    
    val jobsById = jobIds.zip(jobs).toMap

    val poller = Poller.drmaa(drmaaClient)

    def statuses(jobId: String) = Jobs.monitor(poller, pollingFrequencyInHz)(jobId)

    val jobsToFutureResults: Iterable[(LJob, Future[Result])] = for {
      jobId <- jobIds
      job = jobsById(jobId)
      futureResult = statuses(jobId).lastL.runAsync.collect { case Some(status) => resultFrom(job, status) }
    } yield {
      job -> futureResult
    }

    Futures.toMap(jobsToFutureResults)
  }
}

object UgerChunkRunner extends Loggable {
  private[uger] def isCommandLineJob(job: LJob): Boolean = job match {
    case clj: CommandLineJob => true
    case _                   => false
  }

  private[uger] def resultFrom(job: LJob, status: JobStatus): LJob.Result = {
    //TODO: Anything better; this was purely expedient
    if (status.isDone) {
      LJob.SimpleSuccess(s"$job")
    } else {
      LJob.SimpleFailure(s"$job")
    }
  }

  private[uger] def makeAllFailureMap(jobs: Seq[LJob], cause: Option[Exception]): Future[Map[LJob, Result]] = {
    val msg = cause match {
      case Some(e) => s"Couldn't submit jobs to UGER: ${e.getMessage}"
      case None    => "Couldn't submit jobs to UGER"
    }

    cause.foreach(e => error(msg, e))

    Future.successful(jobs.map(j => j -> SimpleFailure(msg)).toMap)
  }
  
  private[uger] def createScriptFile(contents: String, file: Path): Path = {
    Files.writeTo(file)(contents)
    
    file
  }
  
  //TODO: Store the script somewhere more permanent, for debugging or review
  private[uger] def createScriptFile(contents: String): Path = createScriptFile(contents, Files.tempFile(".sh"))
}