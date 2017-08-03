package loamstream.model.execute

import java.nio.file.Path

import loamstream.db.LoamDao
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.{Execution, LJob, OutputRecord}
import loamstream.util.Loggable

/**
 * @author clint
 *         kyuksel
 * date: Sep 30, 2016
 */
final class DbBackedJobFilter(val dao: LoamDao) extends JobFilter with Loggable {
  override def shouldRun(job: LJob): Boolean = {
    lazy val noOutputs = job.outputs.isEmpty

    def anyOutputNeedsToBeRun = job.outputs.exists(o => needsToBeRun(job.toString, o.toOutputRecord))

    if (noOutputs) { debug(s"Job $job will be run because it has no known outputs.") }

    noOutputs || anyOutputNeedsToBeRun || hasDistinctCommand(job)
  }

  override def record(executions: Iterable[Execution]): Unit = {
    //NB: We can only insert command executions (UGER or command-line jobs, anything with an in exit status code)
    //for now
    val insertableExecutions = executions.filter(_.isCommandExecution)

    debug(s"RECORDING $insertableExecutions")

    dao.insertExecutions(insertableExecutions)
  }

  // If performance becomes an issue, not 'findOutput()'ing multiple times
  // for a given OutputRecord should help
  private[execute] def needsToBeRun(jobStr: String, rec: OutputRecord): Boolean = {
    val msg = s"Job $jobStr will be run because its output"

    lazy val missing = rec.isMissing
    lazy val older = isOlder(rec)
    lazy val noHash = notHashed(rec)
    lazy val differentHash = hasDifferentHash(rec)

    if (missing) { debug(s"$msg $rec is missing.") }
    else if (older) { debug(s"$msg $rec is older.") }
    else if (noHash) { debug(s"$msg $rec does not have a hash value.") }
    else if (differentHash) { debug(s"$msg $rec has a different hash.") }

    missing || older || noHash || differentHash
  }

  private def normalize(p: Path) = p.toAbsolutePath

  private def findOutput(loc: String): Option[OutputRecord] = {
    dao.findOutputRecord(loc)
  }
  private def findCommand(loc: String): Option[String] = {
    dao.findCommand(loc)
  }


  private def isHashed(rec: OutputRecord): Boolean = {
    findOutput(rec.loc) match {
      case Some(matchingRec) => matchingRec.isHashed
      case None => false
    }
  }

  private def notHashed(rec: OutputRecord): Boolean = !isHashed(rec)

  private[execute] def hasDifferentHash(rec: OutputRecord): Boolean = {
    findOutput(rec.loc) match {
      case Some(matchingRec) => matchingRec.hasDifferentHashThan(rec)
      case None => false
    }
  }

  private[execute] def isOlder(currentRec: OutputRecord): Boolean = {
    findOutput(currentRec.loc) match {
      case Some(matchingRec) => currentRec.isOlderThan(matchingRec)
      case None => false
    }
  }

  private[execute] def hasDistinctCommand(job: LJob): Boolean = !hasSameCommand(job)

  private[execute] def hasSameCommand(job: LJob): Boolean = {
    isCommandLineJob(job) && hasAtLeastOneOutput(job) && matchesRecordedCommand(job)
  }

  /**
   * Requires job to be a CommandLineJob with at least one output
   */
  private[execute] def matchesRecordedCommand(job: LJob): Boolean = {
    assert(
      isCommandLineJob(job),
      s"We only know how to look up ${classOf[CommandLineJob].getSimpleName}s but found: $job")

    assert(
      hasAtLeastOneOutput(job),
      s"We only know how to look up commands for jobs with at least one output.")

    val cmdLineJob = job.asInstanceOf[CommandLineJob]
    val outputLocation = cmdLineJob.outputs.head.toOutputRecord.loc
    val cmd = cmdLineJob.commandLineString

    val recordedCommandOpt = findCommand(outputLocation)

    recordedCommandOpt.contains(cmd)
  }

  private[execute] def isCommandLineJob(job: LJob): Boolean = job match {
    case j: CommandLineJob => true
    case _ => false
  }

  private[execute] def hasAtLeastOneOutput(job: LJob): Boolean = job.outputs.nonEmpty
}
