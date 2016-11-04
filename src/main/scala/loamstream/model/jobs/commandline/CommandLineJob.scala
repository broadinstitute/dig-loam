package loamstream.model.jobs.commandline

import java.nio.file.{Path, Files => JFiles}

import loamstream.model.jobs.{JobState, LJob}
import loamstream.util.{Futures, Loggable}

import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.{ProcessBuilder, ProcessLogger}

/**
  * LoamStream
  * Created by oliverr on 6/17/2016.
  */

/** A job based on a command line definition */
trait CommandLineJob extends LJob {
  def workDir: Path

  override def workDirOpt: Option[Path] = Some(workDir)

  def processBuilder: ProcessBuilder

  def commandLineString: String

  def logger: ProcessLogger = CommandLineJob.stdErrProcessLogger

  def exitValueCheck: Int => Boolean

  def exitValueIsOk(exitValue: Int): Boolean = exitValueCheck(exitValue)

  override protected def executeSelf(implicit context: ExecutionContext): Future[JobState] = {
    Futures.runBlocking {
      trace(s"RUNNING: $commandLineString")
      JFiles.createDirectories(workDir)
      val exitValue = processBuilder.run(logger).exitValue

      if (exitValueIsOk(exitValue)) {
        trace(s"SUCCEEDED: $commandLineString")
      } else {
        trace(s"FAILED: $commandLineString")
      }

      JobState.CommandResult(exitValue)
    }.recover {
      case exception: Exception => JobState.CommandInvocationFailure(exception)
    }
  }

  override def toString: String = s"'$commandLineString'"
}

object CommandLineJob extends Loggable {

  val mustBeZero: Int => Boolean = _ == 0
  val acceptAll: Int => Boolean = i => true

  val defaultExitValueChecker = mustBeZero

  val noOpProcessLogger = ProcessLogger(line => ())
  val stdErrProcessLogger = ProcessLogger(line => (), line => trace(line))

}
