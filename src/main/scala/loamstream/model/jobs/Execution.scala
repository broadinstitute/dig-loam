package loamstream.model.jobs

import loamstream.model.execute.Environment
import loamstream.model.execute.Resources
import loamstream.model.execute.Settings
import loamstream.model.jobs.commandline.CommandLineJob
import java.nio.file.Path

/**
 * @author clint
 *         kyuksel
 * date: Sep 22, 2016
 */
final case class Execution(
    id: Option[Int] = None,
    env: Environment,
    cmd: Option[String] = None,
    status: JobStatus,
    result: Option[JobResult] = None,
    resources: Option[Resources] = None,
    outputs: Set[OutputRecord] = Set.empty,
    outputStreams: Option[OutputStreams]) {

  def isSuccess: Boolean = status.isSuccess
  def isFailure: Boolean = status.isFailure

  def settings: Settings = env.settings
  
  //NB :(
  //We're a command execution if we wrap a CommandResult or CommandInvocationFailure, and a
  //command-line string is defined.
  def isCommandExecution: Boolean = result.exists {
    case _: JobResult.CommandResult | _: JobResult.CommandInvocationFailure => cmd.isDefined
    case _ => false
  }

  def withOutputRecords(newOutputs: Set[OutputRecord]): Execution = copy(outputs = newOutputs)
  def withOutputRecords(newOutput: OutputRecord, others: OutputRecord*): Execution = {
    withOutputRecords((newOutput +: others).toSet)
  }

  def withResources(rs: Resources): Execution = copy(resources = Some(rs))

  def withId(newId: Int): Execution = withId(Option(newId))
  def withId(newId: Option[Int]): Execution = copy(id = newId)
}

object Execution {
  // TODO Remove when dynamic statuses flow in
  def apply(env: Environment,
            cmd: Option[String],
            result: JobResult,
            outputStreams: OutputStreams,
            outputs: Set[OutputRecord]): Execution = {
    
    Execution(
        id = None, 
        env = env, 
        cmd = cmd, 
        status = result.toJobStatus, 
        result = Option(result), 
        resources = None, 
        outputs = outputs,
        outputStreams = Option(outputStreams))
  }

  // TODO Remove when dynamic statuses flow in
  def apply(env: Environment,
            cmd: String,
            result: JobResult,
            outputStreams: OutputStreams,
            outputs: OutputRecord*): Execution = {
    
    Execution(
        id = None, 
        env = env, 
        cmd = Option(cmd), 
        status = result.toJobStatus, 
        result = Option(result), 
        resources = None, 
        outputs = outputs.toSet,
        outputStreams = Option(outputStreams))
  }

  def apply(env: Environment,
            cmd: String,
            status: JobStatus,
            result: JobResult,
            outputStreams: OutputStreams,
            outputs: OutputRecord*): Execution = {
    
    Execution(
        id = None, 
        env = env, 
        cmd = Option(cmd), 
        status = status, 
        result = Option(result), 
        resources = None, 
        outputs = outputs.toSet,
        outputStreams = Option(outputStreams))
  }

  def fromOutputs(env: Environment,
                  cmd: String,
                  result: JobResult,
                  outputStreams: OutputStreams,
                  outputs: Set[Output]): Execution = {
    
    apply(env, Option(cmd), result, outputStreams, outputs.map(_.toOutputRecord))
  }

  def from(job: LJob, jobStatus: JobStatus, outputStreams: OutputStreams): Execution = {
    from(job, jobStatus, result = None, Option(outputStreams))
  }
  
  def from(job: LJob, jobStatus: JobStatus): Execution = {
    from(job, jobStatus, result = None, outputStreams = None)
  }

  def from(
      job: LJob, 
      status: JobStatus, 
      result: Option[JobResult], 
      outputStreams: Option[OutputStreams]): Execution = {
    
    val commandLine: Option[String] = job match {
      case clj: CommandLineJob => Option(clj.commandLineString)
      case _ => None
    }

    // TODO Replace the placeholder for `resources` object put in place to get the code to compile
    Execution(
      id = None,
      env = job.executionEnvironment,
      cmd = commandLine,
      status = status,
      result = result,
      resources = None, // TODO: smell: we have no idea how this job was run
      outputs = job.outputs.map(_.toOutputRecord),
      outputStreams = outputStreams) 
  }
}
