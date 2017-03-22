package loamstream.db.slick

import loamstream.model.execute.{ExecutionEnvironment, Resources, Settings}
import loamstream.model.jobs.{Execution, OutputRecord}
import loamstream.model.jobs.JobResult.CommandResult

/**
 * @author clint
 * date: Sep 22, 2016
 */
final case class ExecutionRow(id: Int, env: String, cmd: String, exitStatus: Int) {
  def toExecution(settings: Settings, resources: Option[Resources], outputs: Set[OutputRecord]): Execution = {
    val commandResult = CommandResult(exitStatus, resources)
    
    Execution(ExecutionEnvironment.fromString(env), Option(cmd), settings, commandResult, outputs)
  }
}
