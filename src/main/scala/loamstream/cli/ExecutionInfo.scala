package loamstream.cli

import loamstream.model.jobs.Execution
import java.nio.file.Path
import loamstream.db.LoamDao
import loamstream.model.jobs.OutputRecord
import java.net.URI

/**
 * @author clint
 * Dec 11, 2017
 */
object ExecutionInfo {
  
  /**
   * Produce an Option wrapping a description of the Execution that produced this output, or None
   * if no such Execution is found. 
   */
  def forOutput(dao: LoamDao)(pathOrUri: Either[Path, URI]): Option[String] = {
    val outputRecord = pathOrUri match {
      case Left(path) => OutputRecord(path)
      case Right(uri) => OutputRecord(uri)
    }
    
    doForOutput(dao)(outputRecord)
  }
  
  /**
   * Produce an Option wrapping a description of the Execution that produced this output, or None
   * if no such Execution is found. 
   */
  private def doForOutput(dao: LoamDao)(output: OutputRecord): Option[String] = {
    val executionOpt = dao.findExecution(output)
    
    executionOpt.map(describe)
  }
  
  private def describe(execution: Execution): String = {
    val commandString = execution.cmd.map(c => s"'$c'")getOrElse("<None>")
  
    val envType = execution.env.tpe
    val envSettings = execution.env.settings
  
    val stdout = execution.outputStreams.map(_.stdout).getOrElse("<Not produced>")
    val stderr = execution.outputStreams.map(_.stderr).getOrElse("<Not produced>")
  
    val numOutputs = execution.outputs.size
  
    val withoutOutputs = s"""#
                             #Command: $commandString
                             #Environment: $envType
                             #Settings: $envSettings
                             #Output streams:
                             #  Stdout: $stdout
                             #  Stderr: $stderr
                             #$numOutputs Output(s):""".stripMargin('#')
      
      
    val outputs = execution.outputs.map(_.toString).mkString("\n  ")

    s"${withoutOutputs}\n  ${outputs}"
  }
}
