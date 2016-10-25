package loamstream.db

import java.nio.file.Path

import loamstream.model.jobs.Execution
import loamstream.model.jobs.Output
import loamstream.model.jobs.Output.CachedOutput
import loamstream.model.jobs.Output.PathOutput

/**
 * @author clint
 * date: Aug 4, 2016
 */
trait LoamDao {
  
  def findOutput(path: Path): Option[Output.PathBased]
  def findHashedOutput(path: Path): Option[CachedOutput]
  def findFailedOutput(path: Path): Option[PathOutput]
  
  final def deleteOutput(path: Path, others: Path*): Unit = deleteOutput(path +: others)
  def deleteOutput(paths: Iterable[Path]): Unit
  
  def allOutputs: Seq[Output.PathBased]
  def allHashedOutputs: Seq[CachedOutput]
  def allFailedOutputs: Seq[PathOutput]
  
  final def insertExecutions(execution: Execution, others: Execution*): Unit = insertExecutions(execution +: others)
  
  def insertExecutions(rows: Iterable[Execution]): Unit
  
  def allExecutions: Seq[Execution]
  
  def findExecution(output: Output.PathBased): Option[Execution]
  
  def createTables(): Unit
  
  def dropTables(): Unit
  
  def shutdown(): Unit
}