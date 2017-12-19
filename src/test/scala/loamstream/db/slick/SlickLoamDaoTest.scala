package loamstream.db.slick

import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant

import org.scalatest.FunSuite

import loamstream.model.execute.Environment
import loamstream.model.execute.GoogleSettings
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.ProvidesEnvAndResources
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.execute.UgerSettings
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.Output.PathOutput
import loamstream.model.jobs.OutputRecord
import loamstream.model.jobs.OutputStreams
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.uger.Queue
import loamstream.util.Hashes
import loamstream.TestHelpers

/**
 * @author clint
 *         kyuksel
 *         date: 8/9/16
 */
final class SlickLoamDaoTest extends FunSuite with ProvidesSlickLoamDao with ProvidesEnvAndResources {
  private val path0 = Paths.get("src/test/resources/for-hashing/foo.txt")
  private val path1 = Paths.get("src/test/resources/for-hashing/bigger")
  private val path2 = Paths.get("src/test/resources/for-hashing/empty.txt")
  private val path3 = Paths.get("src/test/resources/blarg")

  private lazy val hash0 = Hashes.sha1(path0)
  private lazy val hash1 = Hashes.sha1(path1)
  private lazy val hash2 = Hashes.sha1(path2)

  private val cmd0 = "(echo 10 ; sed '1d' ancestry.txt | cut -f5- | sed 's/\t/ /g') > ancestry.fet"
  private val cmd1 = "R --vanilla --args pca_scores.tsv < plot_pca.r"

  private def noOutputs: Boolean = dao.allOutputRecords.isEmpty
  
  private def noExecutions: Boolean = dao.allExecutions.isEmpty

  import TestHelpers.dummyOutputStreams
  
  private def store(cmd: String, paths: Path*): Execution = {
    val outputs = paths.map { path =>
      val hash = Hashes.sha1(path)
      
      cachedOutput(path, hash)
    }
    
    val execution = {
      Execution(
          env = mockEnv, 
          cmd = Option(cmd), 
          result = JobResult.CommandResult(0),
          outputStreams = dummyOutputStreams,
          outputs = outputs.toSet)
    }
    
    dao.insertExecutions(execution)
    
    execution
  }
  
  private def allExecutionRows: Seq[ExecutionRow] = {
    val driver = dao.descriptor.dbType.driver

    import driver.api._
    
    dao.runBlocking(dao.tables.executions.result) 
  }

  private def store(paths: Path*): Execution = {
    store(mockCmd, paths:_*)
  }
  
  private def storeFailures(paths: Path*): Unit = {
    val outputs = paths.map(OutputRecord(_))
    
    //NB: Failure
    val result = JobResult.CommandResult(42)
    
    assert(result.isFailure)
    
    val execution = Execution(mockEnv, Option(mockCmd), result, dummyOutputStreams, outputs.toSet)
    
    dao.insertExecutions(execution)
  }

  test("insert/allExecutions") {
    createTablesAndThen {
      val stored = store(path0)
      
      assert(stored.outputs.nonEmpty)
      
      val Seq(retrieved) = dao.allExecutions
      
      assert(dao.allOutputRecords.toSet === stored.outputs)

      // TODO Stop ignoring the ID field
      assert(stored.withId(None) === retrieved.withId(None))
    }
  }

  test("insert/Read Outputs") {
    createTablesAndThen {
      assert(noOutputs)
      
      store(path0)
      
      val expected =  Seq(cachedOutput(path0, hash0))
      
      assert(dao.allOutputRecords === expected)
    }
  }

  test("insert/allOutputs") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1, path2)

      val expected = Set(cachedOutput(path0, hash0), cachedOutput(path1, hash1), cachedOutput(path2, hash2))

      //Use Sets to ignore order
      assert(dao.allOutputRecords.toSet == expected)
    }
  }
  
  test("insert/allOutputs - only failures") {
    createTablesAndThen {

      assert(noOutputs)

      storeFailures(path0, path1, path2)

      val expected = Set(OutputRecord(path0), OutputRecord(path1), OutputRecord(path2))

      //Use Sets to ignore order
      assert(dao.allOutputRecords.toSet == expected)
      assert(dao.allOutputRecords.forall(dao.findExecution(_).get.isFailure))
    }
  }
  
  test("insert/allOutputs - some failures") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1)
      storeFailures(path2, path3)

      val failedOutputs = Set(OutputRecord(path2), OutputRecord(path3))
      val hashedOutputs = Set(cachedOutput(path0, hash0), cachedOutput(path1, hash1))

      val all: Set[OutputRecord] = failedOutputs ++ hashedOutputs
      
      //Use Sets to ignore order
      assert(dao.allOutputRecords.toSet == all)
      assert(dao.allOutputRecords.count(dao.findExecution(_).get.isFailure) == failedOutputs.size)
    }
  }

  test("delete all Outputs") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1, path2)

      assert(dao.allOutputRecords.size == 3)

      dao.deletePathOutput(path0, path1, path2)

      assert(noOutputs)
    }
  }
  
  test("delete some Outputs") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1, path2)

      assert(dao.allOutputRecords.size == 3)

      dao.deletePathOutput(path0, path2)

      assert(dao.allOutputRecords.map(_.loc) == Seq(path1.toAbsolutePath.toString))
      
      dao.deletePathOutput(path0, path1)
      
      assert(noOutputs)
    }
  }

  test("insertExecutionRow") {
    import Helpers.dummyId

    createTablesAndThen {
      assert(noOutputs)
      assert(noExecutions)

      val outputStreams = dummyOutputStreams
      
      val toInsert = new ExecutionRow(
          dummyId, 
          mockEnv.toString, 
          mockCmd, 
          mockStatus, 
          mockExitCode, 
          outputStreams.stdout.toString, 
          outputStreams.stderr.toString)
      
      assert(allExecutionRows.isEmpty)
      
      val insertAction = dao.insertExecutionRow(toInsert)
      
      dao.runBlocking(insertAction)

      val Seq(recorded) = allExecutionRows
      
      // The DB must assign an auto-incremented 'id' upon insertion
      assert(recorded.id != dummyId)
      assert(recorded.env === mockEnv.toString)
      assert(recorded.cmd === mockCmd)
      assert(recorded.status === mockStatus)
      assert(recorded.exitCode === mockExitCode)
      assert(recorded.stdoutPath === outputStreams.stdout.toString)
      assert(recorded.stderrPath === outputStreams.stderr.toString)
    }
  }

  test("insertExecutions/allExecutionRows") {
    createTablesAndThen {
      val output0 = cachedOutput(path0, hash0)
      val output1 = cachedOutput(path1, hash1)
      val output2 = cachedOutput(path2, hash2)
      
      val localSettings = LocalSettings
      val ugerSettings = UgerSettings(Cpus(8), Memory.inGb(4))
      val googleSettings = GoogleSettings("some-cluster")
      
      val localEnv: Environment = Environment.Local
      val ugerEnv: Environment = Environment.Uger(ugerSettings)

      val localResources = LocalResources(Instant.ofEpochMilli(123), Instant.ofEpochMilli(456))
      
      val ugerResources = UgerResources(
          Memory.inGb(2.1), 
          CpuTime.inSeconds(12.34), 
          Some("nodeName"), 
          Some(Queue.Broad),
          Instant.ofEpochMilli(64532), 
          Instant.ofEpochMilli(9345345))
        
      val googleResources = GoogleResources("clusterName",
        Instant.ofEpochMilli(1), Instant.ofEpochMilli(72345))

      val failed0 = Execution(localEnv, Option(mockCmd), CommandResult(42), dummyOutputStreams, Set(output0))
                              
      val failed1 = Execution(ugerEnv, Option(mockCmd), CommandResult(1), dummyOutputStreams, Set.empty[OutputRecord])
                              
      val succeeded = Execution(
          Environment.Google(googleSettings), 
          Option(mockCmd), 
          CommandResult(0), 
          dummyOutputStreams, 
          Set(output1, output2))

      assert(failed0.isFailure)
      assert(failed1.isFailure)
      assert(succeeded.isSuccess)
      
      assert(noOutputs)
      assert(noExecutions)

      dao.insertExecutions(failed0)

      val expected0 = {
        Execution(localEnv, mockCmd, CommandResult(42), failed0.outputStreams.get, OutputRecord(output0.loc))
      }

      assertEqualFieldsFor(dao.allExecutions.toSet, Set(expected0))

      dao.insertExecutions(succeeded, failed1)
      val expected1 = failed1
      val expected2 = succeeded
      assertEqualFieldsFor(dao.allExecutions.toSet, Set(expected0, expected1, expected2))
    }
  }
  
  test("insertExecutions - CommandInvocationFailure") {
    createTablesAndThen {
      val output0 = PathOutput(path0)
      
      val failed = Execution(
          mockEnv, 
          mockCmd, 
          JobResult.CommandInvocationFailure(new Exception),
          dummyOutputStreams,
          output0.toOutputRecord)

      assert(failed.isFailure)
      
      assert(noOutputs)
      assert(noExecutions)

      dao.insertExecutions(failed)
      
      val expected0 = Execution(
          mockEnv, 
          mockCmd,
          CommandResult(JobResult.DummyExitCode), 
          failed.outputStreams.get,
          failedOutput(path0))

      assertEqualFieldsFor(dao.allExecutions.toSet, Set(expected0))
    }
  }
  
  test("insertExecutions - should throw") {
    def doTest(command: Option[String], jobResult: JobResult): Unit = {
      createTablesAndThen {
        val output0 = PathOutput(path0)
        
        val failed = Execution(mockEnv, command, JobResult.Failure, dummyOutputStreams, Set(output0.toOutputRecord))
  
        assert(failed.isFailure)
        
        assert(noOutputs)
        assert(noExecutions)
  
        intercept[Exception] {
          dao.insertExecutions(failed)
        }
        
        assert(noOutputs)
        assert(noExecutions)
      }
    }
    
    doTest(Some(mockCmd), JobResult.Failure)
    doTest(None, JobResult.Failure)
    doTest(Some(mockCmd), JobResult.CommandResult(1))
    doTest(Some(mockCmd), JobResult.CommandResult(1))
    doTest(None, JobResult.CommandResult(0))
  }
  
  test("findExecution") {
    createTablesAndThen {
      val output0 = cachedOutput(path0, hash0)
      val output1 = cachedOutput(path1, hash1)
      val output2 = cachedOutput(path2, hash2)
      
      val ex0 = Execution(mockEnv, mockCmd, CommandResult(42), dummyOutputStreams, output0)
      val ex1 = Execution(mockEnv, mockCmd, CommandResult(0), dummyOutputStreams, output1, output2)
      val ex2 = Execution(mockEnv, Option(mockCmd), CommandResult(1), dummyOutputStreams, Set.empty[OutputRecord])
      
      assert(ex0.isFailure)
      assert(ex1.isSuccess)
      assert(ex2.isFailure)
      assert(noOutputs)
      assert(noExecutions)

      dao.insertExecutions(ex0)
      
      val expected0 = Execution(mockEnv, mockCmd, CommandResult(42), ex0.outputStreams.get, failedOutput(path0))

      assertEqualFieldsFor(dao.allExecutions.toSet, Set(expected0))
      assertEqualFieldsFor(dao.findExecution(output0), Some(expected0))

      assert(dao.findExecution(output1) === None)
      assert(dao.findExecution(output2) === None)
      
      dao.insertExecutions(ex1, ex2)
      
      val expected1 = ex1
      val expected2 = ex2

      assertEqualFieldsFor(dao.allExecutions.toSet, Set(expected0, expected1, expected2))

      assertEqualFieldsFor(dao.findExecution(output0), Some(expected0))
      assertEqualFieldsFor(dao.findExecution(output1), Some(expected1))
      assertEqualFieldsFor(dao.findExecution(output2), Some(expected1))
    }
  }
  
  test("findOutput") {
    createTablesAndThen {
      assert(noOutputs)
      
      store(path0)
      
      assert(dao.findOutputRecord(PathOutput(path0).toOutputRecord) === Some(cachedOutput(path0, hash0)))
      
      assert(dao.findOutputRecord(path1) === None)
      
      storeFailures(path1)
      
      assert(dao.findOutputRecord(path0) === Some(cachedOutput(path0, hash0)))
      
      assert(dao.findOutputRecord(path1) === Some(failedOutput(path1)))
    }
  }

  test("findCommand") {
    createTablesAndThen {
      assert(noOutputs)

      store(cmd0, path0)

      assert(dao.findCommand(path0) === Some(cmd0))

      assert(dao.findCommand(path1) === None)

      store(cmd1, path1, path2)

      assert(dao.findCommand(path0) === Some(cmd0))

      assert(dao.findCommand(path1) === Some(cmd1))
      assert(dao.findCommand(path2) === Some(cmd1))
    }
  }
}
