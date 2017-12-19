package loamstream.model.jobs

import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.model.execute.Environment
import loamstream.model.execute.ProvidesEnvAndResources
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.TypeBox


/**
 * @author clint
 * Oct 14, 2015
 */
final class ExecutionTest extends FunSuite with ProvidesEnvAndResources {
  private val p0 = Paths.get("foo/bar/baz")
  private val p1 = Paths.get("nuh")
  
  import TestHelpers.dummyOutputStreams
  
  test("from(LJob, JobStatus, JobResult)") {
    import TestHelpers.path
    
    val result0 = CommandResult(0)
    val status0 = JobStatus.fromExitCode(result0.exitCode)
    val status1 = JobStatus.Succeeded
    
    val job0 = CommandLineJob("foo", path("."), Environment.Uger(TestHelpers.defaultUgerSettings))
    val job1 = MockJob(status1)
    
    val outputStreamsOpt = Some(dummyOutputStreams)
    
    val e0 = Execution.from(job0, status0, Option(result0), outputStreamsOpt)
    val e1 = Execution.from(job1, status1)
    
    assert(e0.cmd === Some("foo"))
    assert(e0.env.isUger)
    assert(e0.result === Some(result0))
    assert(e0.outputs === Set.empty)
    assert(e0.outputStreams === outputStreamsOpt)
    //TODO: Check settings field once it's no longer a placeholder 
    
    assert(e1.cmd === None)
    assert(e1.env.isLocal)
    assert(e1.status === status1)
    assert(e1.result === None)
    assert(e1.outputs === Set.empty)
    assert(e1.outputStreams === None)
    //TODO: Check settings field once it's no longer a placeholder
  }

  test("isCommandExecution") {
    def assertIsCommandExecution(result: JobResult, cmd: Option[String] = Option(mockCmd)): Unit = {
      val execution = Execution(
          env = mockEnv, 
          cmd = cmd, 
          result = result,
          outputStreams = dummyOutputStreams,
          outputs = Set.empty[OutputRecord])
      
      assert(execution.isCommandExecution)
    }
    
    def assertIsNOTCommandExecution(result: JobResult, cmd: Option[String] = Option(mockCmd)): Unit = {
      val execution = Execution(
          env = mockEnv, 
          cmd = cmd, 
          result = result,
          outputStreams = dummyOutputStreams,
          outputs = Set.empty[OutputRecord])
      
      assert(!execution.isCommandExecution)
    }

    val e = new Exception
    
    import JobResult._
    
    assertIsCommandExecution(CommandResult(0))
    assertIsCommandExecution(CommandResult(1))
    assertIsCommandExecution(CommandResult(-1))
    assertIsCommandExecution(CommandResult(42))

    assertIsCommandExecution(CommandInvocationFailure(e))

    assertIsNOTCommandExecution(CommandResult(0), None)
    assertIsNOTCommandExecution(CommandResult(1), None)
    assertIsNOTCommandExecution(CommandResult(-1), None)
    assertIsNOTCommandExecution(CommandResult(42), None)

    assertIsNOTCommandExecution(CommandInvocationFailure(e), None)
    
    assertIsNOTCommandExecution(Success)
    assertIsNOTCommandExecution(Failure)
    assertIsNOTCommandExecution(FailureWithException(e))
    assertIsNOTCommandExecution(ValueSuccess(123, TypeBox.of[Int]))

    assertIsNOTCommandExecution(Success, None)
    assertIsNOTCommandExecution(Failure, None)
    assertIsNOTCommandExecution(FailureWithException(e), None)
    assertIsNOTCommandExecution(ValueSuccess(123, TypeBox.of[Int]), None)
  }

  //scalastyle:on magic.number
}
