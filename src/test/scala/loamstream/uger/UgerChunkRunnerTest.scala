package loamstream.uger

import java.nio.file.Paths

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamPredef
import loamstream.conf.UgerConfig
import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamScriptContext
import loamstream.model.execute.Environment
import loamstream.model.execute.UgerSettings
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.NoOpJob
import loamstream.model.jobs.Output
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.uger.UgerChunkRunnerTest.MockJobSubmitter
import loamstream.util.ObservableEnrichments
import rx.lang.scala.Observable
import rx.lang.scala.schedulers.IOScheduler
import loamstream.model.jobs.LocalJob
import loamstream.model.jobs.JobNode
import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.commandline.HasCommandLine


/**
 * @author clint
 * @author kyuksel
 * Jul 25, 2016
 */
final class UgerChunkRunnerTest extends FunSuite {
  private val scheduler = IOScheduler()
  
  import loamstream.TestHelpers.neverRestart
  import loamstream.TestHelpers.path
  
  private val tempDir = TestHelpers.getWorkDir(getClass.getSimpleName) 
  
  private val ugerConfig = {
    import scala.concurrent.duration.DurationInt
    
    val workDir = tempDir.resolve("foo")
    
    UgerConfig(
      workDir = workDir, 
      maxNumJobs = 42,
      defaultCores = Cpus(2),
      defaultMemoryPerCore = Memory.inGb(2),
      defaultMaxRunTime = CpuTime.inHours(7))
  }
  
  private val executionConfig: ExecutionConfig = ExecutionConfig(42, tempDir.resolve("bar")) 
  
  import loamstream.util.Futures.waitFor
  import loamstream.util.ObservableEnrichments._
  
  test("NoOpJob is not attempted to be executed") {
    val mockDrmaaClient = MockDrmaaClient(Map.empty)
    val runner = UgerChunkRunner(
        executionConfig = executionConfig,
        ugerConfig = ugerConfig,
        jobSubmitter = JobSubmitter.Drmaa(mockDrmaaClient, ugerConfig),
        jobMonitor = new JobMonitor(scheduler, Poller.drmaa(mockDrmaaClient)))
    
    val noOpJob = NoOpJob(Set.empty)
    
    val result = waitFor(runner.run(Set(noOpJob), neverRestart).firstAsFuture)
    
    assert(result === Map.empty)
  }

  test("No failures when empty set of jobs is presented") {
    val mockDrmaaClient = new MockDrmaaClient(Map.empty)
    val runner = UgerChunkRunner(
        executionConfig = executionConfig,
        ugerConfig = ugerConfig,
        jobSubmitter = JobSubmitter.Drmaa(mockDrmaaClient, ugerConfig),
        jobMonitor = new JobMonitor(scheduler, Poller.drmaa(mockDrmaaClient)))
    
    val result = waitFor(runner.run(Set.empty, neverRestart).firstAsFuture)
    
    assert(result === Map.empty)
  }
  
  test("combine") {
    import UgerChunkRunner.combine
    
    assert(combine(Map.empty, Map.empty) == Map.empty)
    
    val m1 = Map("a" -> 1, "b" -> 2, "c" -> 3)
    
    assert(combine(Map.empty, m1) == Map.empty)
    
    assert(combine(m1, Map.empty) == Map.empty)
    
    val m2 = Map("a" -> 42.0, "c" -> 99.0, "x" -> 123.456)
    
    assert(combine(m1, m2) == Map("a" -> (1, 42.0), "c" -> (3, 99.0)))
    
    assert(combine(m2, m1) == Map("a" -> (42.0, 1), "c" -> (99.0, 3)))
  }
  
  test("handleFailureStatus") {
    import UgerChunkRunner.handleFailureStatus
    import JobStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    
    def doTest(failureStatus: JobStatus): Unit = {
      val job = MockJob(NotStarted)
      
      assert(job.status === NotStarted)
      
      handleFailureStatus(alwaysRestart, failureStatus)(job)
      
      assert(job.status === failureStatus)
      
      handleFailureStatus(neverRestart, failureStatus)(job)
      
      assert(job.status === FailedPermanently)
    }
    
    doTest(Failed)
    doTest(FailedWithException)
    doTest(Terminated)
  }

  test("handleUgerStatus") {
    import UgerChunkRunner.handleUgerStatus
    import JobStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    
    def doTest(ugerStatus: UgerStatus, isFailure: Boolean): Unit = {
      val jobStatus = UgerStatus.toJobStatus(ugerStatus)
      
      {
        val job = MockJob(NotStarted)
        
        assert(job.status === NotStarted)
        
        handleUgerStatus(alwaysRestart, job)(ugerStatus)
        
        assert(job.status === jobStatus)
      }
      
      {
        val job = MockJob(NotStarted)
        
        assert(job.status === NotStarted)
      
        handleUgerStatus(neverRestart, job)(ugerStatus)
      
        val expected = if(isFailure) FailedPermanently else jobStatus
      
        assert(job.status === expected)
      }
    }
    
    doTest(UgerStatus.Failed(), isFailure = true)
    doTest(UgerStatus.CommandResult(1, None), isFailure = true)
    doTest(UgerStatus.DoneUndetermined(), isFailure = true)
    doTest(UgerStatus.Suspended(), isFailure = true)
    
    doTest(UgerStatus.Done, isFailure = false)
    doTest(UgerStatus.Queued, isFailure = false)
    doTest(UgerStatus.QueuedHeld, isFailure = false)
    doTest(UgerStatus.Requeued, isFailure = false)
    doTest(UgerStatus.RequeuedHeld, isFailure = false)
    doTest(UgerStatus.Running, isFailure = false)
    doTest(UgerStatus.Undetermined(), isFailure = false)
    doTest(UgerStatus.CommandResult(0, None), isFailure = false)
  }
  
  private def toTuple(jobWrapper: UgerJobWrapper): (UgerJobWrapper, Observable[UgerStatus]) = {
    import UgerChunkRunnerTest.MockUgerJob
    
    val mockJob = jobWrapper.commandLineJob.asInstanceOf[MockUgerJob]
    
    jobWrapper -> Observable.from(mockJob.statusesToReturn)
  }
  
  test("toExecutions - one failed job") {
    import UgerChunkRunner.toExecutions
    import UgerStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    import UgerChunkRunnerTest.MockUgerJob
    import ObservableEnrichments._
    
    val id = "failed"
    
    def doTest(shouldRestart: LJob => Boolean, lastUgerStatus: UgerStatus, expectedLastStatus: JobStatus): Unit = {
      val job = MockUgerJob(id, Queued, Queued, Running, Running, lastUgerStatus)
      
      val failed = UgerJobWrapper(ExecutionConfig.default, job, 1)
      
      assert(job.runCount === 0)
      
      val result = waitFor(toExecutions(shouldRestart, Map(id -> toTuple(failed))).firstAsFuture)
      
      val Seq((actualJob, execution)) = result.toSeq
      
      assert(actualJob === job)    
      assert(execution.status === JobStatus.Failed)
      assert(execution.isFailure)
      //TODO: Other assertions about execution?
      
      val expectedStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastStatus)
      
      val statuses = if(shouldRestart(job)) job.statuses.take(3) else job.statuses 

      assert(waitFor(statuses.to[Seq].firstAsFuture) === expectedStatuses)
      
      assert(job.runCount === 1)
    }
    
    doTest(neverRestart, Failed(), JobStatus.FailedPermanently)
    doTest(neverRestart, DoneUndetermined(), JobStatus.FailedPermanently)
    doTest(neverRestart, CommandResult(1, None), JobStatus.FailedPermanently)
    
    doTest(alwaysRestart, Failed(), JobStatus.Failed)
    doTest(alwaysRestart, DoneUndetermined(), JobStatus.Failed)
    doTest(alwaysRestart, CommandResult(1, None), JobStatus.Failed)
  }
  
  test("toExecutions - one successful job") {
    import UgerChunkRunner.toExecutions
    import UgerStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    import UgerChunkRunnerTest.MockUgerJob
    import ObservableEnrichments._
    
    val id = "worked"
    
    def doTest(shouldRestart: LJob => Boolean, lastUgerStatus: UgerStatus, expectedLastStatus: JobStatus): Unit = {
      val job = MockUgerJob(id, Queued, Queued, Running, Running, lastUgerStatus)
      
      val worked = UgerJobWrapper(ExecutionConfig.default, job, 1)
      
      assert(job.runCount === 0)
      
      val result = waitFor(toExecutions(shouldRestart, Map(id -> toTuple(worked))).firstAsFuture)
      
      val Seq((actualJob, execution)) = result.toSeq
      
      assert(actualJob === job)
      assert(execution.status === JobStatus.Succeeded)
      assert(execution.isSuccess)
      //TODO: Other assertions about execution?
      
      val expectedStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastStatus)
      
      val statuses = job.statuses.to[Seq] 

      assert(waitFor(statuses.firstAsFuture) === expectedStatuses)
      
      assert(job.runCount === 1)
    }
    
    doTest(neverRestart, Done, JobStatus.Succeeded)
    doTest(neverRestart, CommandResult(0, None), JobStatus.Succeeded)
    
    doTest(alwaysRestart, Done, JobStatus.Succeeded)
    doTest(alwaysRestart, CommandResult(0, None), JobStatus.Succeeded)
  }
  
  test("toExecutions - one successful job, one failed job") {
    import UgerChunkRunner.toExecutions
    import UgerStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    import UgerChunkRunnerTest.MockUgerJob
    import ObservableEnrichments._
    
    val goodId = "worked"
    val badId = "failed"
    
    def doTest(
        shouldRestart: LJob => Boolean, 
        lastGoodUgerStatus: UgerStatus, 
        lastBadUgerStatus: UgerStatus, 
        expectedLastGoodStatus: JobStatus,
        expectedLastBadStatus: JobStatus): Unit = {
      
      val workedJob = MockUgerJob(goodId, Queued, Queued, Running, Running, lastGoodUgerStatus)
      val failedJob = MockUgerJob(badId, Queued, Queued, Running, Running, lastBadUgerStatus)
      
      val worked = UgerJobWrapper(ExecutionConfig.default, workedJob, 1)
      val failed = UgerJobWrapper(ExecutionConfig.default, failedJob, 2)
      
      assert(workedJob.runCount === 0)
      assert(failedJob.runCount === 0)
      
      val input = Map(goodId -> toTuple(worked), badId -> toTuple(failed))
      
      val result = waitFor(toExecutions(shouldRestart, input).firstAsFuture)
      
      val goodExecution = result(workedJob)
      val badExecution = result(failedJob)
      
      assert(result.size === 2)
      
      assert(goodExecution.status === JobStatus.Succeeded)
      assert(goodExecution.isSuccess)
      assert(badExecution.status === JobStatus.Failed)
      assert(badExecution.isFailure)
      //TODO: Other assertions about execution?
      
      val expectedGoodStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastGoodStatus)
      val expectedBadStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastBadStatus)
      
      val goodStatuses = workedJob.statuses.to[Seq] 
      val badStatuses = (if(shouldRestart(failedJob)) failedJob.statuses.take(3) else failedJob.statuses).to[Seq]

      assert(waitFor(goodStatuses.firstAsFuture) === expectedGoodStatuses)
      assert(waitFor(badStatuses.firstAsFuture) === expectedBadStatuses)
      
      assert(workedJob.runCount === 1)
      assert(failedJob.runCount === 1)
    }
    
    val failedPermanently = JobStatus.FailedPermanently
    
    doTest(neverRestart, Done, Failed(), JobStatus.Succeeded, failedPermanently)
    doTest(neverRestart, CommandResult(0, None), CommandResult(1, None), JobStatus.Succeeded, failedPermanently)
    
    doTest(alwaysRestart, Done, Failed(), JobStatus.Succeeded, JobStatus.Failed)
    doTest(alwaysRestart, CommandResult(0, None), CommandResult(1, None), JobStatus.Succeeded, JobStatus.Failed)
  }
  
  test("Uger config is propagated to DRMAA client - 2 jobs, same settings") {
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
    
      val a = store.at("a.txt").asInput
      val b = store.at("b.txt")
      val c = store.at("c.txt")
    
      ugerWith(cores = 4, mem = 16, maxRunTime = 5) {
        cmd"cp $a $b".in(a).out(b)
        cmd"cp $a $c".in(a).out(c)
      }
    }
    
    val executable = LoamEngine.toExecutable(graph)

    //NB: Skip NoOpJob
    val jobs = executable.jobs.head.inputs.toSeq
    
    assert(jobs.size === 2)
    
    val expectedSettings = UgerSettings(Cpus(4), Memory.inGb(16), CpuTime.inHours(5))
    val expectedEnv = Environment.Uger(expectedSettings)
    
    assert(jobs(0).job.executionEnvironment === expectedEnv)
    assert(jobs(1).job.executionEnvironment === expectedEnv)
        
    val mockDrmaaClient = MockDrmaaClient(Map.empty)
    val mockJobSubmitter = new MockJobSubmitter
    
    val chunkRunner = UgerChunkRunner(
        executionConfig = executionConfig,
        ugerConfig = ugerConfig,
        jobSubmitter = mockJobSubmitter,
        jobMonitor = new JobMonitor(poller = Poller.drmaa(mockDrmaaClient)))
        
    import ObservableEnrichments._        
    
    val results = waitFor(chunkRunner.run(jobs.map(_.job).toSet, neverRestart).firstAsFuture)
    
    val actualSubmissionParams = mockJobSubmitter.params
    
    val Seq((actualSettings, actualSubmittedJobs)) = actualSubmissionParams
    
    assert(actualSettings === expectedSettings)
    assert(actualSubmittedJobs.ugerJobs.map(_.commandLineJob).toSet === jobs.toSet)
  }
  
  test("Uger config is propagated to DRMAA client - 2 pairs of jobs with different settings") {
    val (graph, tool0, tool1, tool2, tool3) = { 
      implicit val sc = new LoamScriptContext(TestHelpers.emptyProjectContext)
      
      import LoamPredef._
      import LoamCmdTool._
    
      val a = store.at("a.txt").asInput
      val b = store.at("b.txt")
      val c = store.at("c.txt")
      val d = store.at("d.txt")
      val e = store.at("e.txt")
    
      val (tool0, tool1) = ugerWith(cores = 4, mem = 16, maxRunTime = 5) {
        (cmd"cp $a $b".in(a).out(b)) -> (cmd"cp $a $c".in(a).out(c))
      }
      
      val (tool2, tool3) = ugerWith(cores = 7, mem = 9, maxRunTime = 11) {
        (cmd"cp $a $d".in(a).out(d)) -> (cmd"cp $a $e".in(a).out(e))
      }
      
      (sc.projectContext.graph, tool0, tool1, tool2, tool3)
    }
    
    val executable = LoamEngine.toExecutable(graph)

    //NB: Skip NoOpJob
    val jobs = executable.jobs.head.inputs.toSeq
    
    assert(jobs.size === 4)
    
    val expectedSettings0 = UgerSettings(Cpus(4), Memory.inGb(16), CpuTime.inHours(5))
    val expectedSettings1 = UgerSettings(Cpus(7), Memory.inGb(9), CpuTime.inHours(11))
    
    val expectedEnv0 = Environment.Uger(expectedSettings0)
    val expectedEnv1 = Environment.Uger(expectedSettings1)
    
    def findJob(tool: LoamCmdTool): CommandLineJob = {
      jobs.iterator.map(_.asInstanceOf[CommandLineJob]).find(_.commandLineString == tool.commandLine).get
    }
    
    assert(findJob(tool0).executionEnvironment === expectedEnv0)
    assert(findJob(tool1).executionEnvironment === expectedEnv0)
    
    assert(findJob(tool2).executionEnvironment === expectedEnv1)
    assert(findJob(tool3).executionEnvironment === expectedEnv1)
        
    val mockDrmaaClient = MockDrmaaClient(Map.empty)
    val mockJobSubmitter = new MockJobSubmitter
    
    val chunkRunner = UgerChunkRunner(
        executionConfig = executionConfig,
        ugerConfig = ugerConfig,
        jobSubmitter = mockJobSubmitter,
        jobMonitor = new JobMonitor(poller = Poller.drmaa(mockDrmaaClient)))
        
    import ObservableEnrichments._        
    
    val results = waitFor(chunkRunner.run(jobs.map(_.job).toSet, neverRestart).firstAsFuture)
    
    val actualSubmissionParams = mockJobSubmitter.params
    
    val actualParamsUnordered: Set[(UgerSettings, Set[LJob])] = {
      actualSubmissionParams.map { case (settings, taskArray) => 
        (settings, taskArray.ugerJobs.map(_.commandLineJob).toSet[LJob]) 
      }.toSet
    }
    
    val expectedParamsUnordered: Set[(UgerSettings, Set[LJob])] = Set(
        expectedSettings0 -> Set(findJob(tool0), findJob(tool1)),
        expectedSettings1 -> Set(findJob(tool2), findJob(tool3)))
    
    assert(actualParamsUnordered === expectedParamsUnordered)
  }
}

object UgerChunkRunnerTest {
  final class MockJobSubmitter extends JobSubmitter {
    @volatile var params: Seq[(UgerSettings, UgerTaskArray)] = Vector.empty
    
    override def submitJobs(ugerSettings: UgerSettings, taskArray: UgerTaskArray): DrmaaClient.SubmissionResult = {
      params :+= (ugerSettings -> taskArray)
      
      DrmaaClient.SubmissionSuccess(Map.empty)
    }
  }
  
  final case class MockUgerJob(name: String, statusesToReturn: UgerStatus*) extends LocalJob with HasCommandLine {
    require(statusesToReturn.nonEmpty)

    override def commandLineString: String = name //NB: The content of the command line is unimportant
    
    override val executionEnvironment: Environment = Environment.Local
    
    override def inputs: Set[JobNode] = Set.empty

    override def outputs: Set[Output] = Set.empty
    
    override def execute(implicit context: ExecutionContext): Future[Execution] = {
      Future.successful(Execution.from(this, UgerStatus.toJobStatus(statusesToReturn.last)))
    }
  }
}
