package loamstream.model.execute

import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.compiler.LoamCompiler
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobStatus.Skipped
import loamstream.model.jobs.JobStatus.Succeeded
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.Output
import loamstream.util.Hashes
import loamstream.util.PathUtils
import loamstream.util.Sequence

/**
  * @author clint
  *         kaan
  *         date: Aug 12, 2016
  */
final class ExecutionResumptionTest extends FunSuite with ProvidesSlickLoamDao with ProvidesEnvAndResources {
  private def runsEverythingExecuter = RxExecuter.default

  private def dbBackedExecuter = RxExecuter.defaultWith(new DbBackedJobFilter(dao))

  private def hashAndStore(p: Path, exitCode: Int = 0): Execution = {
    val hash = Hashes.sha1(p)
    val lastModified = PathUtils.lastModifiedTime(p)

    val e = Execution(
        id = None,
        env = mockEnv,
        cmd = Option(mockCmd),
        status = JobStatus.fromExitCode(exitCode),
        result = Option(JobResult.CommandResult(exitCode)),
        resources = Some(mockResources),
        outputStreams = Some(TestHelpers.dummyOutputStreams),
        outputs = Set(cachedOutput(p, hash, lastModified)))
    
    store(e)
    
    e
  }

  test("Pipelines can be resumed after stopping 1/3rd of the way through") {
    doTest(Seq(Skipped, Succeeded, Succeeded)) { (start, f1, f2, f3) =>
      import java.nio.file.{Files => JFiles}

      assert(!f1.toFile.exists)
      
      JFiles.copy(start, f1)
      
      hashAndStore(f1)
      
      assert(f1.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f1))
    }
  }

  test("Pipelines can be resumed after stopping 2/3rds of the way through") {
    doTest(Seq(Skipped, Skipped, Succeeded)) { (start, f1, f2, f3) =>
      import java.nio.file.{Files => JFiles}

      JFiles.copy(start, f1)
      JFiles.copy(start, f2)

      hashAndStore(f1)
      hashAndStore(f2)

      assert(f1.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f1))

      assert(f2.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f2))
    }
  }

  test("Re-running a finished pipelines does nothing") {
    doTest(Seq(Skipped, Skipped, Skipped)) { (start, f1, f2, f3) =>
      import java.nio.file.{Files => JFiles}

      JFiles.copy(start, f1)
      JFiles.copy(start, f2)
      JFiles.copy(start, f3)

      hashAndStore(f1)
      hashAndStore(f2)
      hashAndStore(f3)

      assert(f1.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f1))

      assert(f2.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f2))

      assert(f3.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f3))
    }
  }

  test("Every job is run for Pipelines with no existing outputs") {
    doTest(Seq(Succeeded, Succeeded, Succeeded)) { (start, f1, f2, f3) =>
      //No setup
    }
  }

  private def mockJob(name: String, outputs: Set[Output], inputs: Set[JobNode] = Set.empty)(body: => Any): MockJob = {
    val successfulExecution = TestHelpers.executionFromResult(JobResult.CommandResult(0))

    val successfulExecutionWithOutputs = successfulExecution.copy(outputs = outputs.map(_.toOutputRecord))
    
    new MockJob(successfulExecutionWithOutputs, name, inputs, outputs, delay = 0) {
      override def execute(implicit context: ExecutionContext): Future[Execution] = {
        body
        
        super.execute
      }
    }
  }

  // scalastyle:off method.length
  //NB: Tests with the 'run-everything' JobFilter as well as a DB-backed one.
  private def doTest(expectations: Seq[JobStatus])(setup: (Path, Path, Path, Path) => Any): Unit = {

    def doTestWithExecuter(executer: RxExecuter): Unit = {
      import java.nio.file.{ Files => JFiles }
      import loamstream.util.PathEnrichments._
      val workDir = makeWorkDir()

      def path(s: String) = Paths.get(s)

      // Setting the option to replace existing files so if a 'cp' job was mistakenly run (instead of being skipped),
      // the test fails with a more descriptive message instead of a FileAlreadyExistsException
      def copy(source: Path, target: Path) = JFiles.copy(source, target, StandardCopyOption.REPLACE_EXISTING)

      val start = path("src/test/resources/a.txt")
      val f1 = workDir / "fileOut1.txt"
      val f2 = workDir / "fileOut2.txt"
      val f3 = workDir / "fileOut3.txt"

      val startToF1 = mockJob(copyCmd(start, f1), Set(Output.PathOutput(f1))) {
        copy(start, f1)
      }

      val f1ToF2 = mockJob(copyCmd(f1, f2), Set(Output.PathOutput(f2)), Set(startToF1)) {
        copy(f1, f2)
      }

      val f2ToF3 = mockJob(copyCmd(f2, f3), Set(Output.PathOutput(f3)), Set(f1ToF2)) {
        copy(f2, f3)
      }

      assert(startToF1.status == JobStatus.NotStarted)
      assert(f1ToF2.status == JobStatus.NotStarted)
      assert(f2ToF3.status == JobStatus.NotStarted)
  
      val executable = Executable(Set(f2ToF3))
  
      def runningEverything: Boolean = executer match {
        case RxExecuter(_, _, jobFilter, _, _) => jobFilter == JobFilter.RunEverything
        case _ => false
      }

      createTablesAndThen {
        assert(start.toFile.exists)
        assert(!f1.toFile.exists)
        assert(!f2.toFile.exists)
        assert(!f3.toFile.exists)

        if (!runningEverything) {
          setup(start, f1, f2, f3)
        }

        val jobResults = executer.execute(executable)

        val jobStatuses = Seq(startToF1.status, f1ToF2.status, f2ToF3.status)

        val expectedStatuses = {
          if (runningEverything) {
            Seq(Succeeded, Succeeded, Succeeded)
          }
          else {
            expectations
          }
        }

        assert(jobStatuses == expectedStatuses)

        val expectedNumResults = if (runningEverything) 3 else expectations.count(_.isSuccess)

        assert(jobResults.size == expectedNumResults)

        assert(jobResults.values.forall(_.isSuccess))
      }
    }

    doTestWithExecuter(runsEverythingExecuter)

    doTestWithExecuter(dbBackedExecuter)
  }

  // scalastyle:on method.length

  private def copyCmd(file1: Path, file2: Path): String = s"cp $file1 $file2"

  private lazy val compiler = new LoamCompiler

  private val sequence: Sequence[Int] = Sequence()

  private def makeWorkDir(): Path = TestHelpers.getWorkDir(getClass.getSimpleName)
}
