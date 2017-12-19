package loamstream.uger

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.model.jobs.commandline.CommandLineJob

/**
 * @author clint
 * Nov 22, 2017
 */
final class UgerJobWrapperTest extends FunSuite {
  import TestHelpers.path
  import UgerTaskArrayTest._
  
  private def wrapper(commandLineJob: CommandLineJob, ugerIndex: Int): UgerJobWrapper = {
    UgerJobWrapper(executionConfig, commandLineJob, ugerIndex)
  }
  
  test("ugerStdOutPath") {
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, jobs)
    
    val Seq(wrapper0, wrapper1, wrapper2) = taskArray.ugerJobs
    
    val jobName = UgerTaskArray.makeJobName(jobs)
    
    val ugerStdOutPath0 = wrapper0.ugerStdOutPath(taskArray)
    val ugerStdOutPath1 = wrapper1.ugerStdOutPath(taskArray)
    val ugerStdOutPath2 = wrapper2.ugerStdOutPath(taskArray)
    
    val expected0 = path(s"/foo/bar/baz/$jobName.1.stdout")
    val expected1 = path(s"/foo/bar/baz/$jobName.2.stdout")
    val expected2 = path(s"/foo/bar/baz/$jobName.3.stdout")
    
    assert(ugerStdOutPath0 === expected0)
    assert(ugerStdOutPath1 === expected1)
    assert(ugerStdOutPath2 === expected2)
  }
  
  test("ugerStdErrPath") {
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, jobs)
    
    val Seq(wrapper0, wrapper1, wrapper2) = taskArray.ugerJobs
    
    val jobName = UgerTaskArray.makeJobName(jobs)
    
    val ugerStdErrPath0 = wrapper0.ugerStdErrPath(taskArray)
    val ugerStdErrPath1 = wrapper1.ugerStdErrPath(taskArray)
    val ugerStdErrPath2 = wrapper2.ugerStdErrPath(taskArray)
    
    val expected0 = path(s"/foo/bar/baz/$jobName.1.stderr")
    val expected1 = path(s"/foo/bar/baz/$jobName.2.stderr")
    val expected2 = path(s"/foo/bar/baz/$jobName.3.stderr")
    
    assert(ugerStdErrPath0 === expected0)
    assert(ugerStdErrPath1 === expected1)
    assert(ugerStdErrPath2 === expected2)
  }
  
  test("outputStreams.stdout") {
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, Seq(j0))
    
    val jobName = UgerTaskArray.makeJobName(Seq(j0))
    
    val Seq(wrapper0) = taskArray.ugerJobs
    
    val expected = path(s"${executionConfig.outputDir}/${j0.id}.stdout")
    
    assert(wrapper0.outputStreams.stdout === expected)
  }
  
  test("outputStreams.stderr") {
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, Seq(j0))
    
    val Seq(wrapper0) = taskArray.ugerJobs
    
    val expected = path(s"${executionConfig.outputDir}/${j0.id}.stderr")
    
    assert(wrapper0.outputStreams.stderr === expected)
  }
  
  test("ugerCommandLine") {
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, Seq(j0))
    
    val Seq(wrapper0) = taskArray.ugerJobs
    
    val jobName = UgerTaskArray.makeJobName(Seq(j0))
    
    // scalastyle:off line.size.limit
    val expected = s"""|${j0.commandLineString}
                       |
                       |LOAMSTREAM_JOB_EXIT_CODE=$$?
                       |
                       |mkdir -p $outputDir
                       |mv /foo/bar/baz/$jobName.1.stdout $outputDir/${j0.id}.stdout || echo "Couldn't move Uger std out log" > $outputDir/${j0.id}.stdout
                       |mv /foo/bar/baz/$jobName.1.stderr $outputDir/${j0.id}.stderr || echo "Couldn't move Uger std err log" > $outputDir/${j0.id}.stderr
                       |
                       |exit $$LOAMSTREAM_JOB_EXIT_CODE
                       |""".stripMargin
    // scalastyle:on line.size.limit
    
    assert(wrapper0.ugerCommandLine(taskArray) === expected)
  }
}
