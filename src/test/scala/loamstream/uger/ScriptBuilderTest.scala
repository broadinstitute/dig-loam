package loamstream.uger

import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.model.execute.Environment
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.TestHelpers
import loamstream.conf.ExecutionConfig

/**
 * Created by kyuksel on 2/29/2016.
 */
object ScriptBuilderTest {
  private final implicit class EnrichedString(val string: String) extends AnyVal {
    def withNormalizedLineBreaks: String = string.replaceAll("\r\n", "\n")
  }
}

final class ScriptBuilderTest extends FunSuite {

  import ScriptBuilderTest.EnrichedString

  test("A shell script is generated out of a CommandLineJob, and can be used to submit a UGER job") {
    val ugerConfig = TestHelpers.config.ugerConfig.get
    
    val jobs = Seq(getShapeItCommandLineJob(0), getShapeItCommandLineJob(1), getShapeItCommandLineJob(2))
    val taskArray = UgerTaskArray.fromCommandLineJobs(ExecutionConfig.default, ugerConfig, jobs)
    val ugerScriptContents = ScriptBuilder.buildFrom(taskArray).withNormalizedLineBreaks
    
    val jobIds: (String, String, String) = (jobs(0).id, jobs(1).id, jobs(2).id)
    val discriminators = (0, 1, 2)
    
    val expectedScriptContents = expectedScriptAsString(discriminators, jobIds).withNormalizedLineBreaks

    assert(ugerScriptContents == expectedScriptContents)
  }

  import TestHelpers.path
  
  private def getShapeItCommandLineJob(discriminator: Int): CommandLineJob = {
    val shapeItExecutable = "/some/shapeit/executable"
    val shapeItWorkDir = path("someWorkDir")
    val vcf = s"/some/vcf/file.$discriminator"
    val map = s"/some/map/file.$discriminator"
    val hap = s"/some/haplotype/file.$discriminator"
    val samples = "/some/sample/file"
    val log = "/some/log/file"
    val numThreads = 2

    val commandLineString = getShapeItCommandLineString(shapeItExecutable, vcf, map, hap, samples, log, numThreads)

    CommandLineJob(commandLineString, shapeItWorkDir, Environment.Local)
  }

  private def getShapeItCommandLineTokens(
      shapeItExecutable: String,
      vcf: String,
      map: String,
      haps: String,
      samples: String,
      log: String,
      numThreads: Int = 1): Seq[String] = {

    Seq(
      shapeItExecutable,
      "-V",
      vcf,
      "-M",
      map,
      "-O",
      haps,
      samples,
      "-L",
      log,
      "--thread",
      numThreads).map(_.toString)
  }

  private def getShapeItCommandLineString(
      shapeItExecutable: String,
      vcf: String,
      map: String,
      haps: String,
      samples: String,
      log: String,
      numThreads: Int = 1): String = {

    getShapeItCommandLineTokens(shapeItExecutable, vcf, map, haps, samples, log, numThreads).mkString(" ")
  }

  // scalastyle:off method.length
  private def expectedScriptAsString(discriminators: (Int, Int, Int), jobIds: (String, String, String)): String = {
    val (discriminator0, discriminator1, discriminator2) = discriminators
    val (jobId0, jobId1, jobId2) = jobIds
    
    val jobName = s"LoamStream-${jobId0}_${jobId1}_${jobId2}"
    
    val ugerDir = "/humgen/diabetes/users/kyuksel/imputation/shapeit_example"
    val outputDir = path("job-outputs").toAbsolutePath
    
    val sixSpaces = "      "
    
    // scalastyle:off line.size.limit
    s"""#!/bin/bash
#$$ -cwd

source /broad/software/scripts/useuse
reuse -q UGER
reuse -q Java-1.8

export PATH=/humgen/diabetes/users/dig/miniconda2/bin:$$PATH
conda env create -f /humgen/diabetes/users/dig/hail/environment.yml
source activate hail

i=$$SGE_TASK_ID
$sixSpaces
if [ $$i -eq 1 ]
then
/some/shapeit/executable -V /some/vcf/file.$discriminator0 -M /some/map/file.$discriminator0 -O /some/haplotype/file.$discriminator0 /some/sample/file -L /some/log/file --thread 2

LOAMSTREAM_JOB_EXIT_CODE=$$?

mkdir -p $outputDir
mv $ugerDir/${jobName}.1.stdout $outputDir/${jobId0}.stdout || echo "Couldn't move Uger std out log" > $outputDir/${jobId0}.stdout
mv $ugerDir/${jobName}.1.stderr $outputDir/${jobId0}.stderr || echo "Couldn't move Uger std err log" > $outputDir/${jobId0}.stderr

exit $$LOAMSTREAM_JOB_EXIT_CODE

elif [ $$i -eq 2 ]
then
/some/shapeit/executable -V /some/vcf/file.$discriminator1 -M /some/map/file.$discriminator1 -O /some/haplotype/file.$discriminator1 /some/sample/file -L /some/log/file --thread 2

LOAMSTREAM_JOB_EXIT_CODE=$$?

mkdir -p $outputDir
mv $ugerDir/${jobName}.2.stdout $outputDir/${jobId1}.stdout || echo "Couldn't move Uger std out log" > $outputDir/${jobId1}.stdout
mv $ugerDir/${jobName}.2.stderr $outputDir/${jobId1}.stderr || echo "Couldn't move Uger std err log" > $outputDir/${jobId1}.stderr

exit $$LOAMSTREAM_JOB_EXIT_CODE

elif [ $$i -eq 3 ]
then
/some/shapeit/executable -V /some/vcf/file.$discriminator2 -M /some/map/file.$discriminator2 -O /some/haplotype/file.$discriminator2 /some/sample/file -L /some/log/file --thread 2

LOAMSTREAM_JOB_EXIT_CODE=$$?

mkdir -p $outputDir
mv $ugerDir/${jobName}.3.stdout $outputDir/${jobId2}.stdout || echo "Couldn't move Uger std out log" > $outputDir/${jobId2}.stdout
mv $ugerDir/${jobName}.3.stderr $outputDir/${jobId2}.stderr || echo "Couldn't move Uger std err log" > $outputDir/${jobId2}.stderr

exit $$LOAMSTREAM_JOB_EXIT_CODE

fi
"""
  // scalastyle:on line.size.limit
  }
  // scalastyle:on method.length
}
