package loamstream

import java.io.File
import java.nio.file.Files
import _root_.utils.Loggable.Level
import utils.{LoamFileUtils, TestUtils, StringUtils}
import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal._
import loamstream.conf.SampleFiles
import loamstream.map.LToolMapper
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.commons.io.FileUtils
import scala.io.Source

/**
  * Created by kyuksel on 2/29/2016.
  */
class HailSingletonEndToEndTest extends FunSuite with BeforeAndAfter {
  import TestData.sampleFiles

  val hailVdsFilePath = sampleFiles.hailVdsOpt.get
  val hailVcfFilePath = sampleFiles.hailVcfOpt.get
  val hailSingletonFilePath = sampleFiles.singletonsOpt.get

  // Make sure to not mistakenly use an output file from a previous run, if any
  FileUtils.deleteQuietly(new File(hailVdsFilePath.toString))
  FileUtils.deleteQuietly(new File(hailSingletonFilePath.toString))

  val vcfFiles = Seq(StringUtils.pathTemplate(hailVcfFilePath.toString, "XXX"),
    StringUtils.pathTemplate(hailVdsFilePath.toString, "XXX"))
  val vdsFiles = Seq(hailVdsFilePath)
  val singletonFiles = Seq(hailSingletonFilePath)

  val config = MiniToolBox.InteractiveFallbackConfig(vcfFiles, vdsFiles, singletonFiles)
  val pipeline = HailPipeline.pipeline
  val toolbox = MiniToolBox(config)
  val mappings = LToolMapper.findAllSolutions(pipeline, toolbox)
  for (mapping <- mappings)
    LToolMappingLogger.logMapping(Level.trace, mapping)
  val mappingCostEstimator = LPipelineMiniCostEstimator
  val mapping = mappingCostEstimator.pickCheapest(mappings)
  LToolMappingLogger.logMapping(Level.trace, mapping)

  val genotypesJob = toolbox.createJobs(HailPipeline.genotypeCallsRecipe, pipeline, mapping)
  val importVcfJob = toolbox.createJobs(HailPipeline.vdsRecipe, pipeline, mapping)
  val calculateSingletonsJob = toolbox.createJobs(HailPipeline.singletonRecipe, pipeline, mapping)

  val executable = toolbox.createExecutable(pipeline, mapping)

  test("Methods and piles are successfully mapped to tools and stores") {
    assert(mappings.size == 1)
    assert(mappings.head.stores.size == 3)
    assert(mappings.head.tools.size == 3)
  }

  test("Jobs are successfully created") {
    assert(TestUtils.isHitOfSetOfOne(genotypesJob))
    assert(TestUtils.isHitOfSetOfOne(calculateSingletonsJob))
    assert(TestUtils.isHitOfSetOfOne(importVcfJob))
  }

  ignore("Singletons are successfully counted using Hail") {
    MiniExecuter.execute(executable)

    val source = Source.fromFile(hailSingletonFilePath.toFile)
    LoamFileUtils.enclosed(source)(bufSrc => {
      val singletonCounts = bufSrc.getLines().toList
      assert(singletonCounts.size == 101)
      assert(singletonCounts.head == "SAMPLE\tSINGLETONS")
      assert(singletonCounts.tail.head == "C1046::HG02024\t0")
      assert(singletonCounts.last == "HG00629\t0")
    })
  }

  // Make sure to not mistakenly use an output file from a previous run, if any
  FileUtils.deleteQuietly(new File(hailVdsFilePath.toString))
  FileUtils.deleteQuietly(new File(hailSingletonFilePath.toString))
}
