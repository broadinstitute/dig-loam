package loamstream

import java.nio.file.{ Files, Path }
import scala.io.Source
import org.scalatest.FunSuite
import loamstream.model.execute.LExecutable
import loamstream.model.execute.ChunkedExecuter
import loamstream.model.jobs.LToolBox
import loamstream.tools.core.{ CoreToolBox, LCoreEnv }
import loamstream.tools.core.LCoreDefaultStoreIds
import loamstream.util.Hit
import loamstream.util.LoamFileUtils
import loamstream.util.StringUtils
import loamstream.model.Tool
import loamstream.tools.core.CoreTool
import loamstream.model.LPipeline
import loamstream.model.HasAst
import loamstream.model.Store
import loamstream.model.AST

/**
 * Created by kyuksel on 2/29/2016.
 */
final class MiniAppEndToEndTest extends FunSuite {

  private val executer = ChunkedExecuter.default

  test("Pipeline successfully extracts sample IDs from VCF (via AST)") {
    val (toolbox, pipeline, extractedSamplesFilePath) = makePipelineAndToolbox()

    val executable = toolbox.createExecutable(pipeline.ast)

    doTestExecutable(executable, extractedSamplesFilePath)
  }

  private def doTestExecutable(executable: LExecutable, extractedSamplesFilePath: Path): Unit = {
    val results = executer.execute(executable)

    assert(results.size == 2)

    assert(results.values.forall {
      case Hit(r) => r.isSuccess
      case _      => false
    })

    val source = Source.fromFile(extractedSamplesFilePath.toFile)

    val samples = LoamFileUtils.enclosed(source)(_.getLines.toList)

    val expectedSamplesList = List("Sample1", "Sample2", "Sample3")

    assert(samples == expectedSamplesList)
  }

  private def makePipelineAndToolbox(): (LToolBox, MiniPipeline, Path) = {
    import TestData.sampleFiles

    val miniVcfFilePath = sampleFiles.miniVcfOpt.get
    val extractedSamplesFilePath = Files.createTempFile("samples", "txt")

    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath.toString, "XXX"))
    val sampleFilePaths = Seq(extractedSamplesFilePath)

    val env = {
      LCoreEnv.FileInteractiveFallback.env(vcfFiles, sampleFilePaths, Seq.empty[Path]) +
        (LCoreEnv.Keys.genotypesId -> LCoreDefaultStoreIds.genotypes)
    }

    val genotypesId = env(LCoreEnv.Keys.genotypesId)
    val pipeline = MiniPipeline(miniVcfFilePath, extractedSamplesFilePath)
    val toolbox = CoreToolBox(env)

    (toolbox, pipeline, extractedSamplesFilePath)
  }
}