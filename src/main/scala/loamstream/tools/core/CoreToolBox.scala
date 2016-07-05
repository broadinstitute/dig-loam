package loamstream.tools.core

import java.io.File
import java.nio.file.{ Files, Path }

import scala.concurrent.{ ExecutionContext, Future }

import htsjdk.variant.variantcontext.Genotype

import loamstream.LEnv
import loamstream.conf.Impute2Config
import loamstream.conf.ShapeItConfig
import loamstream.model.AST
import loamstream.model.AST.ToolNode
import loamstream.model.LPipeline
import loamstream.model.Tool
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.{ LJob, LToolBox }
import loamstream.model.jobs.commandline.CommandLineBuilderJob
import loamstream.model.jobs.LJob.{ Result, SimpleFailure, SimpleSuccess }
import loamstream.tools.{ HailTools, PcaProjecter, PcaWeightsReader, VcfParser }
import loamstream.tools.LineCommand
import loamstream.tools.VcfUtils
import loamstream.tools.klusta.{ KlustaKwikKonfig, KlustaKwikLineCommand }
import loamstream.tools.klusta.{ KlustaKwikLineCommand, KlustaKwikInputWriter }
import loamstream.util.{ Hit, Miss, Shot }
import loamstream.util.LoamFileUtils
import loamstream.util.SnagMessage

/**
 * LoamStream
 * Created by oliverr on 2/23/2016.
 */
object CoreToolBox {

  final case class FileExists(path: Path) extends LJob.Success {
    override def successMessage: String = s"'$path' exists"
  }

  trait CheckPreexistingFileJob extends LJob {
    def file: Path

    override def execute(implicit context: ExecutionContext): Future[Result] = Future {
      Result.attempt {
        if (Files.exists(file)) { FileExists(file) }
        else { SimpleFailure(s"$file does not exist.") }
      }
    }
  }

  final case class CheckPreexistingVcfFileJob(
      file: Path,
      inputs: Set[LJob] = Set.empty) extends CheckPreexistingFileJob {

    @deprecated("", "")
    override def toString = s"CheckPreexistingVcfFileJob($file, ...)"

    override def withInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
  }

  final case class CheckPreexistingPcaWeightsFileJob(
      file: Path,
      inputs: Set[LJob] = Set.empty) extends CheckPreexistingFileJob {

    @deprecated("", "")
    override def toString = s"CheckPreexistingPcaWeightsFileJob($file, ...)"

    override def withInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
  }

  final case class ExtractSampleIdsFromVcfFileJob(
      vcfFile: Path,
      samplesFile: Path,
      inputs: Set[LJob] = Set.empty) extends LJob {

    override def withInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override def execute(implicit context: ExecutionContext): Future[Result] = runBlocking {
      Result.attempt {
        val samples = VcfParser(vcfFile).samples

        LoamFileUtils.printToFile(samplesFile.toFile) {
          p => samples.foreach(p.println) // scalastyle:ignore
        }

        SimpleSuccess("Extracted sample ids.")
      }
    }
  }

  final case class ImportVcfFileJob(vcfFile: Path, vdsFile: Path, inputs: Set[LJob] = Set.empty) extends LJob {

    override def withInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override def execute(implicit context: ExecutionContext): Future[Result] = runBlocking {
      Result.attempt {
        HailTools.importVcf(vcfFile, vdsFile)

        SimpleSuccess("Imported VCF in VDS format.")
      }
    }
  }

  final case class CalculateSingletonsJob(
      vdsDir: Path,
      singletonsFile: Path,
      inputs: Set[LJob] = Set.empty) extends LJob {

    override def withInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override def execute(implicit context: ExecutionContext): Future[Result] = runBlocking {
      Result.attempt {
        HailTools.calculateSingletons(vdsDir, singletonsFile)

        SimpleSuccess("Calculated singletons from VDS.")
      }
    }
  }

  final case class CalculatePcaProjectionsJob(vcfFile: Path,
                                              pcaWeightsFile: Path,
                                              klustaKwikKonfig: KlustaKwikKonfig,
                                              inputs: Set[LJob] = Set.empty) extends LJob {

    @deprecated("", "")
    override def toString = s"CalculatePcaProjectionsJob($vcfFile, $pcaWeightsFile, ...)"

    override def withInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override def execute(implicit context: ExecutionContext): Future[Result] = runBlocking {
      Result.attempt {
        val weights = PcaWeightsReader.read(pcaWeightsFile)
        val pcaProjecter = PcaProjecter(weights)
        val vcfParser = VcfParser(vcfFile)
        val samples = vcfParser.samples
        val genotypeToDouble: Genotype => Double = { genotype => VcfUtils.genotypeToAltCount(genotype).toDouble }
        val pcaProjections = pcaProjecter.project(samples, vcfParser.genotypeMapIter, genotypeToDouble)

        KlustaKwikInputWriter.writeFeatures(klustaKwikKonfig.inputFile, pcaProjections)

        SimpleSuccess(s"Wrote PCA projections to file ${klustaKwikKonfig.inputFile}")
      }
    }
  }
}

final case class CoreToolBox(env: LEnv) extends LToolBox {

  import CoreToolBox._

  private def pathShot(path: Path): Shot[Path] = {
    if (path.toFile.exists) { Hit(path) }
    else { Miss(s"Couldn't find '$path'") }
  }

  def sampleFileShot(path: Path): Shot[Path] = pathShot(path)

  def vcfFileJobShot(path: Path): Shot[CheckPreexistingVcfFileJob] = Hit(CheckPreexistingVcfFileJob(path))

  def pcaWeightsFileJobShot(path: Path): Shot[CheckPreexistingPcaWeightsFileJob] = {
    Hit(CheckPreexistingPcaWeightsFileJob(path))
  }

  def extractSamplesJobShot(vcfFile: Path, sampleFile: Path): Shot[ExtractSampleIdsFromVcfFileJob] = {
    Hit(ExtractSampleIdsFromVcfFileJob(vcfFile, sampleFile))
  }

  def convertVcfToVdsJobShot(vcfFile: Path, vdsPath: Path): Shot[ImportVcfFileJob] = {
    Hit(ImportVcfFileJob(vcfFile, vdsPath))
  }

  def calculateSingletonsJobShot(vdsDir: Path, singletonsFile: Path): Shot[CalculateSingletonsJob] = {
    Hit(CalculateSingletonsJob(vdsDir, singletonsFile))
  }

  def calculatePcaProjectionsJobShot(
    vcfFile: Path,
    pcaWeightsFile: Path,
    klustaConfig: KlustaKwikKonfig): Shot[CalculatePcaProjectionsJob] = {

    Hit(CalculatePcaProjectionsJob(vcfFile, pcaWeightsFile, klustaConfig))
  }

  private def klustaKlwikCommandLine(klustaConfig: KlustaKwikKonfig): LineCommand.CommandLine = {
    import KlustaKwikLineCommand._

    klustaKwik(klustaConfig) + useDistributional(0)
  }

  def calculateClustersJobShot(klustaConfig: KlustaKwikKonfig): Shot[CommandLineBuilderJob] = Shot {
    CommandLineBuilderJob(
      klustaKlwikCommandLine(klustaConfig),
      klustaConfig.workDir,
      Set.empty)
  }

  private def commandLineJobShot(tokens: Seq[String], workDir: Path): Shot[CommandLineBuilderJob] = {
    def commandLine(parts: Seq[String]): LineCommand.CommandLine = new LineCommand.CommandLine {
      override def tokens: Seq[String] = parts
      override def commandLine = tokens.mkString(LineCommand.tokenSep)
    }

    Shot(CommandLineBuilderJob(commandLine(tokens), workDir))
  }

  //TODO: Shouldn't be here
  private def shapeitJobShot(config: ShapeItConfig, inputVcf: Path, outputHaps: Path): Shot[LJob] = {
    def tempFile: Path = File.createTempFile("loamstream", "shapeit-output-samples").toPath.toAbsolutePath

    //TODO
    val tokens: Seq[String] = Seq(
      config.executable.toString,
      "-V",
      inputVcf.toString,
      "-M",
      config.mapFile.toString,
      "-O",
      outputHaps.toString,
      tempFile.toString,
      "-L",
      config.logFile.toString,
      "--thread",
      config.numThreads.toString)

    commandLineJobShot(tokens, config.workDir)
  }

  //TODO: Shouldn't be here
  private def impute2JobShot(config: Impute2Config, inputFile: Path, outputFile: Path): Shot[LJob] = {

    val tokens: Seq[String] = Seq(
      config.executable.toString,
      "-use_prephased_g",
      "-m",
      "example.chr22.map",
      "-h", //example.chr22.1kG.haps.gz
      inputFile.toString,
      "-l",
      "example.chr22.1kG.legend.gz",
      "-known_haps_g",
      "example.chr22.prephasing.impute2_haps.gz",
      "-int",
      "20.4e6",
      "20.5e6",
      "-Ne",
      "20000",
      "-o",
      //NB: Must be an absolute path or impute2 will go haywire and never terminate.
      outputFile.toAbsolutePath.toString,
      "-verbose",
      "-o_gz")

    commandLineJobShot(tokens, config.workDir)
  }

  def toolToJobShot(tool: Tool): Shot[LJob] = tool match { //scalastyle:ignore
    case CoreTool.CheckPreExistingVcfFile(vcfFile)                 => vcfFileJobShot(vcfFile)

    case CoreTool.CheckPreExistingPcaWeightsFile(pcaWeightsFile)   => pcaWeightsFileJobShot(pcaWeightsFile)

    case CoreTool.ExtractSampleIdsFromVcfFile(vcfFile, sampleFile) => extractSamplesJobShot(vcfFile, sampleFile)

    case CoreTool.ConvertVcfToVds(vcfFile, vdsDir)                 => convertVcfToVdsJobShot(vcfFile, vdsDir)

    case CoreTool.CalculateSingletons(vdsDir, singletonsFile)      => calculateSingletonsJobShot(vdsDir, singletonsFile)

    case CoreTool.ProjectPcaNative(vcfFile, pcaWeightsFile, klustaKonfig) =>
      calculatePcaProjectionsJobShot(vcfFile, pcaWeightsFile, klustaKonfig)

    case CoreTool.ProjectPca(vcfFile, pcaWeightsFile, klustaKonfig) =>
      calculatePcaProjectionsJobShot(vcfFile, pcaWeightsFile, klustaKonfig)

    case CoreTool.KlustaKwikClustering(klustaConfig) => calculateClustersJobShot(klustaConfig)

    case CoreTool.ClusteringSamplesByFeatures(klustaConfig) => calculateClustersJobShot(klustaConfig)

    //TODO: Shouldn't be here
    case CoreTool.Phase(config, inputVcf, outputHaps) => shapeitJobShot(config, inputVcf, outputHaps)

    //TODO: Shouldn't be here
    case CoreTool.Impute(config, inputVcf, output) => impute2JobShot(config, inputVcf, output)

    case _ => Miss(SnagMessage(s"Have not yet implemented tool $tool"))
  }

}