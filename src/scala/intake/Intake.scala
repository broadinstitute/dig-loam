import loamstream.loam.LoamSyntax
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.LoamScriptContext
import loamstream.util.Maps
import loamstream.loam.intake.aggregator.{ ColumnNames => AggregatorColumnNames }
import loamstream.loam.intake.aggregator.ConfigData
import loamstream.loam.LoamCmdSyntax
import loamstream.loam.intake.aggregator.Metadata
import loamstream.loam.intake.aggregator.AggregatorIntakeConfig
import loamstream.util.Traversables
import com.typesafe.config.Config
import loamstream.conf.DataConfig
import loamstream.loam.intake.aggregator.AggregatorCommands
import loamstream.loam.LoamProjectContext
import loamstream.conf.LoamConfig
import com.typesafe.config.ConfigFactory
import loamstream.loam.intake.aggregator.SourceColumns
import scala.sys.error
import java.util.zip.GZIPInputStream
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.util.Calendar
import java.util.Date

object Intake extends loamstream.LoamFile {
  import IntakeSyntax._

  def delToChar(d: String): Char = {
    d match {
      case "space" => ' '
      case "tab" => '\t'
      case "comma" => ','
      case _ => sys.error("file delimiter " + d + " not recognized")
    }
  }

  object Paths {
    val workDir: Path = path("./")

    def dataFile(s: String): Path = {
      path(s"${s}")
    }
  }
  
  def sourceStore(phenotypeConfig: PhenotypeConfig): Store = {
    store(Paths.dataFile(phenotypeConfig.file))
  }
  
  def sourceStores(phenotypesToFiles: Map[String, PhenotypeConfig]): Map[String, Store] = {
    import Maps.Implicits._
    
    phenotypesToFiles.strictMapValues(sourceStore(_).asInput)
  }

  private val intakeTypesafeConfig: Config = loadConfig("INTAKE_CONF", "").config
  
  private val intakeMetadataTypesafeConfig: Config = loadConfig("INTAKE_METADATA_CONF", "").config

  def rowDef(source: CsvSource, name: String, phenoCfg: PhenotypeConfig): RowDef = {

    val gis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(name)))
    val br = new BufferedReader(new InputStreamReader(gis));
    val header = br.readLine().split(delToChar(phenoCfg.delimiter))
    gis.close()
    br.close()

    val CHROM = phenoCfg.CHROM.asColumnName
    val POS = phenoCfg.POS.asColumnName
    val REF = phenoCfg.REF.asColumnName
    val ALT = phenoCfg.ALT.asColumnName
    val EAF = phenoCfg.EAF match { case Some(s) => Some(s.asColumnName); case _ => None }
    val MAF = phenoCfg.MAF match { case Some(s) => Some(s.asColumnName); case _ => None }
    val BETA = phenoCfg.BETA match { case Some(s) => Some(s.asColumnName); case _ => None }
    val STDERR = phenoCfg.STDERR match { case Some(s) => Some(s.asColumnName); case _ => None }
    val ODDS_RATIO = phenoCfg.ODDS_RATIO match { case Some(s) => Some(s.asColumnName); case _ => None }
    val ZSCORE = phenoCfg.ZSCORE match { case Some(s) => Some(s.asColumnName); case _ => None }
    val PVALUE = phenoCfg.PVALUE.asColumnName
    val N = phenoCfg.N match { case Some(s) => Some(s.asColumnName); case _ => None }

    val varId = ColumnDef(
      AggregatorColumnNames.marker, 
      //"{chrom}_{pos}_{ref}_{alt}"
      strexpr"${CHROM}_${POS}_${REF}_${ALT}",
      //"{chrom}_{pos}_{alt}_{ref}"
      strexpr"${CHROM}_${POS}_${ALT}_${REF}")

    val neff = ColumnName("Neff")
    val n = ColumnName("n")

    var filteredSource = source.filterNot(REF === "I").filterNot(REF === "D")

    (EAF, MAF) match { case (None, None) => sys.error("at least one of EAF or MAF columns is required"); case _ => () }

    var otherCols = Seq(ColumnDef(AggregatorColumnNames.pvalue, PVALUE))

    EAF match {
      case Some(_) => 
        val eaf = EAF.get.asDouble
        otherCols = otherCols ++ Seq(ColumnDef(AggregatorColumnNames.eaf, eaf, 1.0 - eaf))
      case None => ()
    }

    BETA match {
      case Some(_) => 
        val beta = BETA.get.asDouble
        filteredSource = filteredSource.filter(BETA.get.asDouble < 42.0)
        otherCols = otherCols ++ Seq(ColumnDef(AggregatorColumnNames.beta, beta, -1.0 * beta))
      case None => ()
    }

    ZSCORE match {
      case Some(_) => 
        val zscore = ZSCORE.get.asDouble
        otherCols = otherCols ++ Seq(ColumnDef(AggregatorColumnNames.zscore, zscore, -1.0 * zscore))
      case None => ()
    }

    MAF match { case Some(_) => otherCols = otherCols ++ Seq(ColumnDef(AggregatorColumnNames.maf, MAF.get)); case None => () }
    STDERR match { case Some(_) => otherCols = otherCols ++ Seq(ColumnDef(AggregatorColumnNames.stderr, STDERR.get)); case None => () }
    N match { case Some(_) => otherCols = otherCols ++ Seq(ColumnDef(AggregatorColumnNames.n, N.get)); case None => () }

    val otherColumns = phenoCfg.dichotomous match {

      case true =>

        (BETA, ODDS_RATIO) match { case (None, None) => sys.error("dichotomous traits require at least one of BETA or ODDS_RATIO columns"); case _ => () }

        ODDS_RATIO match { 
          case Some(_) =>
            val odds_ratio = ODDS_RATIO.get.asDouble
            filteredSource = filteredSource.filter(odds_ratio > 0.0)
            otherCols = otherCols ++ Seq(ColumnDef(AggregatorColumnNames.odds_ratio, odds_ratio, 1.0 / odds_ratio))
          case None => ()
        }

        otherCols

      case false =>

        otherCols
    }
      
    UnsourcedRowDef(varId, otherColumns).from(filteredSource)
  }
  
  def processPhenotype(
      dataset: String,
      phenotype: String, 
      sourceStore: Store,
      phenoCfg: PhenotypeConfig,
      aggregatorIntakeConfig: AggregatorIntakeConfig,
      flipDetector: FlipDetector,
      destOpt: Option[Store] = None): Store = {
    
    require(sourceStore.isPathStore)

    val today: Date = new Date()
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(today)
    
    val dest: Store = destOpt.getOrElse(store(Paths.workDir / s"""${dataset}_${phenotype}.intake.tsv"""))
    
    val csvFormat = org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter(delToChar(phenoCfg.delimiter)).withFirstRecordAsHeader
    
    val source = CsvSource.fromCommandLine(s"zcat ${sourceStore.path}", csvFormat)
    
    val columns = rowDef(source, s"${sourceStore.path}", phenoCfg)
        
    produceCsv(dest)
      .from(columns)
      .using(flipDetector)
      .tag(s"process-phenotype-$phenotype")
      .in(sourceStore)
        
    dest
  }

  val generalMetadata: Metadata.NoPhenotypeOrQuantitative = {
    Metadata.NoPhenotypeOrQuantitative.fromConfig(intakeMetadataTypesafeConfig).get
  }
  
  val aggregatorIntakePipelineConfig: AggregatorIntakeConfig = {
    AggregatorIntakeConfig.fromConfig(intakeTypesafeConfig).get
  }
  
  final case class PhenotypeConfig(
    file: String, 
    delimiter: String,
    dichotomous: Boolean,
    subjects: Option[Int], 
    cases: Option[Int], 
    controls: Option[Int],
    CHROM: String,
    POS: String,
    REF: String,
    ALT: String,
    EAF: Option[String],
    MAF: Option[String],
    BETA: Option[String],
    STDERR: Option[String],
    ODDS_RATIO: Option[String],
    ZSCORE: Option[String],
    PVALUE: String,
    N: Option[String]) {

      def toQuantitative: Option[Metadata.Quantitative] = (subjects, cases, controls) match {
        case (_, Some(cs), Some(ctrls)) => Some(Metadata.Quantitative.CasesAndControls(cases = cs, controls = ctrls))
        case (Some(s), _, _) => Some(Metadata.Quantitative.Subjects(s))
        case _ => None
      }

  }
  
  val phenotypesToConfigs: Map[String, PhenotypeConfig] = {
    val key = "loamstream.aggregator.intake.phenotypesToFiles"
    
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    
    intakeTypesafeConfig.as[Map[String, PhenotypeConfig]](key)
  }
  
  import AggregatorCommands.upload
  
  def toMetadata(phenotypeConfigTuple: (String, PhenotypeConfig)): Metadata = {
    val (phenotype, phenotypeConfig) = phenotypeConfigTuple
    generalMetadata.toMetadata(phenotype, phenotypeConfig.toQuantitative)
  }
  
  val flipDetector: FlipDetector = new FlipDetector.Default(
    referenceDir = aggregatorIntakePipelineConfig.genomeReferenceDir,
    isVarDataType = true,
    pathTo26kMap = aggregatorIntakePipelineConfig.twentySixKIdMap)

  def fromColumnNames(columns: Iterable[ColumnName]): SourceColumns = {
    import loamstream.loam.intake.aggregator.ColumnNames
    type Setter = SourceColumns => SourceColumns
    import ColumnNames._
    def nameToSetter(columnName: ColumnName): Setter = columnName match {
      case `eaf` => _.withDefaultEaf
      case `maf` => _.withDefaultMaf
      case `beta` => _.withDefaultBeta
      case `odds_ratio` => _.withDefaultOddsRatio
      case `stderr` => _.withDefaultStderr
      case `zscore` => _.withDefaultZscore
      case `n` => _.withDefaultN
      case _ => identity
    }
    columns.foldLeft(SourceColumns.defaultMarkerAndPvalueOnly) { (acc, column) =>
      nameToSetter(column).apply(acc)
    }
  }
  
  for {
    (phenotype, sourceStore) <- sourceStores(phenotypesToConfigs)
  } {

    val phenotypeConfig = phenotypesToConfigs(phenotype)
    
    val dataInAggregatorFormat = processPhenotype(generalMetadata.dataset, phenotype, sourceStore, phenotypeConfig, aggregatorIntakePipelineConfig, flipDetector)

    val metadata = toMetadata(phenotype -> phenotypeConfig)

    var cols = Seq[ColumnName]()
    phenotypeConfig.EAF match { case Some(_) => cols = cols ++ Seq("eaf".asColumnName); case None => () }
    phenotypeConfig.BETA match { case Some(_) => cols = cols ++ Seq("beta".asColumnName); case None => () }
    phenotypeConfig.ZSCORE match { case Some(_) => cols = cols ++ Seq("zscore".asColumnName); case None => () }
    phenotypeConfig.MAF match { case Some(_) => cols = cols ++ Seq("maf".asColumnName); case None => () }
    phenotypeConfig.STDERR match { case Some(_) => cols = cols ++ Seq("stderr".asColumnName); case None => () }
    phenotypeConfig.N match { case Some(_) => cols = cols ++ Seq("n".asColumnName); case None => () }
    
    phenotypeConfig.dichotomous match { case true => phenotypeConfig.ODDS_RATIO match { case Some(_) => cols = cols ++ Seq("odds_ratio".asColumnName); case None => () }; case false => () }

    val sourceColumnMapping = fromColumnNames(cols)

    val aggregatorConfigFileName: Path = {
      Paths.workDir / s"aggregator-intake-${metadata.dataset}-${metadata.phenotype}.conf"
    }

    val configData = ConfigData(metadata, sourceColumnMapping, dataInAggregatorFormat.path)      
    val aggregatorConfigFile = store(aggregatorConfigFileName)

	println(s"${metadata}")
    produceAggregatorIntakeConfigFile(aggregatorConfigFile).
      from(configData).
      tag(s"make-aggregator-conf-${metadata.dataset}-${metadata.phenotype}").
      in(dataInAggregatorFormat)
    
    //if(intakeTypesafeConfig.getBoolean("AGGREGATOR_INTAKE_DO_UPLOAD")) {
    //
    //  upload(
    //    aggregatorIntakePipelineConfig, 
    //    metadata, 
    //    dataInAggregatorFormat,
    //    sourceColumnMapping,
    //    workDir = Paths.workDir, 
    //    yes = true).tag(s"upload-to-s3-${phenotype}")
    //
    //}

  }
}
