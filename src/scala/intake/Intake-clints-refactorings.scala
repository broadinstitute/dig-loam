import com.typesafe.config.Config

import loamstream.loam.intake.aggregator.AggregatorIntakeConfig
import loamstream.loam.intake.aggregator.ColumnDefs
import loamstream.loam.intake.aggregator.Metadata
import loamstream.loam.intake.aggregator.SourceColumns
import org.apache.commons.csv.CSVFormat

object Intake extends loamstream.LoamFile {
  import loamstream.loam.intake.IntakeSyntax._

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

    def dataFile(s: String): Path = path(s)
  }
  
  def sourceStore(phenotypeConfig: PhenotypeConfig): Store = {
    store(Paths.dataFile(phenotypeConfig.file))
  }
  
  def sourceStores(phenotypesToFiles: Map[String, PhenotypeConfig]): Map[String, Store] = {
    import loamstream.util.Maps.Implicits._
    
    phenotypesToFiles.strictMapValues(sourceStore(_).asInput)
  }

  private val intakeTypesafeConfig: Config = loadConfig("INTAKE_CONF", "").config
  
  private val intakeMetadataTypesafeConfig: Config = loadConfig("INTAKE_METADATA_CONF", "").config

  def makeRowDef(phenoCfg: PhenotypeConfig): UnsourcedRowDef = {

    import phenoCfg.columnNames._

    val neff = ColumnName("Neff")
    val n = ColumnName("n")
    
    require(
        EAF.isDefined || MAF.isDefined, 
        s"at least one of EAF or MAF columns is required, but got EAF = $EAF and MAF = $MAF")

    val varId = ColumnDefs.marker(chromColumn = CHROM, posColumn = POS, refColumn = REF, altColumn = ALT)
        
    val otherColumns: Seq[UnsourcedColumnDef] = {
      val baseColumns = {
        Seq(
          Some(ColumnDefs.PassThru.pvalue(PVALUE)),
          EAF.map(ColumnDefs.eaf(_)),
          BETA.map(ColumnDefs.beta(_)),
          ZSCORE.map(ColumnDefs.zscore(_)),
          MAF.map(ColumnDefs.PassThru.maf(_)),
          STDERR.map(ColumnDefs.PassThru.n(_))).flatten
      }
      
      val oddsRatioPart = {
        if(phenoCfg.dichotomous) {
          require(
              BETA.isDefined || ODDS_RATIO.isDefined,
              s"Dichotomous traits require at least one of BETA or ODDS_RATIO columns, " +
              s"but got BETA = $BETA and ODDS_RATIO = $ODDS_RATIO")
  
          ODDS_RATIO.map(ColumnDefs.oddsRatio(_))
        } else {
          None
        }
      }
      
      baseColumns ++ oddsRatioPart
    }
    
    UnsourcedRowDef(varId, otherColumns)
  }
  
  def filteredSource(phenotypeConfig: PhenotypeConfig, source: CsvSource): CsvSource = { 
    val withOddsRatioFilter = phenotypeConfig.columnNames.ODDS_RATIO match { 
      case Some(oddsRatio) => source.filter(oddsRatio.asDouble > 0.0)
      case None => source
    }
    
    import phenotypeConfig.columnNames.ALT
    import phenotypeConfig.columnNames.REF
    
    withOddsRatioFilter.filter(REF !== "D").filter(REF !== "I").filter(ALT !== "D").filter(ALT !== "I")
  }
  
  def processPhenotype(
      dataset: String,
      phenotype: String, 
      sourceStore: Store,
      phenoCfg: PhenotypeConfig,
      aggregatorIntakeConfig: AggregatorIntakeConfig,
      flipDetector: FlipDetector,
      destOpt: Option[Store] = None): (Store, SourceColumns) = {
    
    require(sourceStore.isPathStore)

    val today = java.time.LocalDate.now
    
    //NB: Munge LocalDate's string rep from yyyy-mm-dd to yyyy_mm_dd
    val todayAsString = today.toString.replaceAll("-", "_")  
    
    val dest: Store = {
      destOpt.getOrElse(store(Paths.workDir / s"""${dataset}_${phenotype}.intake_${todayAsString}.tsv"""))
    }
    
    val csvFormat = CSVFormat.DEFAULT.withDelimiter(delToChar(phenoCfg.delimiter)).withFirstRecordAsHeader
    
    val source = filteredSource(
        phenoCfg, 
        CsvSource.fromCommandLine(s"zcat ${sourceStore.path}", csvFormat))
    
    val rowDef = makeRowDef(phenoCfg).from(source)
        
    produceCsv(dest)
      .from(rowDef)
      .using(flipDetector)
      .tag(s"process-phenotype-$phenotype")
      .in(sourceStore)
        
    (dest, sourceColumnsFromRowDef(rowDef))
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

    def toQuantitative: Metadata.Quantitative = (subjects, cases, controls) match {
      case (_, Some(cs), Some(ctrls)) => Metadata.Quantitative.CasesAndControls(cases = cs, controls = ctrls)
      case (Some(s), _, _) => Metadata.Quantitative.Subjects(s)
      case _ => sys.error("Couldn't find subjects or both cases and controls")
    }
    
    val columnNames: ColumnNames = new ColumnNames(this)
  }
  
  final class ColumnNames(phenotypeConfig: PhenotypeConfig) {
    val CHROM = phenotypeConfig.CHROM.asColumnName
    val POS = phenotypeConfig.POS.asColumnName
    val REF = phenotypeConfig.REF.asColumnName
    val ALT = phenotypeConfig.ALT.asColumnName
    val EAF = phenotypeConfig.EAF.map(_.asColumnName)
    val MAF = phenotypeConfig.MAF.map(_.asColumnName)
    val BETA = phenotypeConfig.BETA.map(_.asColumnName)
    val STDERR = phenotypeConfig.STDERR.map(_.asColumnName)
    val ODDS_RATIO = phenotypeConfig.ODDS_RATIO.map(_.asColumnName)
    val ZSCORE = phenotypeConfig.ZSCORE.map(_.asColumnName)
    val PVALUE = phenotypeConfig.PVALUE.asColumnName
    val N = phenotypeConfig.N.map(_.asColumnName)
  }
  
  val phenotypesToConfigs: Map[String, PhenotypeConfig] = {
    val key = "loamstream.aggregator.intake.phenotypesToFiles"
    
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    
    intakeTypesafeConfig.as[Map[String, PhenotypeConfig]](key)
  }
  
  import loamstream.loam.intake.aggregator.AggregatorCommands.upload
  
  def toMetadata(phenotypeConfigTuple: (String, PhenotypeConfig)): Metadata = {
    val (phenotype, phenotypeConfig) = phenotypeConfigTuple
    
    generalMetadata.toMetadata(phenotype, phenotypeConfig.toQuantitative)
  }
  
  val flipDetector: FlipDetector = new FlipDetector.Default(
    referenceDir = aggregatorIntakePipelineConfig.genomeReferenceDir,
    isVarDataType = true,
    pathTo26kMap = aggregatorIntakePipelineConfig.twentySixKIdMap)

  def sourceColumnsFromColumnNames(columns: Iterable[ColumnName]): SourceColumns = {
    import loamstream.loam.intake.aggregator.ColumnNames._
    
    def nameToSetter(columnName: ColumnName): SourceColumns => SourceColumns = columnName match {
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
      nameToSetter(column)(acc)
    }
  }
  
  def sourceColumnsFromRowDef(rowDef: RowDef): SourceColumns = {
    val columnsFromRowDef = rowDef.varIdDef +: rowDef.otherColumns
    
    sourceColumnsFromColumnNames(columnsFromRowDef.map(_.columnDef.name.mapName(_.toLowerCase)))
  }
  
  for {
    (phenotype, sourceStore) <- sourceStores(phenotypesToConfigs)
  } {
    val phenotypeConfig = phenotypesToConfigs(phenotype)
    
    val (dataInAggregatorFormat, sourceColumnMapping) = {
      processPhenotype(
          generalMetadata.dataset, 
          phenotype, 
          sourceStore, 
          phenotypeConfig, 
          aggregatorIntakePipelineConfig, 
          flipDetector)
    }
    
    if(intakeTypesafeConfig.getBoolean("AGGREGATOR_INTAKE_DO_UPLOAD")) {
      val metadata = toMetadata(phenotype -> phenotypeConfig)

      upload(
        aggregatorIntakePipelineConfig, 
        metadata, 
        dataInAggregatorFormat,
        sourceColumnMapping,
        workDir = Paths.workDir, 
        yes = false).tag(s"upload-to-s3-${phenotype}")
    }
  }
}
