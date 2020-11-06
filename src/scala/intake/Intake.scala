import org.apache.commons.csv.CSVFormat

import com.typesafe.config.Config

import loamstream.loam.intake.AggregatorIntakeConfig
import loamstream.loam.intake.RowPredicate
import loamstream.loam.intake.SourceColumns

object Intake extends loamstream.LoamFile {
  import loamstream.loam.intake.IntakeSyntax._

  def delToChar(d: String): Char = d match {
    case "space" => ' '
    case "tab" => '\t'
    case "comma" => ','
    case _ => sys.error(s"file delimiter ${d} not recognized")
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

  def makeAggregatorRowExpr(phenoCfg: PhenotypeConfig): AggregatorRowExpr = {

    import phenoCfg.columnNames._
    
    val neff = ColumnName("Neff")
    val n = ColumnName("n")
    
    require(
        EAF.isDefined || MAF.isDefined, 
        s"at least one of EAF or MAF columns is required, but got EAF = $EAF and MAF = $MAF")

    val varId = AggregatorColumnDefs.marker(chromColumn = CHROM, posColumn = POS, refColumn = REF, altColumn = ALT)
        
    val oddsRatioDefOpt: Option[NamedColumnDef[Double]] = {
      if(phenoCfg.dichotomous) {
        require(
            BETA.isDefined || ODDS_RATIO.isDefined,
            s"Dichotomous traits require at least one of BETA or ODDS_RATIO columns, " +
            s"but got BETA = $BETA and ODDS_RATIO = $ODDS_RATIO")

        ODDS_RATIO.map(AggregatorColumnDefs.oddsRatio(_))
      } else {
        None
      }
    }
    
    AggregatorRowExpr(
        markerDef = varId,
        pvalueDef = AggregatorColumnDefs.PassThru.pvalue(PVALUE),
        zscoreDef = ZSCORE.map(AggregatorColumnDefs.zscore(_)),
        stderrDef = STDERR.map(AggregatorColumnDefs.PassThru.stderr(_)),
        betaDef = BETA.map(AggregatorColumnDefs.beta(_)),
        oddsRatioDef = oddsRatioDefOpt,
        eafDef = EAF.map(AggregatorColumnDefs.eaf(_)),
        mafDef = MAF.map(AggregatorColumnDefs.PassThru.maf(_)),
        nDef = N.map(AggregatorColumnDefs.PassThru.n(_)))
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
    
    val source = Source.fromCommandLine(s"zcat ${sourceStore.path}", csvFormat)
    
    val toAggregatorRows = makeAggregatorRowExpr(phenoCfg)
        
    //TODO: FIXME
    val filterLog: Store = store(path(s"${dest.path.toString}.filtered-rows"))
    //TODO: FIXME
    val unknownToBioIndexFile: Store = store(path(s"${dest.path.toString}.unknown-to-bio-index"))
    //TODO: FIXME
    val disagreeingZBetaStdErrFile: Store = store(path(s"${dest.path.toString}.disagreeing-z-Beta-stderr"))
    //TODO: FIXME
    val countFile: Store = store(path(s"${dest.path.toString}.variant-count"))
    
    val oddsRatioFilter: Option[RowPredicate] = phenoCfg.columnNames.ODDS_RATIO.map { oddsRatio =>
      CsvRowFilters.logToFile(filterLog, append = true) {
        oddsRatio.asDouble > 0.0
      }
    }
    
    import phenoCfg.columnNames
    
    produceCsv(dest).
        from(source).
        using(flipDetector).
        filter(CsvRowFilters.noDsNorIs(
            refColumn = columnNames.REF, 
            altColumn = columnNames.ALT, 
            logStore = filterLog,
            append = true)).
        filter(CsvRowFilters.filterRefAndAlt( //TODO: Example
            refColumn = columnNames.REF, 
            altColumn = columnNames.ALT, 
            disallowed = Set("foo", "BAR", "Baz"),
            logStore = filterLog,
            append = true)).
        filter(oddsRatioFilter).
        via(toAggregatorRows).
        filter(DataRowFilters.validEaf(filterLog, append = true)).
        filter(DataRowFilters.validMaf(filterLog, append = true)).
        map(DataRowTransforms.upperCaseAlleles).
        map(DataRowTransforms.clampPValues(filterLog, append = true)).
        filter(DataRowFilters.validPValue(filterLog, append = true)).
        withMetric(Metrics.count(countFile)).
        withMetric(Metrics.fractionUnknownToBioIndex(unknownToBioIndexFile)).
        withMetric(Metrics.fractionWithDisagreeingBetaStderrZscore(disagreeingZBetaStdErrFile, flipDetector)).
        write().
        tag(s"process-phenotype-$phenotype").
        in(sourceStore).
        out(dest).
        out(unknownToBioIndexFile).
        out(disagreeingZBetaStdErrFile).
        out(countFile).
        in(sourceStore)
        
    (dest, toAggregatorRows.sourceColumns)
  }

  val generalMetadata: AggregatorMetadata.NoPhenotypeOrQuantitative = {
    AggregatorMetadata.NoPhenotypeOrQuantitative.fromConfig(intakeMetadataTypesafeConfig).get
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

    def toQuantitative: Option[AggregatorMetadata.Quantitative] = (subjects, cases, controls) match {
      case (_, Some(cs), Some(ctrls)) => {
        Some(AggregatorMetadata.Quantitative.CasesAndControls(cases = cs, controls = ctrls))
      }
      case (Some(s), _, _) => Some(AggregatorMetadata.Quantitative.Subjects(s))
      case _ => None
    }
    
    val columnNames: ColumnNames = new ColumnNames(this)
  }
  
  final class ColumnNames(phenotypeConfig: PhenotypeConfig) {
    val CHROM: ColumnName = phenotypeConfig.CHROM.asColumnName
    val POS: ColumnName = phenotypeConfig.POS.asColumnName
    val REF: ColumnName = phenotypeConfig.REF.asColumnName
    val ALT: ColumnName = phenotypeConfig.ALT.asColumnName
    val EAF: Option[ColumnName] = phenotypeConfig.EAF.map(_.asColumnName)
    val MAF: Option[ColumnName] = phenotypeConfig.MAF.map(_.asColumnName)
    val BETA: Option[ColumnName] = phenotypeConfig.BETA.map(_.asColumnName)
    val STDERR: Option[ColumnName] = phenotypeConfig.STDERR.map(_.asColumnName)
    val ODDS_RATIO: Option[ColumnName] = phenotypeConfig.ODDS_RATIO.map(_.asColumnName)
    val ZSCORE: Option[ColumnName] = phenotypeConfig.ZSCORE.map(_.asColumnName)
    val PVALUE: ColumnName = phenotypeConfig.PVALUE.asColumnName
    val N: Option[ColumnName] = phenotypeConfig.N.map(_.asColumnName)
  }
  
  val phenotypesToConfigs: Map[String, PhenotypeConfig] = {
    val key = "loamstream.aggregator.intake.phenotypesToFiles"
    
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    
    intakeTypesafeConfig.as[Map[String, PhenotypeConfig]](key)
  }
  
  def toMetadata(phenotypeConfigTuple: (String, PhenotypeConfig)): AggregatorMetadata = {
    val (phenotype, phenotypeConfig) = phenotypeConfigTuple
    
    generalMetadata.toMetadata(phenotype, phenotypeConfig.toQuantitative)
  }
  
  val flipDetector: FlipDetector = new FlipDetector.Default(
    referenceDir = aggregatorIntakePipelineConfig.genomeReferenceDir,
    isVarDataType = true,
    pathTo26kMap = aggregatorIntakePipelineConfig.twentySixKIdMap)

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

      loamstream.loam.intake.AggregatorCommands.upload(
        aggregatorIntakePipelineConfig, 
        metadata, 
        dataInAggregatorFormat,
        sourceColumnMapping,
        workDir = Paths.workDir, 
        yes = false).tag(s"upload-to-s3-${phenotype}")
    }
  }
}

