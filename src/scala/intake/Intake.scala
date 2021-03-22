import org.apache.commons.csv.CSVFormat
import scala.sys.error

import com.typesafe.config.Config

import loamstream.loam.intake.AggregatorIntakeConfig
import loamstream.loam.intake.DataRowPredicate
import loamstream.loam.intake.ColumnDef

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

  val intakeUtilsConfig = loadConfig("INTAKE_UTILS_CONF", "")

  private val intakeTypesafeConfig: Config = loadConfig("INTAKE_CONF", "").config

  val makeQqPlot: Boolean = intakeTypesafeConfig.getBoolean("QQPLOT")
  val makeMhtPlot: Boolean = intakeTypesafeConfig.getBoolean("MHTPLOT")
  val makeTopResults: Boolean = intakeTypesafeConfig.getBoolean("TOPRESULTS")
  val splitByChr: Boolean = intakeTypesafeConfig.getBoolean("SPLITBYCHR")
  val mungeFile: Boolean = intakeTypesafeConfig.getBoolean("MUNGEFILE")
  
  private val intakeMetadataTypesafeConfig: Config = loadConfig("INTAKE_METADATA_CONF", "").config

  val imgPython2: String = intakeUtilsConfig.getStr("imgPython2")
  val imgR: String = intakeUtilsConfig.getStr("imgR")
  val imgEnsemblVep: String = intakeUtilsConfig.getStr("imgEnsemblVep")
  val imgTexLive: String = intakeUtilsConfig.getStr("imgTexLive")
  val pyQqPlot: String = intakeUtilsConfig.getStr("pyQqPlot")
  val pyMhtPlot: String = intakeUtilsConfig.getStr("pyMhtPlot")
  val pyTopResults: String = intakeUtilsConfig.getStr("pyTopResults")
  val pySummary: String = intakeUtilsConfig.getStr("pySummary")
  val shSplitByChr: String = intakeUtilsConfig.getStr("shSplitByChr")
  val shAnnotateResults: String = intakeUtilsConfig.getStr("shAnnotateResults")
  val pyMakeSiteVcf: String = intakeUtilsConfig.getStr("pyMakeSiteVcf")
  val rTop20: String = intakeUtilsConfig.getStr("rTop20")
  val texSummary: String = intakeUtilsConfig.getStr("texSummary")
  val fasta: Store = store(path(intakeUtilsConfig.getStr("fasta"))).asInput
  val vepCacheDir: Store = store(path(intakeUtilsConfig.getStr("vepCacheDir"))).asInput
  val vepPluginsDir: Store = store(path(intakeUtilsConfig.getStr("vepPluginsDir"))).asInput

  def makePValueVariantRowExpr(phenoCfg: PhenotypeConfig, metadata: AggregatorMetadata): VariantRowExpr.PValueVariantRowExpr = {

    import phenoCfg.columnNames._
    
    val neff = ColumnName("Neff")
    val n = ColumnName("n")
    
    val varId = AggregatorColumnDefs.marker(
        chromColumn = CHROM, 
        posColumn = POS, 
        refColumn = REF, 
        altColumn = ALT,
        forceAlphabeticChromNames = true) //"23" => "X", "24" => "Y", etc
        
    val oddsRatioDefOpt: Option[ColumnDef[Double]] = {
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
    
    VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
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
      metadata: AggregatorMetadata,
      dataset: String,
      phenotype: String, 
      sourceStore: Store,
      phenoCfg: PhenotypeConfig,
      aggregatorIntakeConfig: AggregatorIntakeConfig,
      flipDetector: FlipDetector,
      destOpt: Option[Store] = None): Store = {
    
    require(sourceStore.isPathStore)

    val today = java.time.LocalDate.now
    
    //NB: Munge LocalDate's string rep from yyyy-mm-dd to yyyy_mm_dd
    //val todayAsString = today.toString.replaceAll("-", "_")  
    
    val dest: Store = {
      destOpt.getOrElse(store(Paths.workDir / s"""${dataset}_${phenotype}.intake.tsv"""))
    }
    
    //TODO: FIXME path name chosen arbitrarily
    val filterLog: Store = store(path(s"${dest.path.toString}.filtered-rows"))

    //TODO: FIXME path name chosen arbitrarily
    //val unknownToBioIndexFile: Store = store(path(s"${dest.path.toString}.unknown-to-bio-index"))
    
    //TODO: FIXME path name chosen arbitrarily
    val disagreeingZBetaStdErrFile: Store = store(path(s"${dest.path.toString}.disagreeing-z-Beta-stderr"))
    
    //TODO: FIXME path name chosen arbitrarily
    val countFile: Store = store(path(s"${dest.path.toString}.variant-count"))
    
    val summaryStatsFile: Store = store(path(s"${dest.path.toString}.summaryStats"))
    
    val csvFormat = CSVFormat.DEFAULT.withDelimiter(delToChar(phenoCfg.delimiter)).withFirstRecordAsHeader
    
    val source = Source.fromCommandLine(s"zcat ${sourceStore.path}", csvFormat)
    
    val toAggregatorRows = makePValueVariantRowExpr(phenoCfg, metadata)
    
    val oddsRatioFilter: Option[DataRowPredicate] = phenoCfg.columnNames.ODDS_RATIO.map { oddsRatio =>
      DataRowFilters.logToFile(filterLog, append = true) {
        oddsRatio.asDouble > 0.0
      }
    }

    //?val betaFilter: Option[RowPredicate] = phenoCfg.columnNames.BETA.map { beta =>
    //?  CsvRowFilters.logToFile(filterLog, append = true) {
    //?    beta.asDouble < 42
    //?  }
    //?}
    val betaFilter: Option[DataRowPredicate] = phenoCfg.columnNames.BETA.map { beta =>
      DataRowFilters.logToFile(filterLog, append = true) {
        def isValid(b: Double): Boolean = b < 10.0 && b > -10.0
        beta.asDouble.map(isValid)
      }
    }     
    
    import phenoCfg.columnNames

    if(mungeFile) {

      drm {
        uploadTo(
            bucketName = "dig-integration-tests",
            uploadType = toAggregatorRows.uploadType,
            metadata = metadata).
        from(source).
        using(flipDetector).
        //Filter out rows with REF or ALT columns == ('D' or 'I')
        filter(DataRowFilters.noDsNorIs(
            refColumn = columnNames.REF, 
            altColumn = columnNames.ALT, 
            logStore = filterLog,
            append = true)).
        filter(oddsRatioFilter). //if ODDS_RATIO is present, only keep rows with ODDS_RATIO > 0.0
        filter(betaFilter). //if BETA is present, only keep rows with -10.0 < BETA < 10.0
        via(toAggregatorRows).
        filter(AggregatorVariantRowFilters.validEaf(filterLog, append = true)). //(eaf > 0.0) && (eaf < 1.0)
        filter(AggregatorVariantRowFilters.validMaf(filterLog, append = true)). //(maf > 0.0) && (maf <= 0.5)
        map(DataRowTransforms.upperCaseAlleles). // "aTgC" => "ATGC"
        map(DataRowTransforms.clampPValues(filterLog, append = true)). //0.0 => min pos value 
        filter(AggregatorVariantRowFilters.validPValue(filterLog, append = true)). //(pvalue > 0.0) && (pvalue < 1.0)
        withMetric(Metrics.count(countFile)).
        withMetric(Metrics.fractionWithDisagreeingBetaStderrZscore(disagreeingZBetaStdErrFile, flipDetector)).
        withMetric(Metrics.writeSummaryStatsTo(summaryStatsFile)).
        write(forceLocal = true).
        in(sourceStore).
        out(dest, filterLog, disagreeingZBetaStdErrFile, countFile, summaryStatsFile).
        tag(s"process-phenotype-$phenotype")
    
        //add this back if time not a concern
        //withMetric(Metrics.fractionUnknownToBioIndex(unknownToBioIndexFile)).
        //out(unknownToBioIndexFile).
    
        //replace with this if want to keep it running locally
      }

    }
        
    dest
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

  val generalMetadata: AggregatorMetadata.NoPhenotypeOrQuantitative = {
    AggregatorMetadata.NoPhenotypeOrQuantitative.fromConfig(intakeMetadataTypesafeConfig).get
  }
  
  val aggregatorIntakePipelineConfig: AggregatorIntakeConfig = {
    AggregatorIntakeConfig.fromConfig(intakeTypesafeConfig).get
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
    
    val metadata = toMetadata(phenotype -> phenotypeConfig)
    
    val aggregatorConfigFile = store(Paths.workDir / s"""aggregator-intake-${metadata.dataset}-${metadata.phenotype}.conf""")
    
    val dataInAggregatorFormat = {
      processPhenotype(
        metadata,
        generalMetadata.dataset, 
        phenotype, 
        sourceStore, 
        phenotypeConfig, 
        aggregatorIntakePipelineConfig, 
        flipDetector)
    }
    
    produceAggregatorIntakeConfigFile(aggregatorConfigFile)
      .from(metadata, forceLocal = true)
      .in(dataInAggregatorFormat)
      .tag(s"make-aggregator-conf-${metadata.dataset}-${metadata.phenotype}")
    
    val qqPlot: Store = store(Paths.workDir / s"""${generalMetadata.dataset}_${phenotype}.intake.qqplot.png""")
    val qqPlotCommon: Store = store(Paths.workDir / s"""${generalMetadata.dataset}_${phenotype}.intake.qqplot.common.png""")
    val mhtPlot: Store = store(Paths.workDir / s"""${generalMetadata.dataset}_${phenotype}.intake.mhtplot.png""")
    val topResults: Store = store(Paths.workDir / s"""${generalMetadata.dataset}_${phenotype}.intake.topresults.tsv""")
    val resultsMht: Store = store(Paths.workDir / s"""${generalMetadata.dataset}_${phenotype}.intake.1e-4.tsv""")
    val topResultsAnnot: Store = store(Paths.workDir / s"""${generalMetadata.dataset}_${phenotype}.intake.topresults.annot.tsv""")
    val topLociAnnot: Store = store(Paths.workDir / s"""${generalMetadata.dataset}_${phenotype}.intake.toploci.annot.tsv""")
    val siteVcf: Store = store(Paths.workDir / s"""${generalMetadata.dataset}_${phenotype}.intake.sites.vcf.gz""")
    val texReport: Store = store(Paths.workDir / s"""${generalMetadata.dataset}_${phenotype}.intake.report.tex""")
    val pdfReport: Store = store(Paths.workDir / s"""${generalMetadata.dataset}_${phenotype}.intake.report.pdf""")

    if(makeTopResults) {

      drmWith(imageName = s"${imgPython2}") {
        cmd"""/usr/local/bin/python ${pyTopResults}
          --results ${dataInAggregatorFormat}
          --n 1000
          --p pvalue
          --out ${topResults}
          --out-mht ${resultsMht}"""
          .in(dataInAggregatorFormat)
          .out(topResults, resultsMht)
          .tag(s"process-phenotype-${phenotype}-topresults")
        
        cmd"""/usr/local/bin/python ${pyMakeSiteVcf}
          --results ${topResults}
          --out ${siteVcf}"""
          .in(topResults)
          .out(siteVcf)
          .tag(s"process-phenotype-${phenotype}-sitevcf")
      }

      drmWith(imageName = s"${imgEnsemblVep}") {
        cmd"""${shAnnotateResults}
          ${siteVcf}
          ${topResults}
          1
          ${fasta}
          ${vepCacheDir}
          ${vepPluginsDir}
          ${topResultsAnnot}"""
          .in(siteVcf, topResults, fasta, vepCacheDir, vepPluginsDir)
          .out(topResultsAnnot)
          .tag(s"process-phenotype-${phenotype}-topresultsannot")
      }

      drmWith(imageName = s"${imgR}") {
        cmd"""/usr/local/bin/Rscript --vanilla --verbose
          ${rTop20}
          --results ${topResultsAnnot}
          --p pvalue
          --out ${topLociAnnot}"""
          .in(topResultsAnnot)
          .out(topLociAnnot)
          .tag(s"process-phenotype-${phenotype}-toplociannot")
      }

    }

    //if(intakeTypesafeConfig.getBoolean("AGGREGATOR_INTAKE_DO_UPLOAD")) {
    //  val metadata = toMetadata(phenotype -> phenotypeConfig)
    //
    //  drm {
    //    loamstream.loam.intake.AggregatorCommands.upload(
    //      aggregatorIntakePipelineConfig, 
    //      metadata, 
    //      dataInAggregatorFormat,
    //      sourceColumnMapping,
    //      workDir = Paths.workDir, 
    //      yes = false,
    //      forceLocal = false
    //    ).tag(s"upload-to-s3-${phenotype}")
    //  }
    //
    //}

    if(makeQqPlot) {

      val eafString = phenotypeConfig.EAF match {
        case Some(s) => "--eaf eaf"
        case None => ""
      }

      val commonString = phenotypeConfig.EAF match {
        case Some(s) => s"--out-common ${qqPlotCommon.toString.split("@")(1)}"
        case None => ""
      }

      var qqplotOut = Seq(qqPlot)
      phenotypeConfig.EAF match {
        case Some(s) => qqplotOut :+ qqPlotCommon
        case None => ()
      }

      drmWith(imageName = s"${imgPython2}") {
        cmd"""/usr/local/bin/python ${pyQqPlot}
          --results ${dataInAggregatorFormat}
          --p pvalue
          ${eafString}
          --out ${qqPlot}
          ${commonString}"""
          .in(dataInAggregatorFormat)
          .out(qqplotOut)
          .tag(s"process-phenotype-${phenotype}-qqplot")
      }

    }

    if(makeMhtPlot) {

      drmWith(imageName = s"${imgPython2}") {
        cmd"""/usr/local/bin/python ${pyMhtPlot}
          --results ${resultsMht}
          --full-results ${dataInAggregatorFormat}
          --p pvalue
          --out ${mhtPlot}"""
          .in(dataInAggregatorFormat, resultsMht)
          .out(mhtPlot)
          .tag(s"process-phenotype-${phenotype}-mhtplot")
      }

    }

    val qqString = makeQqPlot match {
      case true => 
        phenotypeConfig.EAF match {
          case Some(s) =>
            s"--qq ${qqPlot.toString.split("@")(1)} --qq-common ${qqPlotCommon.toString.split("@")(1)}"
          case None =>
            s"--qq ${qqPlot.toString.split("@")(1)}"
        }
      case false => ""
    }

    val summaryIn = Seq(mhtPlot, topLociAnnot) ++ {
      if(makeQqPlot && phenotypeConfig.EAF.isDefined) Seq(qqPlot, qqPlotCommon) else Nil
    }

	drmWith(imageName = s"${imgPython2}") {
      
      cmd"""/usr/local/bin/python ${pySummary}
        --cfg ${aggregatorConfigFile}
        ${qqString}
        --mht ${mhtPlot}
        --top-results ${topLociAnnot}
        --out ${texReport}"""
        .in(summaryIn)
        .out(texReport)
        .tag(s"process-phenotype-${phenotype}-texreport")
    }

    drmWith(imageName = s"${imgTexLive}") {
      
      cmd"""bash -c "/usr/local/bin/pdflatex ${texReport}; sleep 5; /usr/local/bin/pdflatex ${texReport}""""
        .in(texReport)
        .out(pdfReport)
        .tag(s"process-phenotype-${phenotype}-pdfreport")
    
    }


  }
}

