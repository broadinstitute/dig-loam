object ProjectConfig extends loamstream.LoamFile {

  import Fxns._
  import loamstream.googlecloud.ClusterConfig

  val refGenomes = Seq("GRCh37","GRCh38")
  val ancestryCodes = Seq("EUR","AFR","AMR","SAS","EAS")

  val modelDesigns = Seq("full","strat")
  val modelTrans = Seq("log","invn")
  val assocTests = Seq(
    "hail.q.lm",
    "hail.b.wald",
    "hail.b.firth",
    "hail.b.lrt",
    "hail.b.score",
    "epacts.b.burden",
    "epacts.b.burdenFirth",
    "epacts.b.collapse",
    "epacts.b.madsen",
    "epacts.b.wcnt",
    "epacts.q.burden",
    "epacts.q.reverse",
    "epacts.q.wilcox",
    "epacts.b.skat",
    "epacts.b.VT",
    "epacts.b.emmaxCMC",
    "epacts.b.emmaxVT",
    "epacts.q.mmskat",
    "epacts.q.skat",
    "epacts.q.VT",
    "epacts.q.emmaxCMC",
    "epacts.q.emmaxVT"
  )
  
  val famTests = assocTests.filter(e => e.split("\\.")(2).startsWith("emmax"))
  val groupTests = assocTests.filter(e => e.split("\\.")(0) == "epacts")
  val singleTests = assocTests.filter(e => e.split("\\.")(0) == "hail")
  val nonHailTests = assocTests.filter(e => e.split("\\.")(0) != "hail")
  
  final case class ConfigMachine(
    cpus: Int,
    mem: Int,
    maxRunTime: Int) extends Debug
  
  final case class ConfigNumericFilters(
    id: String,
    field: String,
    range: String,
    missingFalse: Boolean,
    expression: String) extends Debug
  
  final case class ConfigBooleanFilters(
    id: String,
    field: String,
    value: Boolean,
    expression: String) extends Debug
  
  final case class ConfigCategoricalFilters(
    id: String,
    field: String,
    incl: Option[Seq[String]],
    excl: Option[Seq[String]],
    substrings: Boolean,
    expression: String) extends Debug
  
  final case class ConfigCompoundFilters(
    id: String,
    filter: String,
    include: Seq[String],
    expression: String) extends Debug
  
  final case class ConfigCloudResources(
    mtCluster: ClusterConfig,
    variantHtCluster: ClusterConfig) extends Debug
  
  final case class ConfigResources(
    matrixTableHail: ConfigMachine,
    tableHail: ConfigMachine,
    standardPlink: ConfigMachine,
    highMemPlink: ConfigMachine,
    standardR: ConfigMachine,
    highMemR: ConfigMachine,
    flashPca: ConfigMachine,
    standardPython: ConfigMachine,
    king: ConfigMachine,
    vep: ConfigMachine,
    klustakwik: ConfigMachine,
    tabix: ConfigMachine,
    lowMemEpacts: ConfigMachine,
    midMemEpacts: ConfigMachine,
    highMemEpacts: ConfigMachine,
    locuszoom: ConfigMachine) extends Debug

  final case class ConfigInputStore(
    local: Option[String],
    google: Option[String]) extends Debug

  final case class ConfigArrayQc(
    config: String,
    arrayId: String,
    baseDir: String) extends Debug

  final case class ConfigArray(
    id: String,
    qc: ConfigArrayQc,
    qcProjectId: String,
    qcCloudHome: Option[String],
    qcHailCloud: Boolean,
    phenoFile: String,
    phenoFileId: String,
    qcSampleFile: String,
    qcSampleFileId: String,
    qcSampleFileSrSex: String,
    qcSampleFileMaleCode: String,
    qcSampleFileFemaleCode: String,
    exportCleanVcf: Boolean) extends Debug
  
  final case class ConfigCohort(
    id: String,
    array: String,
    ancestry: Seq[String],
    stratCol: Option[String],
    stratCodes: Option[Seq[String]],
    minPartitions: Option[Int]) extends Debug
  
  final case class ConfigMeta(
    id: String,
    cohorts: Seq[String],
    minPartitions: Option[Int]) extends Debug
  
  final case class ConfigMerge(
    id: String,
    cohorts_metas: Seq[String],
    minPartitions: Option[Int]) extends Debug
  
  final case class ConfigPheno(
    id: String,
    name: String,
    binary: Boolean,
    desc: String) extends Debug
  
  final case class ConfigKnown(
    id: String,
    data: String,
    hiLd: String,
    n: String,
    nCase: String,
    nCtrl: String,
    desc: String,
    citation: String) extends Debug
  
  final case class CohortFilter(
    cohort: String,
    filters: Seq[String]) extends Debug
  
  final case class MaskFilter(
    id: String,
    filters: Seq[String],
    groupFile: Option[String]) extends Debug
  
  final case class CohortGroupFile(
    cohort: String,
    groupFile: String) extends Debug
  
  final case class ConfigSchema(
    id: String,
    design: String,
    cohorts: Seq[String],
    samplesKeep: Option[String],
    samplesExclude: Option[String],
    filters: Option[Seq[String]],
    cohortFilters: Option[Seq[CohortFilter]],
    knockoutFilters: Option[Seq[CohortFilter]],
    masks: Option[Seq[MaskFilter]],
    maxGroupSize: Option[Int],
    filterCohorts: Seq[String]) extends Debug
  
  final case class ConfigModel(
    id: String,
    schema: String,
    pheno: String,
    trans: Option[String],
    tests: Seq[String],
    assocPlatforms: Seq[String],
    maxPcaOutlierIterations: Int,
    covars: String,
    cohorts: Seq[String],
    metas: Option[Seq[String]],
    merges: Option[Seq[String]],
    knowns: Option[Seq[String]],
    runAssoc: Boolean) extends Debug
  
  //final case class ConfigSection(
  //  id: String,
  //  title: String,
  //  models: Seq[String]) extends Debug
  
  //final case class ConfigReport(
  //  id: String,
  //  name: String,
  //  sections: Seq[ConfigSection]) extends Debug
  
  final case class ProjectConfig(
    loamstreamVersion: String,
    pipelineVersion: String,
    referenceGenome: String,
    hailCloud: Boolean,
    cloudShare: Option[URI],
    cloudHome: Option[URI],
    projectId: String,
    geneIdMap: String,
    fasta: String,
    vepCacheDir: String,
    vepPluginsDir: String,
    dbNSFP: String,
    authors: Seq[String],
    email: String,
    organization: String,
    acknowledgementsAnalysisReport: Option[Seq[String]],
    minPCs: Int,
    maxPCs: Int,
    nStddevs: Int,
    diffMissMinExpectedCellCount: Int,
    cloudResources: ConfigCloudResources,
    resources: ConfigResources,
    nArrays: Int,
    nCohorts: Int,
    nMetas: Int,
    maxSigRegions: Option[Int],
    numericVariantFilters: Seq[ConfigNumericFilters],
    booleanVariantFilters: Seq[ConfigBooleanFilters],
    categoricalVariantFilters: Seq[ConfigCategoricalFilters],
    compoundVariantFilters: Seq[ConfigCompoundFilters],
    numericSampleFilters: Seq[ConfigNumericFilters],
    booleanSampleFilters: Seq[ConfigBooleanFilters],
    categoricalSampleFilters: Seq[ConfigCategoricalFilters],
    compoundSampleFilters: Seq[ConfigCompoundFilters],
    Arrays: Seq[ConfigArray],
    Cohorts: Seq[ConfigCohort],
    Metas: Seq[ConfigMeta],
    Merges: Seq[ConfigMerge],
    Knowns: Seq[ConfigKnown],
    Phenos: Seq[ConfigPheno],
    Schemas: Seq[ConfigSchema],
    Models: Seq[ConfigModel]
    //Reports: Seq[ConfigReport]
    ) extends Debug
  
  final case class Image(
    imgHail: Path,
    imgLocuszoom: Path,
    imgPython2: Path,
    imgR: Path,
    imgTools: Path,
    imgTexLive: Path,
    imgEnsemblVep: Path,
    imgFlashPca: Path,
    imgUmichStatgen: Path) extends Debug
  
  final case class Binary(
    binLiftOver: Path,
    binGenotypeHarmonizer: Path,
    binKing: Path,
    binPlink: Path,
    binTabix: Path,
    binGhostscript: Path,
    binKlustakwik: Path,
    binPython: Path,
    binLocuszoom: Path,
    binPdflatex: Path,
    binRscript: Path,
    binFlashPca: Path,
    binEpacts: Path) extends Debug
  
  final case class Python(
    pyAlignNon1kgVariants: Path,
    pyHailLoad: Path,
    pyHailLoadAnnotations: Path,
    pyHailExportQcData: Path,
    pyHailFilter: Path,
    pyHailAncestryPcaMerge1kg: Path,
    pyHailPcaMerge1kg: Path,
    pyHailSampleqc: Path,
    pyMakeSamplesRestoreTable: Path,
    pyCompileExclusions: Path,
    pyMergeVariantLists: Path,
    pyBimToUid: Path,
    pyHailUtils: Path,
    pyHailSchemaVariantStats: Path,
    pyHailSchemaVariantCaseCtrlStats: Path,
    pyHailAssoc: Path,
    pyHailExportCleanArrayData: Path,
    pyHailFilterSchemaVariants: Path,
    pyHailFilterSchemaPhenoVariants: Path,
    pyHailGenerateGroupfile: Path,
    pyHailGenerateModelVcf: Path,
    pyQqPlot: Path,
    pyMhtPlot: Path,
    pyTopResults: Path,
    pyTopGroupResults: Path,
    pyExtractTopRegions: Path,
    pyPhenoDistPlot: Path
    //pyAddGeneAnnot: Path
    //pyHailModelVariantStats: Path,
    //pyHailFilterModelVariants: Path,
    //pyHailFilterResults: Path,
    //pyHailMerge: Path,
    //pyHailMetaAnalysis: Path,
    //pyGenerateReportHeader: Path,
    //pyGenerateQcReportIntro: Path,
    //pyGenerateQcReportData: Path,
    //pyGenerateQcReportAncestry: Path,
    //pyGenerateQcReportIbdSexcheck: Path,
    //pyGenerateQcReportSampleqc: Path,
    //pyGenerateQcReportVariantqc: Path,
    //pyGenerateQcReportBibliography: Path,
    //pyGenerateAnalysisReportIntro: Path,
    //pyGenerateAnalysisReportData: Path,
    //pyGenerateAnalysisReportStrategy: Path,
    //pyGenerateAnalysisReportPhenoSummary: Path,
    //pyGenerateAnalysisReportPhenoCalibration: Path,
    //pyGenerateAnalysisReportPhenoTopLoci: Path,
    //pyGenerateAnalysisReportPhenoKnownLoci: Path,
    //pyGenerateAnalysisReportBibliography: Path
    ) extends Debug
  
  final case class Bash(
    shFindPossibleDuplicateVariants: Path,
    shAnnotate: Path,
    shAnnotateResults: Path,
    shKing: Path,
    shPlinkPrepare: Path,
    shPlinkToVcfNoHalfCalls: Path,
    shKlustakwikPca: Path,
    shKlustakwikMetric: Path,
    shCrossCohortCommonVariants: Path,
    shFlashPca: Path,
    shEpacts: Path,
    shMergeResults: Path,
    shRegPlot: Path
    //shTopResultsAddGenes: Path
    ) extends Debug
  
  final case class R(
    rFindBestDuplicateVariants: Path,
    rAncestryClusterMerge: Path,
    rCalcKinshipFamSizes: Path,
    rPlotAncestryPca: Path,
    rPlotAncestryCluster: Path,
    rIstatsPcsGmmClusterPlot: Path,
    rIstatsAdjGmmPlotMetrics: Path,
    rCalcIstatsAdj: Path,
    rIstatsAdjPca: Path,
    rModelCohortSamplesAvailable: Path,
    rSchemaCohortSamplesAvailable: Path,
    rMetaCohortSamples: Path,
    rExcludeCrossArray: Path,
    rGeneratePheno: Path,
    rConvertPhenoToPed: Path,
    rTop20: Path,
    rRawVariantsSummaryTable: Path
    //rAncestryClusterTable: Path,
    //rPcair: Path,
    //rUpsetplotBimFam: Path,
    //rMakeOutlierTable: Path,
    //rMakeMetricDistPlot: Path,
    //rTop50Known: Path,
    //rMetaExclusionsTable: Path
    ) extends Debug
  
  final case class Utils(
    imagesDir: Path,
    scriptsDir: Path,
    image: Image,
    binary: Binary,
    python: Python,
    bash: Bash,
    r: R) extends Debug
  
  object ProjectConfig {
  
    def parseConfig(config: loamstream.conf.DataConfig): ProjectConfig = {
  
      // required global values in conf file
      val loamstreamVersion = requiredStr(config = config, field = "loamstreamVersion")
      val pipelineVersion = requiredStr(config = config, field = "pipelineVersion")
      val projectId = requiredStr(config = config, field = "projectId")
      val referenceGenome = requiredStr(config = config, field = "referenceGenome", regex = refGenomes.mkString("|"))
      val hailCloud = requiredBool(config = config, field = "hailCloud")
      val cloudShare = optionalStr(config = config, field = "cloudShare") match { case Some(s) => Some(uri(s)); case None => None }
      val cloudHome = optionalStr(config = config, field = "cloudHome") match { case Some(s) => Some(uri(s)); case None => None }
      val geneIdMap = requiredStr(config = config, field = "geneIdMap")
      val fasta = requiredStr(config = config, field = "fasta")
      val vepCacheDir = requiredStr(config = config, field = "vepCacheDir")
      val vepPluginsDir = requiredStr(config = config, field = "vepPluginsDir")
      val dbNSFP = requiredStr(config = config, field = "dbNSFP")
      val authors = requiredStrList(config = config, field = "authors")
      val email = requiredStr(config = config, field = "email")
      val organization = requiredStr(config = config, field = "organization")
      val acknowledgementsAnalysisReport = optionalStrList(config = config, field = "acknowledgementsAnalysisReport")
      val maxSigRegions = optionalInt(config = config, field = "maxSigRegions", min = Some(0))
      val minPCs = requiredInt(config = config, field = "minPCs", min = Some(0), max = Some(20))
      val maxPCs = requiredInt(config = config, field = "maxPCs", min = Some(0), max = Some(20))
      val nStddevs = requiredInt(config = config, field = "nStddevs", min = Some(1))
      val diffMissMinExpectedCellCount = requiredInt(config = config, field = "diffMissMinExpectedCellCount", min = Some(0), default = Some(5))
  
      val cloudResources = ConfigCloudResources(
        mtCluster = {
          val thisConfig = optionalObj(config = config, field = "mtCluster")
          thisConfig match {
            case Some(o) => 
              ClusterConfig(
                zone = requiredStr(config = o, field = "zone"),
                properties = requiredStr(config = o, field = "properties"),
                masterMachineType = requiredStr(config = o, field = "masterMachineType"),
                masterBootDiskSize = requiredInt(config = o, field = "masterBootDiskSize"),
                workerMachineType = requiredStr(config = o, field = "workerMachineType"),
                workerBootDiskSize = requiredInt(config = o, field = "workerBootDiskSize"),
                numWorkers = requiredInt(config = o, field = "numWorkers"),
                numPreemptibleWorkers = requiredInt(config = o, field = "numPreemptibleWorkers"),
                preemptibleWorkerBootDiskSize = requiredInt(config = o, field = "preemptibleWorkerBootDiskSize"),
                maxClusterIdleTime = requiredStr(config = o, field = "maxClusterIdleTime")
              )
            case None => ClusterConfig()
          }
        },
        variantHtCluster = {
          val thisConfig = optionalObj(config = config, field = "variantHtCluster")
          thisConfig match {
            case Some(o) => 
              ClusterConfig(
                zone = requiredStr(config = o, field = "zone"),
                properties = requiredStr(config = o, field = "properties"),
                masterMachineType = requiredStr(config = o, field = "masterMachineType"),
                masterBootDiskSize = requiredInt(config = o, field = "masterBootDiskSize"),
                workerMachineType = requiredStr(config = o, field = "workerMachineType"),
                workerBootDiskSize = requiredInt(config = o, field = "workerBootDiskSize"),
                numWorkers = requiredInt(config = o, field = "numWorkers"),
                numPreemptibleWorkers = requiredInt(config = o, field = "numPreemptibleWorkers"),
                preemptibleWorkerBootDiskSize = requiredInt(config = o, field = "preemptibleWorkerBootDiskSize"),
                maxClusterIdleTime = requiredStr(config = o, field = "maxClusterIdleTime")
              )
            case None => ClusterConfig()
          }
        }
      )

      val resources = ConfigResources(
        matrixTableHail = {
          val thisConfig = requiredObj(config = config, field = "matrixTableHail")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        tableHail = {
          val thisConfig = requiredObj(config = config, field = "tableHail")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        standardPlink = {
          val thisConfig = requiredObj(config = config, field = "standardPlink")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        highMemPlink = {
          val thisConfig = requiredObj(config = config, field = "highMemPlink")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        standardR = {
          val thisConfig = requiredObj(config = config, field = "standardR")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        highMemR = {
          val thisConfig = requiredObj(config = config, field = "highMemR")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        flashPca = {
          val thisConfig = requiredObj(config = config, field = "flashPca")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        standardPython = {
          val thisConfig = requiredObj(config = config, field = "standardPython")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        king = {
          val thisConfig = requiredObj(config = config, field = "king")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        vep = {
          val thisConfig = requiredObj(config = config, field = "vep")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        klustakwik = {
          val thisConfig = requiredObj(config = config, field = "klustakwik")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        tabix = {
          val thisConfig = requiredObj(config = config, field = "tabix")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        lowMemEpacts = {
          val thisConfig = requiredObj(config = config, field = "lowMemEpacts")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        midMemEpacts = {
          val thisConfig = requiredObj(config = config, field = "midMemEpacts")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        highMemEpacts = {
          val thisConfig = requiredObj(config = config, field = "highMemEpacts")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        locuszoom = {
          val thisConfig = requiredObj(config = config, field = "locuszoom")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        }
      )
  
      val numericVariantFilters = {
        for {
          vfilter <- requiredObjList(config = config, field = "numericVariantFilters")
        } yield {
          ConfigNumericFilters(
            id = requiredStr(config = vfilter, field = "id", regex = "^[a-zA-Z0-9_]*$"),
            field = requiredStr(config = vfilter, field = "field"),
            range = requiredStr(config = vfilter, field = "range"),
            missingFalse = requiredBool(config = vfilter, field = "missingFalse", default = Some(true)),
            expression = intervalToExpression(requiredStr(config = vfilter, field = "field"), requiredStr(config = vfilter, field = "range"), missing_false = requiredBool(config = vfilter, field = "missingFalse", default = Some(true)))
          )
        }
      }
  
      val booleanVariantFilters = {
        for {
          vfilter <- requiredObjList(config = config, field = "booleanVariantFilters")
        } yield {
          ConfigBooleanFilters(
            id = requiredStr(config = vfilter, field = "id", regex = "^[a-zA-Z0-9_]*$"),
            field = requiredStr(config = vfilter, field = "field"),
            value = requiredBool(config = vfilter, field = "value"),
            expression = booleanToExpression(requiredStr(config = vfilter, field = "field"), requiredStr(config = vfilter, field = "value"))
          )
        }
      }
  
      val categoricalVariantFilters = {
        for {
          vfilter <- requiredObjList(config = config, field = "categoricalVariantFilters")
        } yield {
          ConfigCategoricalFilters(
            id = requiredStr(config = vfilter, field = "id", regex = "^[a-zA-Z0-9_]*$"),
            field = requiredStr(config = vfilter, field = "field"),
            incl = optionalStrList(config = vfilter, field = "incl"),
            excl = optionalStrList(config = vfilter, field = "excl"),
            substrings = requiredBool(config = vfilter, field = "substrings", default = Some(false)),
            expression = categoricalToExpression(s = requiredStr(config = vfilter, field = "field"), i = optionalStrList(config = vfilter, field = "incl"), e = optionalStrList(config = vfilter, field = "excl"), substrings = requiredBool(config = vfilter, field = "substrings", default = Some(false)))
          )
        }
      }
  
      val compoundVariantFilters = {
        for {
          vfilter <- requiredObjList(config = config, field = "compoundVariantFilters")
        } yield {
          val filter = requiredStr(config = vfilter, field = "filter")
          val include = filter.replace("|",",").replace("(",",").replace("~",",").replace("&",",").replace(")",",").replace(" ",",").replaceAll(",{2,}",",").replaceAll("^,","").replaceAll(",$","").split(",")
          val expressions = include.map { f =>
            val expr = f match {
              case n if numericVariantFilters.map(e => e.id) contains n =>
                "(" + numericVariantFilters.filter(e => e.id == n).head.expression + ")"
              case b if booleanVariantFilters.map(e => e.id) contains b =>
                "(" + booleanVariantFilters.filter(e => e.id == b).head.expression + ")"
              case c if categoricalVariantFilters.map(e => e.id) contains c =>
                "(" + categoricalVariantFilters.filter(e => e.id == c).head.expression + ")"
              case _ => throw new CfgException("compoundVariantFilters: variant filter '" + f + "' not found")
            }
            f -> expr
          }.toMap
          ConfigCompoundFilters(
            id = requiredStr(config = vfilter, field = "id", regex = "^[a-zA-Z0-9_]*$"),
            filter = filter,
            include = include,
            expression = expressions.foldLeft(filter)((a,b) => a.replaceAllLiterally(b._1, b._2))
          )
        }
      }
  
      val numericSampleFilters = {
        for {
          sfilter <- requiredObjList(config = config, field = "numericSampleFilters")
        } yield {
          ConfigNumericFilters(
            id = requiredStr(config = sfilter, field = "id", regex = "^[a-zA-Z0-9_]*$"),
            field = requiredStr(config = sfilter, field = "field"),
            range = requiredStr(config = sfilter, field = "range"),
            missingFalse = requiredBool(config = sfilter, field = "missingFalse", default = Some(true)),
            expression = intervalToExpression(requiredStr(config = sfilter, field = "field"), requiredStr(config = sfilter, field = "range"), missing_false = requiredBool(config = sfilter, field = "missingFalse", default = Some(true)))
          )
        }
      }
  
      val booleanSampleFilters = {
        for {
          sfilter <- requiredObjList(config = config, field = "booleanSampleFilters")
        } yield {
          ConfigBooleanFilters(
            id = requiredStr(config = sfilter, field = "id", regex = "^[a-zA-Z0-9_]*$"),
            field = requiredStr(config = sfilter, field = "field"),
            value = requiredBool(config = sfilter, field = "value"),
            expression = booleanToExpression(requiredStr(config = sfilter, field = "field"), requiredStr(config = sfilter, field = "value"))
          )
        }
      }
  
      val categoricalSampleFilters = {
        for {
          sfilter <- requiredObjList(config = config, field = "categoricalSampleFilters")
        } yield {
          ConfigCategoricalFilters(
            id = requiredStr(config = sfilter, field = "id", regex = "^[a-zA-Z0-9_]*$"),
            field = requiredStr(config = sfilter, field = "field"),
            incl = optionalStrList(config = sfilter, field = "incl"),
            excl = optionalStrList(config = sfilter, field = "excl"),
            substrings = requiredBool(config = sfilter, field = "substrings", default = Some(false)),
            expression = categoricalToExpression(s = requiredStr(config = sfilter, field = "field"), i = optionalStrList(config = sfilter, field = "incl"), e = optionalStrList(config = sfilter, field = "excl"), substrings = requiredBool(config = sfilter, field = "substrings", default = Some(false)))
          )
        }
      }
  
      val compoundSampleFilters = {
        for {
          sfilter <- requiredObjList(config = config, field = "compoundSampleFilters")
        } yield {
          val filter = requiredStr(config = sfilter, field = "filter")
          val include = filter.replace("|",",").replace("(",",").replace("~",",").replace("&",",").replace(")",",").replace(" ",",").replaceAll(",{2,}",",").replaceAll("^,","").replaceAll(",$","").split(",")
          val expressions = include.map { f =>
            val expr = f match {
              case n if numericSampleFilters.map(e => e.id) contains n =>
                "(" + numericSampleFilters.filter(e => e.id == n).head.expression + ")"
              case b if booleanSampleFilters.map(e => e.id) contains b =>
                "(" + booleanSampleFilters.filter(e => e.id == b).head.expression + ")"
              case c if categoricalSampleFilters.map(e => e.id) contains c =>
                "(" + categoricalSampleFilters.filter(e => e.id == c).head.expression + ")"
              case _ => throw new CfgException("compoundSampleFilters: sample filter '" + f + "' not found")
            }
            f -> expr
          }.toMap
          ConfigCompoundFilters(
            id = requiredStr(config = sfilter, field = "id", regex = "^[a-zA-Z0-9_]*$"),
            filter = filter,
            include = include,
            expression = expressions.foldLeft(filter)((a,b) => a.replaceAllLiterally(b._1, b._2))
          )
        }
      }
  
      // arrays
      val Arrays = {
      
        for {
          array <- requiredObjList(config = config, field = "arrays")
        } yield {

          val arrayQcCfg = requiredObj(config = array, field = "qc")

          val qcConfigFile = requiredStr(config = arrayQcCfg, field = "config")
          val qcArrayId = requiredStr(config = arrayQcCfg, field = "arrayId")

          val qcConfig = loadConfig(checkPath(qcConfigFile))

          val qcProjectId = requiredStr(config = qcConfig, field = "projectId")
          val qcCloudHome = optionalStr(config = qcConfig, field = "cloudHome") match { case Some(s) => Some(uri(s)); case None => None }
          val hailCloud = requiredBool(config = qcConfig, field = "hailCloud")

          val qcConfigArrays = requiredObjList(config = qcConfig, field = "arrays")
          val qcConfigThisArray = qcConfigArrays.filter(e => requiredStr(config = e, field = "id", regex = "^[a-zA-Z0-9_]*$") == qcArrayId).head

          val exportCleanVcf = requiredBool(config = qcConfigThisArray, field = "exportCleanVcf")

          ConfigArray(
            id = requiredStr(config = array, field = "id", regex = "^[a-zA-Z0-9_]*$"),
            qc = ConfigArrayQc(
              config = qcConfigFile,
              arrayId = qcArrayId,
              baseDir = requiredStr(config = arrayQcCfg, field = "baseDir")
            ),
            qcProjectId = requiredStr(config = qcConfig, field = "projectId"),
            qcCloudHome = optionalStr(config = qcConfig, field = "cloudHome"),
            qcHailCloud = requiredBool(config = qcConfig, field = "hailCloud"),
            phenoFile = requiredStr(config = array, field = "phenoFile"),
            phenoFileId = requiredStr(config = array, field = "phenoFileId"),
            qcSampleFile = requiredStr(config = qcConfig, field = "sampleFile"),
            qcSampleFileId = requiredStr(config = qcConfig, field = "sampleFileId"),
            qcSampleFileSrSex = requiredStr(config = qcConfig, field = "sampleFileSrSex"),
            qcSampleFileMaleCode = requiredStr(config = qcConfig, field = "sampleFileMaleCode"),
            qcSampleFileFemaleCode = requiredStr(config = qcConfig, field = "sampleFileFemaleCode"),
            exportCleanVcf = exportCleanVcf
          )

        }
      
      }
  
      val Cohorts = {
      
        for {
          cohort <- requiredObjList(config = config, field = "cohorts")
        } yield {
  
          val array = requiredStr(config = cohort, field = "array")
          Arrays.map(e => e.id) contains array match {
            case true => ()
            case false => throw new CfgException("cohorts.array: array '" + array + "' not found")
          }
  
          ConfigCohort(
            id = requiredStr(config = cohort, field = "id"),
            array = array,
            ancestry = requiredStrList(config = cohort, field = "ancestry", regex = ancestryCodes.mkString("|")),
            minPartitions = optionalInt(config = cohort, field = "minPartitions", min = Some(1)),
            stratCol = optionalStr(config = cohort, field = "stratCol"),
            stratCodes = optionalStrList(config = cohort, field = "stratCodes")
          )
      
        }
      
      }
  
      val Metas = {
      
        for {
          meta <- requiredObjList(config = config, field = "metas")
        } yield {
  
          val cohorts = for {
            c <- requiredStrList(config = meta, field = "cohorts")
          } yield {
            Cohorts.map(e => e.id) contains c match {
              case true => c
              case false => throw new CfgException("metas.cohorts: cohort '" + c + "' not found")
            }
          }
  
          ConfigMeta(
            id = requiredStr(config = meta, field = "id"),
            cohorts = cohorts,
            minPartitions = optionalInt(config = meta, field = "minPartitions", min = Some(1))
          )
      
        }
      
      }
  
      val Merges = {
      
        for {
          merge <- requiredObjList(config = config, field = "merges")
        } yield {
  
          val cohorts_metas = for {
            c <- requiredStrList(config = merge, field = "cohorts_metas")
          } yield {
            Cohorts.map(e => e.id) contains c match {
              case true => c
              case false =>
                Metas.map(e => e.id) contains c match {
                  case true => c
                  case false => throw new CfgException("merges.cohorts_metas: cohorts_metas '" + c + "' not found")
                }
            }
          }
      
          ConfigMerge(
            id = requiredStr(config = merge, field = "id"),
            cohorts_metas = cohorts_metas,
            minPartitions = optionalInt(config = merge, field = "minPartitions", min = Some(1))
          )
      
        }
      
      }
  
      val Phenos = {
      
        for {
          pheno <- requiredObjList(config = config, field = "phenos")
        } yield {
      
          ConfigPheno(
            id = requiredStr(config = pheno, field = "id"),
            name = requiredStr(config = pheno, field = "name"),
            binary = requiredBool(config = pheno, field = "binary"),
            desc = requiredStr(config = pheno, field = "desc")
          )
      
        }
      
      }
  
      val Knowns = {
      
        for {
          known <- requiredObjList(config = config, field = "knowns")
        } yield {
      
          ConfigKnown(
            id = requiredStr(config = known, field = "id"),
            data = requiredStr(config = known, field = "data"),
            hiLd = requiredStr(config = known, field = "hiLd"),
            n = getStrOrBlank(config = known, field = "n"),
            nCase = getStrOrBlank(config = known, field = "nCase"),
            nCtrl = getStrOrBlank(config = known, field = "nCtrl"),
            desc = requiredStr(config = known, field = "desc"),
            citation = requiredStr(config = known, field = "citation")
          )
      
        }
      
      }
  
      val Schemas = {
  
        for {
          schema <- requiredObjList(config = config, field = "schemas")
        } yield {
  
          val id = requiredStr(config = schema, field = "id")
  
          val design = requiredStr(config = schema, field = "design", regex = modelDesigns.mkString("|"))
  
          val cohorts = requiredStrList(config = schema, field = "cohorts")
  
          val filters = optionalStrList(config = schema, field = "filters") match {
            case Some(s) =>
              for {
                f <- s
              } yield {
                f match {
                  case n if numericVariantFilters.map(e => e.id) contains n => ()
                  case o if booleanVariantFilters.map(e => e.id) contains o => ()
                  case p if categoricalVariantFilters.map(e => e.id) contains p => ()
                  case q if compoundVariantFilters.map(e => e.id) contains q => ()
                  case _ => throw new CfgException("schemas.filters: schema " + id + " variant filter '" + f + "' not found")
                }
              }
              Some(s)
            case None => None
          }
  
          val cohortFilters = optionalObjList(config = schema, field = "cohortFilters") match {
            case Some(s) =>
              val x = for {
                cf <- s
              } yield {
                for {
                  f <- requiredStrList(config = cf, field = "filters")
                } yield {
                  f match {
                    case n if numericVariantFilters.map(e => e.id) contains n => ()
                    case o if booleanVariantFilters.map(e => e.id) contains o => ()
                    case p if categoricalVariantFilters.map(e => e.id) contains p => ()
                    case q if compoundVariantFilters.map(e => e.id) contains q => ()
                    case _ => throw new CfgException("schemas.cohortFilters: schema " + id + " cohort filter '" + f + "' not found")
                  }
                }
                CohortFilter(
                  cohort = requiredStr(config = cf, field = "cohort"),
                  filters = requiredStrList(config = cf, field = "filters")
                )
              }
              Some(x)
            case None => None
          }
  
          (cohortFilters, cohorts.size) match {
            case (Some(f), n) if n == 1 => throw new CfgException("schemas.cohortFilters: cohort specific filters require > 1 cohort for schema " + id)
            case _ => ()
          }
  
          val knockoutFilters = optionalObjList(config = schema, field = "knockoutFilters") match {
            case Some(s) =>
              val x = for {
                cf <- s
              } yield {
                for {
                  f <- requiredStrList(config = cf, field = "filters")
                } yield {
                  f match {
                    case n if numericVariantFilters.map(e => e.id) contains n => ()
                    case o if booleanVariantFilters.map(e => e.id) contains o => ()
                    case p if categoricalVariantFilters.map(e => e.id) contains p => ()
                    case q if compoundVariantFilters.map(e => e.id) contains q => ()
                    case _ => throw new CfgException("schemas.knockoutFilters: schema " + id + " knockout filter '" + f + "' not found")
                  }
                }
                CohortFilter(
                  cohort = requiredStr(config = cf, field = "cohort"),
                  filters = requiredStrList(config = cf, field = "filters")
                )
              }
              Some(x)
            case None => None
          }
  
          (knockoutFilters, cohorts.size) match {
            case (Some(f), n) if n == 1 => throw new CfgException("schemas.knockoutFilters: cohort specific knockout filters require > 1 cohort for schema " + id)
            case (Some(f), n) if n > 1 =>
              (design, knockoutFilters) match {
                case ("strat", Some(s)) => throw new CfgException("schemas.knockoutFilters: schema " + id + " 'strat' design and knockoutFilters are not allowed")
                case _ => ()
              }
            case _ => ()
          }
  
          val knockoutPhenoFilters = knockoutFilters match {
            case Some(s) => s.map(e => e.filters).flatten
            case None => Seq()
          }
          for {
            f <- knockoutPhenoFilters
          } yield {
            f match {
              case n if numericVariantFilters.map(e => e.id) contains n =>
                numericVariantFilters.filter(e => e.id == n).head.field.startsWith("variant_qc.diff_miss") match {
                  case true => throw new CfgException("schemas.knockoutFilters: schema " + id + " knockout filter '" + f + "': knockout filters based on phenotype specific metrics are not currently supported")
                  case false => ()
                }
              case o if booleanVariantFilters.map(e => e.id) contains o =>
                booleanVariantFilters.filter(e => e.id == o).head.field.startsWith("variant_qc.diff_miss") match {
                  case true => throw new CfgException("schemas.knockoutFilters: schema " + id + " knockout filter '" + f + "': knockout filters based on phenotype specific metrics are not currently supported")
                  case false => ()
                }
              case p if categoricalVariantFilters.map(e => e.id) contains p =>
                categoricalVariantFilters.filter(e => e.id == p).head.field.startsWith("variant_qc.diff_miss") match {
                  case true => throw new CfgException("schemas.knockoutFilters: schema " + id + " knockout filter '" + f + "': knockout filters based on phenotype specific metrics are not currently supported")
                  case false => ()
                }
              case q if compoundVariantFilters.map(e => e.id) contains q =>
                for {
                  cf <- compoundVariantFilters.filter(e => e.id == q).head.include
                } yield {
                  cf match {
                    case n if numericVariantFilters.map(e => e.id) contains n =>
                      n.startsWith("variant_qc.diff_miss") match {
                        case true => throw new CfgException("schemas.knockoutFilters: schema " + id + " knockout filter '" + f + "': knockout filters based on phenotype specific metrics are not currently supported")
                        case false => ()
                      }
                    case b if booleanVariantFilters.map(e => e.id) contains b =>
                      b.startsWith("variant_qc.diff_miss") match {
                        case true => throw new CfgException("schemas.knockoutFilters: schema " + id + " knockout filter '" + f + "': knockout filters based on phenotype specific metrics are not currently supported")
                        case false => ()
                      }
                    case c if categoricalVariantFilters.map(e => e.id) contains c =>
                      c.startsWith("variant_qc.diff_miss") match {
                        case true => throw new CfgException("schemas.knockoutFilters: schema " + id + " knockout filter '" + f + "': knockout filters based on phenotype specific metrics are not currently supported")
                        case false => ()
                      }
                    case _ => ()
                  }
                }
              case _ => ()
            }
          }
  
          val masks = optionalObjList(config = schema, field = "masks") match {
            case Some(s) =>
              val x = for {
                cf <- s
              } yield {
                for {
                  f <- requiredStrList(config = cf, field = "filters")
                } yield {
                  f match {
                    case n if numericVariantFilters.map(e => e.id) contains n => ()
                    case o if booleanVariantFilters.map(e => e.id) contains o => ()
                    case p if categoricalVariantFilters.map(e => e.id) contains p => ()
                    case q if compoundVariantFilters.map(e => e.id) contains q => ()
                    case _ => throw new CfgException("schemas.masks: schema " + id + " mask filter '" + f + "' not found")
                  }
                }
                MaskFilter(
                  id = requiredStr(config = cf, field = "id"),
                  filters = requiredStrList(config = cf, field = "filters"),
                  groupFile = optionalStr(config = cf, field = "groupFile"),
                )
              }
              Some(x)
            case None => None
          }
  
          var filterCohorts = Seq[String]()
          cohortFilters match {
            case Some(f) =>
              for {
                cf <- f
              } yield {
                filterCohorts = filterCohorts ++ Seq(cf.cohort)
              }
            case _ => ()
          }
          knockoutFilters match {
            case Some(f) =>
              for {
                kf <- f
              } yield {
                filterCohorts = filterCohorts ++ Seq(kf.cohort)
              }
            case _ => ()
          }
          (design, filters) match {
            case ("strat", Some(_)) => filterCohorts = filterCohorts ++ cohorts
            case _ => ()
          }
          filterCohorts.size match {
            case n if n > 1 => filterCohorts = filterCohorts.distinct
            case _ => ()
          }
  
          ConfigSchema(
            id = id,
            design = design,
            cohorts = (design, cohorts.size > 1, Cohorts.filter(e => cohorts.contains(e.id)).map(e => e.array).distinct.size > 1) match {
              case ("strat", false, _) => throw new CfgException("schemas.cohorts: schema " + id + " 'strat' design requires more than 1 cohort")
              case ("full", true, true) => throw new CfgException("schemas.cohorts: schema " + id + " 'full' design requires cohorts from same array")
              case _ => cohorts
            },
            samplesKeep = optionalStr(config = schema, field = "samplesKeep"),
            samplesExclude = optionalStr(config = schema, field = "samplesExclude"),
            filters = filters,
            cohortFilters = cohortFilters,
            knockoutFilters = knockoutFilters,
            masks = masks,
            maxGroupSize = optionalInt(config = schema, field = "maxGroupSize", min = Some(2)),
            filterCohorts = filterCohorts
          )
  
        }
      
      }
  
      val Models = {
  
        for {
          model <- requiredObjList(config = config, field = "models")
        } yield {
  
          val id = requiredStr(config = model, field = "id")
  
          val schema = requiredStr(config = model, field = "schema")
          Schemas.map(e => e.id) contains schema match {
            case true => ()
            case false => throw new CfgException("models.schema: model " + id + " schema '" + schema + "' not found")
          }
  
          val pheno = requiredStr(config = model, field = "pheno")
          Phenos.map(e => e.id) contains pheno match {
            case true => ()
            case false => throw new CfgException("models.pheno: model " + id + " pheno '" + pheno + "' not found")
          }
  
          val tests = requiredStrList(config = model, field = "tests", regex = assocTests.mkString("|"))
  
          val cohorts = optionalStrList(config = model, field = "cohorts") match {
            case Some(s) =>
              (Schemas.filter(e => e.id == schema).head.design, s.toSet.subsetOf(Schemas.filter(e => e.id == schema).head.cohorts.toSet)) match {
                case ("strat", false) => throw new CfgException("models.cohorts: model " + id + " cohorts must be members of schema " + schema + " cohort list")
                case _ => s
              }
            case None => Schemas.filter(e => e.id == schema).head.cohorts
          }
  
          val metas = optionalStrList(config = model, field = "metas") match {
            case Some(s) =>
              for {
                f <- s
              } yield {
                f match {
                  case n if Metas.map(e => e.id) contains n =>
                    val x = Metas.filter(e => e.id == n).head.cohorts
                    Schemas.filter(e => e.id == schema).head.cohorts.intersect(x) == x match {
                      case false => throw new CfgException("models.metas: model " + id + " meta cohorts must be subset of schema cohorts")
                      case true => ()
                    }
                  case _ => throw new CfgException("models.metas: model " + id + " meta '" + f + "' not found")
                }
              }
              Some(s)
            case None => None
          }
   
          val merges = optionalStrList(config = model, field = "merges") match {
            case Some(s) =>
              for {
                f <- s
              } yield {
                f match {
                  case n if Merges.map(e => e.id) contains n =>
                    val x = Cohorts.filter(e => Merges.filter(f => f.id == n).head.cohorts_metas.contains(e.id)).map(e => e.id)
                    Schemas.filter(e => e.id == schema).head.cohorts.intersect(x) == x match {
                      case false => throw new CfgException("models.merges: model " + id + " merge cohorts must be subset of schema cohorts")
                      case true => ()
                    }
                    metas match {
                      case Some(t) =>
                        val y = Metas.filter(e => Merges.filter(f => f.id == n).head.cohorts_metas.contains(e.id)).map(e => e.id)
                        metas.get.intersect(y) == y match {
                          case false => throw new CfgException("models.merges: model " + id + " merge metas must be subset of schema metas")
                          case true => ()
                        }
                      case None => ()
                    }
                  case _ => throw new CfgException("models.merges: model " + id + " merge '" + f + "' not found")
                }
              }
              Some(s)
            case None => None
          }
  
          val knowns = optionalStrList(config = model, field = "knowns") match {
            case Some(s) =>
              for {
                f <- s
              } yield {
                f match {
                  case n if Knowns.map(e => e.id) contains n => ()
                  case _ => throw new CfgException("models.knowns: model " + id + " known '" + f + "' not found")
                }
              }
              Some(s)
            case None => None
          }
  
          ConfigModel(
            id = id,
            schema = schema,
            pheno = pheno,
            trans = optionalStr(config = model, field = "trans", regex = modelTrans.mkString("|")),
            tests = tests,
            assocPlatforms = tests.map(e => e.split("\\.")(0)).distinct,
            maxPcaOutlierIterations = requiredInt(config = model, field = "maxPcaOutlierIterations"),
            covars = requiredStrList(config = model, field = "covars").mkString("+"),
            cohorts = cohorts,
            metas = (Schemas.filter(e => e.id == schema).head.design, metas) match {
              case ("full", Some(s)) => throw new CfgException("models.metas:  model " + id + " schema " + schema + " 'full' design and metas are not allowed")
              case _ => metas
            },
            merges = (Schemas.filter(e => e.id == schema).head.design, merges) match {
              case ("full", Some(s)) => throw new CfgException("models.merges: model " + id + " schema " + schema + " 'full' design and merges are not allowed")
              case _ => merges
            },
            knowns = knowns,
            runAssoc = requiredBool(config = model, field = "runAssoc", default = Some(true))
          )
  
        }
  
      }
  
      //val Reports = {
      //
      //  for {
      //    report <- requiredObjList(config = config, field = "reports")
      //  } yield {
      //
      //    ConfigReport(
      //      id = requiredStr(config = report, field = "id"),
      //      name = requiredStr(config = report, field = "name"),
      //      sections = for {
      //        section <- requiredObjList(config = report, field = "sections")
      //      } yield {
      //
      //        val models = for {
      //          m <- requiredStrList(config = section, field = "models")
      //        } yield {
      //          Models.map(e => e.id) contains m match {
      //            case true => m
      //            case false => throw new CfgException("reports.sections.models: model '" + m + "' not found")
      //          }
      //        }
      //
      //        ConfigSection(
      //          id = requiredStr(config = section, field = "id"),
      //          title = requiredStr(config = section, field = "title"),
      //          models = models
      //        )
      //      
      //      }
      //
      //    )
      //
      //  }
      //
      //}
  
      val nArrays = Arrays.size
      val nCohorts = Cohorts.size
      val nMetas = Metas.size
  
      new ProjectConfig(
  
        loamstreamVersion = loamstreamVersion,
        pipelineVersion = pipelineVersion,
        projectId = projectId,
        hailCloud = hailCloud,
        cloudHome = cloudHome,
        cloudShare = cloudShare,
        referenceGenome = referenceGenome,
        //regionsExclude = regionsExclude,
        geneIdMap = geneIdMap,
        //kgPurcellVcf = kgPurcellVcf,
        //kgSample = kgSample,
        //kgSampleId = kgSampleId,
        //kgSamplePop = kgSamplePop,
        //kgSampleGroup = kgSampleGroup,
        //kgVcf = kgVcf,
        //kgIds = kgIds,
        //humanReferenceWild = humanReferenceWild,
        fasta = fasta,
        vepCacheDir = vepCacheDir,
        vepPluginsDir = vepPluginsDir,
        dbNSFP = dbNSFP,
        //sampleFile = sampleFile,
        //sampleFileId = sampleFileId,
        //sampleFileSrSex = sampleFileSrSex,
        //sampleFileMaleCode = sampleFileMaleCode,
        //sampleFileFemaleCode = sampleFileFemaleCode,
        //sampleFileSrRace = sampleFileSrRace,
        //sampleFileAFRCodes = sampleFileAFRCodes,
        //sampleFileAMRCodes = sampleFileAMRCodes,
        //sampleFileEURCodes = sampleFileEURCodes,
        //sampleFileEASCodes = sampleFileEASCodes,
        //sampleFileSASCodes = sampleFileSASCodes,
        //phenoFile = phenoFile,
        //phenoFileId = phenoFileId,
        authors = authors,
        email = email,
        organization = organization,
        acknowledgementsAnalysisReport = acknowledgementsAnalysisReport,
        //acknowledgementsQcReport = acknowledgementsQcReport,
        //nAncestryInferenceFeatures = nAncestryInferenceFeatures,
        //ancestryInferenceFeatures = ancestryInferenceFeatures,
        minPCs = minPCs,
        maxPCs = maxPCs,
        nStddevs = nStddevs,
        diffMissMinExpectedCellCount = diffMissMinExpectedCellCount,
        cloudResources = cloudResources,
        resources = resources,
        nArrays = nArrays,
        nCohorts = nCohorts,
        nMetas = nMetas,
        maxSigRegions = maxSigRegions,
        numericVariantFilters =  numericVariantFilters,
        booleanVariantFilters = booleanVariantFilters,
        categoricalVariantFilters = categoricalVariantFilters,
        compoundVariantFilters = compoundVariantFilters,
        numericSampleFilters =  numericSampleFilters,
        booleanSampleFilters = booleanSampleFilters,
        categoricalSampleFilters = categoricalSampleFilters,
        compoundSampleFilters = compoundSampleFilters,
        Arrays = Arrays,
        Cohorts = Cohorts,
        Metas = Metas,
        Merges = Merges,
        Knowns = Knowns,
        Phenos = Phenos,
        Schemas = Schemas,
        Models = Models
        //Reports = Reports
  
      )
  
    }
  
    def parseUtils(config: loamstream.conf.DataConfig): Utils = {
  
      val imagesDir = path(checkPath(requiredStr(config = config, field = "imagesDir")))
      val scriptsDir = path(checkPath(requiredStr(config = config, field = "scriptsDir")))
  
      val image = Image(
        imgHail = path(s"${imagesDir}/hail.simg"),
        imgLocuszoom = path(s"${imagesDir}/locuszoom.simg"),
        imgPython2 = path(s"${imagesDir}/python2.simg"),
        imgR = path(s"${imagesDir}/r.simg"),
        imgTools = path(s"${imagesDir}/tools.simg"),
        imgTexLive = path(s"${imagesDir}/texlive.simg"),
        imgEnsemblVep = path(s"${imagesDir}/ensemblvep.simg"),
        imgFlashPca = path(s"${imagesDir}/flashpca.simg"),
        imgUmichStatgen = path(s"${imagesDir}/umich_statgen.simg")
      )
  
      val binary = Binary(
        binLiftOver = path("/usr/local/bin/liftOver"),
        binGenotypeHarmonizer = path("/usr/local/bin/GenotypeHarmonizer.jar"),
        binKing = path("/usr/local/bin/king"),
        binPlink = path("/usr/local/bin/plink"),
        binTabix = path("/usr/local/bin/tabix"),
        binGhostscript = path("/usr/local/bin/gs"),
        binKlustakwik = path("/usr/local/bin/KlustaKwik"),
        binPython = path("/usr/local/bin/python"),
        binLocuszoom = path("/usr/local/bin/locuszoom"),
        binPdflatex = path("/usr/local/bin/pdflatex"),
        binRscript = path("/usr/local/bin/Rscript"),
        binFlashPca = path("/usr/local/bin/flashpca"),
        binEpacts = path("/usr/local/bin/epacts")
      )
  
      val python = Python(
        pyAlignNon1kgVariants = path(s"${scriptsDir}/align_non1kg_variants.py"),
        pyHailLoad = path(s"${scriptsDir}/hail_load.py"),
        pyHailLoadAnnotations = path(s"${scriptsDir}/hail_load_annotations.py"),
        pyHailExportQcData = path(s"${scriptsDir}/hail_export_qc_data.py"),
        pyHailAncestryPcaMerge1kg = path(s"${scriptsDir}/hail_ancestry_pca_merge_1kg.py"),
        pyHailPcaMerge1kg = path(s"${scriptsDir}/hail_pca_merge_1kg.py"),
        pyHailSampleqc = path(s"${scriptsDir}/hail_sampleqc.py"),
        pyHailFilter = path(s"${scriptsDir}/hail_filter.py"),
        pyMakeSamplesRestoreTable = path(s"${scriptsDir}/make_samples_restore_table.py"),
        pyCompileExclusions = path(s"${scriptsDir}/compile_exclusions.py"),
        pyMergeVariantLists = path(s"${scriptsDir}/merge_variant_lists.py"),
        pyBimToUid = path(s"${scriptsDir}/bim_to_uid.py"),
        pyHailUtils = path(s"${scriptsDir}/hail_utils.py"),
        pyHailAssoc = path(s"${scriptsDir}/hail_assoc.py"),
        pyHailSchemaVariantStats = path(s"${scriptsDir}/hail_schema_variant_stats.py"),
        pyHailSchemaVariantCaseCtrlStats = path(s"${scriptsDir}/hail_schema_variant_case_ctrl_stats.py"),
        pyHailExportCleanArrayData = path(s"${scriptsDir}/hail_export_clean_array_data.py"),
        pyHailFilterSchemaVariants = path(s"${scriptsDir}/hail_filter_schema_variants.py"),
        pyHailFilterSchemaPhenoVariants = path(s"${scriptsDir}/hail_filter_schema_pheno_variants.py"),
        pyHailGenerateGroupfile = path(s"${scriptsDir}/hail_generate_groupfile.py"),
        pyHailGenerateModelVcf = path(s"${scriptsDir}/hail_generate_model_vcf.py"),
        pyQqPlot = path(s"${scriptsDir}/qqplot.py"),
        pyMhtPlot = path(s"${scriptsDir}/mhtplot.py"),
        pyTopResults = path(s"${scriptsDir}/top_results.py"),
        pyTopGroupResults = path(s"${scriptsDir}/top_group_results.py"),
        pyExtractTopRegions = path(s"${scriptsDir}/extract_top_regions.py"),
        pyPhenoDistPlot = path(s"${scriptsDir}/pheno_dist_plot.py")
        //pyAddGeneAnnot = path(s"${scriptsDir}/add_gene_annot.py")
        //pyHailModelVariantStats = path(s"${scriptsDir}/hail_model_variant_stats.py"),
        //pyHailFilterModelVariants = path(s"${scriptsDir}/hail_filter_model_variants.py"),
        //pyHailFilterResults = path(s"${scriptsDir}/hail_filter_results.py"),
        //pyHailMerge = path(s"${scriptsDir}/hail_merge.py"),
        //pyHailMetaAnalysis = path(s"${scriptsDir}/hail_meta_analysis.py"),
        //pyGenerateReportHeader = path(s"${scriptsDir}/generate_report_header.py"),
        //pyGenerateQcReportIntro = path(s"${scriptsDir}/generate_qc_report_intro.py"),
        //pyGenerateQcReportData = path(s"${scriptsDir}/generate_qc_report_data.py"),
        //pyGenerateQcReportAncestry = path(s"${scriptsDir}/generate_qc_report_ancestry.py"),
        //pyGenerateQcReportIbdSexcheck = path(s"${scriptsDir}/generate_qc_report_ibd_sexcheck.py"),
        //pyGenerateQcReportSampleqc = path(s"${scriptsDir}/generate_qc_report_sampleqc.py"),
        //pyGenerateQcReportVariantqc = path(s"${scriptsDir}/generate_qc_report_variantqc.py"),
        //pyGenerateQcReportBibliography = path(s"${scriptsDir}/generate_qc_report_bibliography.py"),
        //pyGenerateAnalysisReportIntro = path(s"${scriptsDir}/generate_analysis_report_intro.py"),
        //pyGenerateAnalysisReportData = path(s"${scriptsDir}/generate_analysis_report_data.py"),
        //pyGenerateAnalysisReportStrategy = path(s"${scriptsDir}/generate_analysis_report_strategy.py"),
        //pyGenerateAnalysisReportPhenoSummary = path(s"${scriptsDir}/generate_analysis_report_pheno_summary.py"),
        //pyGenerateAnalysisReportPhenoCalibration = path(s"${scriptsDir}/generate_analysis_report_pheno_calibration.py"),
        //pyGenerateAnalysisReportPhenoTopLoci = path(s"${scriptsDir}/generate_analysis_report_pheno_top_loci.py"),
        //pyGenerateAnalysisReportPhenoKnownLoci = path(s"${scriptsDir}/generate_analysis_report_pheno_known_loci.py"),
        //pyGenerateAnalysisReportBibliography = path(s"${scriptsDir}/generate_analysis_report_bibliography.py")
      )
  
      val bash = Bash(
        shFindPossibleDuplicateVariants = path(s"${scriptsDir}/find_possible_duplicate_variants.sh"),
        shAnnotate = path(s"${scriptsDir}/annotate.sh"),
        shAnnotateResults = path(s"${scriptsDir}/annotate_results.sh"),
        shKing = path(s"${scriptsDir}/king.sh"),
        shPlinkPrepare = path(s"${scriptsDir}/plink_prepare.sh"),
        shPlinkToVcfNoHalfCalls = path(s"${scriptsDir}/plink_to_vcf_no_half_calls.sh"),
        shKlustakwikPca = path(s"${scriptsDir}/klustakwik_pca.sh"),
        shKlustakwikMetric = path(s"${scriptsDir}/klustakwik_metric.sh"),
        shCrossCohortCommonVariants = path(s"${scriptsDir}/cross_cohort_common_variants.sh"),
        shFlashPca = path(s"${scriptsDir}/flashpca.sh"),
        shEpacts = path(s"${scriptsDir}/epacts.sh"),
        shMergeResults = path(s"${scriptsDir}/merge_results.sh"),
        shRegPlot = path(s"${scriptsDir}/regplot.sh")
        //shTopResultsAddGenes = path(s"${scriptsDir}/top_results_add_genes.sh")
      )
  
      val r = R(
        rFindBestDuplicateVariants = path(s"${scriptsDir}/find_best_duplicate_variants.r"),
        rAncestryClusterMerge = path(s"${scriptsDir}/ancestry_cluster_merge.r"),
        rCalcKinshipFamSizes = path(s"${scriptsDir}/calc_kinship_fam_sizes.r"),
        rPlotAncestryPca = path(s"${scriptsDir}/plot_ancestry_pca.r"),
        rPlotAncestryCluster = path(s"${scriptsDir}/plot_ancestry_cluster.r"),
        rIstatsPcsGmmClusterPlot = path(s"${scriptsDir}/istats_pcs_gmm_cluster_plot.r"),
        rIstatsAdjGmmPlotMetrics = path(s"${scriptsDir}/istats_adj_gmm_plot_metrics.r"),
        rCalcIstatsAdj = path(s"${scriptsDir}/calc_istats_adj.r"),
        rIstatsAdjPca = path(s"${scriptsDir}/istats_adj_pca.r"),
        rModelCohortSamplesAvailable = path(s"${scriptsDir}/model_cohort_samples_available.r"),
        rSchemaCohortSamplesAvailable = path(s"${scriptsDir}/schema_cohort_samples_available.r"),
        rMetaCohortSamples = path(s"${scriptsDir}/meta_cohort_samples.r"),
        rExcludeCrossArray = path(s"${scriptsDir}/exclude_cross_array.r"),
        rGeneratePheno = path(s"${scriptsDir}/generate_pheno.r"),
        rConvertPhenoToPed = path(s"${scriptsDir}/convert_pheno_to_ped.r"),
        rTop20 = path(s"${scriptsDir}/top20.r"),
        rRawVariantsSummaryTable = path(s"${scriptsDir}/raw_variants_summary_table.r")
        //rAncestryClusterTable = path(s"${scriptsDir}/ancestry_cluster_table.r"),
        //rPcair = path(s"${scriptsDir}/pcair.r"),
        //rUpsetplotBimFam = path(s"${scriptsDir}/upsetplot.bimfam.r"),
        //rMakeOutlierTable = path(s"${scriptsDir}/make_outlier_table.r"),
        //rMakeMetricDistPlot = path(s"${scriptsDir}/make_metric_dist_plot.r"),
        //rTop50Known = path(s"${scriptsDir}/top50_known.r"),
        //rMetaExclusionsTable = path(s"${scriptsDir}/meta_exclusions_table.r")
      )
  
      new Utils(
        imagesDir = imagesDir,
        scriptsDir = scriptsDir,
        image = image,
        binary = binary,
        python = python,
        bash = bash,
        r = r)
  
    }
  
  }
  
  // Initialize configuration and utilities, verify them, and write all objects to file
  println("Loading Project Configuration File ...")
  val dataConfig = loadConfig("dataConfig", "")
  
  val projectConfig = ProjectConfig.parseConfig(dataConfig)
  projectConfig.debugVars()
  for ( d <- projectConfig.Arrays ) { d.debugVars() }
  for ( d <- projectConfig.Cohorts ) { d.debugVars() }
  for ( d <- projectConfig.Metas ) { d.debugVars() }
  for ( d <- projectConfig.Merges ) { d.debugVars() }
  for ( d <- projectConfig.Phenos ) { d.debugVars() }
  for ( d <- projectConfig.Models ) { d.debugVars() }
  for ( d <- projectConfig.Knowns ) { d.debugVars() }
  //for ( d <- projectConfig.Reports ) { d.debugVars() }
  println("... Project Configuration Loaded Successfully!")
  
  println("Loading Pipeline Utilities Configuration ...")
  val utils = ProjectConfig.parseUtils(dataConfig)
  utils.debugVars()
  utils.image.debugVars()
  utils.binary.debugVars()
  utils.python.debugVars()
  utils.bash.debugVars()
  utils.r.debugVars()
  println("... Pipeline Utilities Configuration Loaded Successfully!")
  
  //// verify phenotype file if defined
  //projectConfig.phenoFile match {
  //  case "" => ()
  //  case _ => verifyPheno(phenoFile = projectConfig.phenoFile, models = projectConfig.Models)
  //}

}