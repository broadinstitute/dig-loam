object ProjectConfig extends loamstream.LoamFile {

  import Fxns._
  import loamstream.googlecloud.ClusterConfig
  
  val refGenomes = Seq("GRCh37","GRCh38")
  val ancestryCodes = Seq("EUR","AFR","AMR","SAS","EAS")
  val gwasTech = Seq("gwas")
  val seqTech = Seq("wgs","wes")
  val arrayFormats = Seq("plink","vcf","mt")
  
  val inputTypesPlink = {
    (for {
      x <- gwasTech
      y <- Seq("plink")
    } yield {
      (x, y)
    }) ++
    (for {
      x <- seqTech
      y <- Seq("plink")
    } yield {
      (x, y)
    })
  }
  
  val inputTypesGwasVcf = for {
    x <- gwasTech
    y <- Seq("vcf")
  } yield {
    (x, y)
  }
  
  val inputTypesSeqPlink = for {
    x <- seqTech
    y <- Seq("plink")
  } yield {
    (x, y)
  }
  
  val inputTypesSeqVcf = for {
    x <- seqTech
    y <- Seq("vcf")
  } yield {
    (x, y)
  }

  val inputTypesSeqMT = for {
    x <- seqTech
    y <- Seq("mt")
  } yield {
    (x, y)
  }

  val inputTypesGwasMT = for {
    x <- gwasTech
    y <- Seq("mt")
  } yield {
    (x, y)
  }
  
  val defaultSampleMetricsGwas = Seq(
    "n_non_ref",
    "n_het",
    "n_called",
    "call_rate",
    "r_ti_tv",
    "het",
    "het_low",
    "het_high",
    "n_hom_var",
    "r_het_hom_var"
  )
  val defaultSampleMetricsSeq = defaultSampleMetricsGwas ++ Seq(
    "n_singleton",
    "avg_ab",
    "avg_ab50"
  )
  
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
    standardPlinkMultiCpu: ConfigMachine,
    standardR: ConfigMachine,
    highMemR: ConfigMachine,
    flashPca: ConfigMachine,
    liftOver: ConfigMachine,
    genotypeHarmonizer: ConfigMachine,
    standardPython: ConfigMachine,
    vep: ConfigMachine,
    king: ConfigMachine,
    klustakwik: ConfigMachine,
    tabix: ConfigMachine,
    bgenix: ConfigMachine) extends Debug
  
  final case class ConfigArray(
    id: String,
    filename: String,
    format: String,
    technology: String,
    description: String,
    keepIndels: Boolean,
    minPartitions: Option[Int],
    liftOver: Option[String] = None,
    sampleQcMetrics: Seq[String],
    nSampleMetricPcs: Option[Int],
    sampleMetricCovars: Option[String],
    chrs: Seq[String],
    gqThreshold: Option[Int],
    ancestryOutliersKeep: Option[String],
    duplicatesKeep: Option[String],
    famsizeKeep: Option[String],
    sampleqcKeep: Option[String],
    sexcheckKeep: Option[String],
    qcVariantFilters: Option[Seq[String]],
    qcVariantSampleN: Option[Int],
    qcVariantSampleSeed: Option[Int],
    postQcSampleFilters: Option[Seq[String]],
    postQcVariantFilters: Option[Seq[String]],
    varUidMaxAlleleLen: Int,
    exportCleanBgen: Boolean) extends Debug

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
    hailCloud: Boolean,
    hailVersion: String,
    tmpDir: String,
    cloudShare: Option[URI],
    cloudHome: Option[URI],
    projectId: String,
    referenceGenome: String,
    dbSNPht: String,
    regionsExclude: String,
    kgPurcellVcf: String,
    kgSample: String,
    kgSampleId: String,
    kgSamplePop: String,
    kgSampleGroup: String,
    kgVcf: Option[String],
    kgIds: Option[String],
    humanReferenceWild: String,
    fasta: String,
    vepCacheDir: String,
    vepPluginsDir: String,
    dbNSFP: String,
    vepConservation: String,
    vepGerpBW: Option[String],
    gnomad: String,
    sampleFile: String,
    sampleFileId: String,
    sampleFileSrSex: Option[String],
    sampleFileMaleCode: Option[String],
    sampleFileFemaleCode: Option[String],
    sampleFileSrRace: Option[String],
    sampleFileAFRCodes: Option[Seq[String]],
    sampleFileAMRCodes: Option[Seq[String]],
    sampleFileEURCodes: Option[Seq[String]],
    sampleFileEASCodes: Option[Seq[String]],
    sampleFileSASCodes: Option[Seq[String]],
    authors: Seq[String],
    email: String,
    organization: String,
    acknowledgements: Option[Seq[String]],
    nAncestryInferenceFeatures: Int,
    ancestryInferenceFeatures: String,
    cloudResources: ConfigCloudResources,
    resources: ConfigResources,
    nArrays: Int,
    numericVariantFilters: Seq[ConfigNumericFilters],
    booleanVariantFilters: Seq[ConfigBooleanFilters],
    categoricalVariantFilters: Seq[ConfigCategoricalFilters],
    compoundVariantFilters: Seq[ConfigCompoundFilters],
    numericSampleFilters: Seq[ConfigNumericFilters],
    booleanSampleFilters: Seq[ConfigBooleanFilters],
    categoricalSampleFilters: Seq[ConfigCategoricalFilters],
    compoundSampleFilters: Seq[ConfigCompoundFilters],
    Arrays: Seq[ConfigArray]
    //Reports: Seq[ConfigReport]
    ) extends Debug
  
  final case class Image(
    imgHail: Path,
    imgLocuszoom: Path,
    imgPython2: Path,
    imgR: Path,
    imgTools: Path,
    imgKing: Path,
    imgPlink2: Path,
    imgBgen: Path,
    imgTexLive: Path,
    imgEnsemblVep: Path,
    imgFlashPca: Path,
    imgImagemagick: Path) extends Debug
  
  final case class Binary(
    binLiftOver: Path,
    binGenotypeHarmonizer: Path,
    binKing: Path,
    binPlink: Path,
    binPlink2: Path,
    binBgenix: Path,
    binTabix: Path,
    binBgzip: Path,
    binGhostscript: Path,
    binKlustakwik: Path,
    binPython: Path,
    binLocuszoom: Path,
    binPdflatex: Path,
    binRscript: Path,
    binFlashPca: Path,
    binConvert: Path) extends Debug
  
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
    pyHailExportVcf: Path,
    pyGenerateReportHeader: Path,
    pyGenerateQcReportIntro: Path,
    pyGenerateQcReportData: Path,
    pyGenerateQcReportAncestry: Path,
    pyGenerateQcReportIbdSexcheck: Path,
    pyGenerateQcReportSampleqc: Path,
    pyGenerateQcReportVariantqc: Path,
    pyGenerateQcReportBibliography: Path) extends Debug
  
  final case class Bash(
    shFindPossibleDuplicateVariants: Path,
    shExtractIndels: Path,
    shAnnotate: Path,
    shAnnotateResults: Path,
    shKing: Path,
    shPlinkPrepare: Path,
    shPlinkToVcfNoHalfCalls: Path,
    shKlustakwikPca: Path,
    shKlustakwikMetric: Path,
    shTabixExtract: Path) extends Debug
  
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
    rRawVariantsSummaryTable: Path,
    rSeqVariantsSummaryTable: Path,
    rVariantsExcludeSummaryTable: Path,
    rAncestryClusterTable: Path,
    rMakeOutlierTable: Path,
    rUpsetplotVariantSample: Path,
    rMakeMetricDistPlot: Path) extends Debug
  
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
      val loamstreamVersion = System.getProperty("loamstreamVersion")
      val pipelineVersion = System.getProperty("pipelineVersion")
      val projectId = requiredStr(config = config, field = "projectId")
      val hailCloud = requiredBool(config = config, field = "hailCloud")
      val hailVersion = requiredStr(config = config, field = "hailVersion", default = Some("latest"))
      val tmpDir = System.getProperty("tmpDir")
      val cloudShare = optionalStr(config = config, field = "cloudShare") match { case Some(s) => Some(uri(s)); case None => None }
      val cloudHome = optionalStr(config = config, field = "cloudHome") match { case Some(s) => Some(uri(s)); case None => None }
      val referenceGenome = requiredStr(config = config, field = "referenceGenome", regex = refGenomes.mkString("|"))
      val dbSNPht = requiredStr(config = config, field = "dbSNPht")
      val regionsExclude = requiredStr(config = config, field = "regionsExclude")
      val kgPurcellVcf = requiredStr(config = config, field = "kgPurcellVcf")
      val kgSample = requiredStr(config = config, field = "kgSample")
      val kgSampleId = requiredStr(config = config, field = "kgSampleId")
      val kgSamplePop = requiredStr(config = config, field = "kgSamplePop")
      val kgSampleGroup = requiredStr(config = config, field = "kgSampleGroup")
      val kgVcf = optionalStr(config = config, field = "kgVcf")
      val kgIds = optionalStr(config = config, field = "kgIds")
      val humanReferenceWild = requiredStr(config = config, field = "humanReferenceWild")
      val fasta = requiredStr(config = config, field = "fasta")
      val vepCacheDir = requiredStr(config = config, field = "vepCacheDir")
      val vepPluginsDir = requiredStr(config = config, field = "vepPluginsDir")
      val dbNSFP = requiredStr(config = config, field = "dbNSFP")
      val vepConservation = requiredStr(config = config, field = "vepConservation")
      val vepGerpBW = optionalStr(config = config, field = "vepGerpBW")
      val gnomad = requiredStr(config = config, field = "gnomad")
      val sampleFile = requiredStr(config = config, field = "sampleFile")
      val sampleFileId = requiredStr(config = config, field = "sampleFileId")
      val sampleFileSrSex = optionalStr(config = config, field = "sampleFileSrSex")
      val sampleFileMaleCode = optionalStr(config = config, field = "sampleFileMaleCode")
      val sampleFileFemaleCode = optionalStr(config = config, field = "sampleFileFemaleCode")
      val sampleFileSrRace = optionalStr(config = config, field = "sampleFileSrRace")
      val sampleFileAFRCodes = optionalStrList(config = config, field = "sampleFileAFRCodes")
      val sampleFileAMRCodes = optionalStrList(config = config, field = "sampleFileAMRCodes")
      val sampleFileEURCodes = optionalStrList(config = config, field = "sampleFileEURCodes")
      val sampleFileEASCodes = optionalStrList(config = config, field = "sampleFileEASCodes")
      val sampleFileSASCodes = optionalStrList(config = config, field = "sampleFileSASCodes")
      val authors = requiredStrList(config = config, field = "authors")
      val email = requiredStr(config = config, field = "email")
      val organization = requiredStr(config = config, field = "organization")
      val acknowledgements = optionalStrList(config = config, field = "acknowledgements")
      val nAncestryInferenceFeatures = requiredInt(config = config, field = "nAncestryInferenceFeatures", default = Some(3), min = Some(1), max = Some(20))

      (referenceGenome, vepGerpBW) match {
        case ("GRCh38", None) => throw new CfgException("projectConfig: config setting for vepGerpBW required with reference genome GRCh38")
        case _ => ()
      }
  
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
        standardPlinkMultiCpu = {
          val thisConfig = requiredObj(config = config, field = "standardPlinkMultiCpu")
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
        liftOver = {
          val thisConfig = requiredObj(config = config, field = "liftOver")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        genotypeHarmonizer = {
          val thisConfig = requiredObj(config = config, field = "genotypeHarmonizer")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        standardPython = {
          val thisConfig = requiredObj(config = config, field = "standardPython")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        vep = {
          val thisConfig = requiredObj(config = config, field = "vep")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        },
        king = {
          val thisConfig = requiredObj(config = config, field = "king")
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
        bgenix = {
          val thisConfig = requiredObj(config = config, field = "bgenix")
          ConfigMachine(cpus = requiredInt(config = thisConfig, field = "cpus"), mem = requiredInt(config = thisConfig, field = "mem"), maxRunTime = requiredInt(config = thisConfig, field = "maxRunTime"))
        }
      )
  
      // inferred global values
      val ancestryInferenceFeatures = "1" * nAncestryInferenceFeatures + "0" * (20 - nAncestryInferenceFeatures)
  
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
  
          val technology = requiredStr(config = array, field = "technology", regex = (gwasTech ++ seqTech).mkString("|"))
  
          val qcVariantFilters = optionalStrList(config = array, field = "qcVariantFilters") match {
            case Some(s) =>
              for {
                f <- s
              } yield {
                f match {
                  case n if numericVariantFilters.map(e => e.id) contains n => ()
                  case b if booleanVariantFilters.map(e => e.id) contains b => ()
                  case c if categoricalVariantFilters.map(e => e.id) contains c => ()
                  case d if compoundVariantFilters.map(e => e.id) contains d => ()
                  case _ => throw new CfgException("arrays.qcVariantFilters: variant filter '" + f + "' not found")
                }
              }
              Some(s)
            case None => None
          }
  
          val postQcSampleFilters = optionalStrList(config = array, field = "postQcSampleFilters") match {
            case Some(s) =>
              for {
                f <- s
              } yield {
                f match {
                  case n if numericSampleFilters.map(e => e.id) contains n => ()
                  case b if booleanSampleFilters.map(e => e.id) contains b => ()
                  case c if categoricalSampleFilters.map(e => e.id) contains c => ()
                  case d if compoundSampleFilters.map(e => e.id) contains d => ()
                  case _ => throw new CfgException("arrays.postQcSampleFilters: sample filter '" + f + "' not found")
                }
              }
              Some(s)
            case None => None
          }
  
          val postQcVariantFilters = optionalStrList(config = array, field = "postQcVariantFilters") match {
            case Some(s) =>
              for {
                f <- s
              } yield {
                f match {
                  case n if numericVariantFilters.map(e => e.id) contains n => ()
                  case b if booleanVariantFilters.map(e => e.id) contains b => ()
                  case c if categoricalVariantFilters.map(e => e.id) contains c => ()
                  case d if compoundVariantFilters.map(e => e.id) contains d => ()
                  case _ => throw new CfgException("arrays.postQcVariantFilters: variant filter '" + f + "' not found")
                }
              }
              Some(s)
            case None => None
          }

          val chrs = requiredStrList(config = array, field = "chrs", regex = "[1-9]|1[0-9]|2[0-2]|X|Y|MT|(1-[2-9]|1-1[0-9]|1-2[0-2])|(2-[3-9]|2-1[0-9]|2-2[0-2])|(3-[4-9]|3-1[0-9]|3-2[0-2])|(4-[5-9]|4-1[0-9]|4-2[0-2])|(5-[6-9]|5-1[0-9]|5-2[0-2])|(6-[7-9]|6-1[0-9]|6-2[0-2])|(7-[8-9]|7-1[0-9]|7-2[0-2])|(8-9|8-1[0-9]|8-2[0-2])|(9-1[0-9]|9-2[0-2])|(10-1[1-9]|10-2[0-2])|(11-1[2-9]|11-2[0-2])|(12-1[3-9]|12-2[0-2])|(13-1[4-9]|13-2[0-2])|(14-1[5-9]|14-2[0-2])|(15-1[6-9]|15-2[0-2])|(16-1[7-9]|16-2[0-2])|(17-1[8-9]|17-2[0-2])|(18-19|18-2[0-2])|(19-2[0-2])|(20-2[1-2])|(21-22)")

          chrs.contains("X") match {
            case false =>
              (sampleFileSrSex, sampleFileMaleCode, sampleFileFemaleCode) match {
                case (Some(_), Some(_), Some(_)) => ()
                case _ => throw new CfgException("arrays.chrs: if any arrays do not include X chromosome, then sampleFileSrSex, sampleFileMaleCode, and sampleFileFemaleCode must be defined")
              }
            case true => ()
          }
      
          ConfigArray(
            id = requiredStr(config = array, field = "id", regex = "^[a-zA-Z0-9_]*$"),
            filename = requiredStr(config = array, field = "filename"),
            format = requiredStr(config = array, field = "format", regex = arrayFormats.mkString("|")),
            technology = technology,
            description = requiredStr(config = array, field = "description"),
            keepIndels = requiredBool(config = array, field = "keepIndels"),
            minPartitions = optionalInt(config = array, field = "minPartitions", min = Some(1)),
            liftOver = optionalStr(config = array, field = "liftOver"),
            sampleQcMetrics = optionalStrList(config = array, field = "sampleQcMetrics") match { 
              case None =>
                technology match {
                  case m if seqTech.contains(m) => defaultSampleMetricsSeq
                  case n if gwasTech.contains(n) => defaultSampleMetricsGwas
                  case o => throw new CfgException("arrays.sampleQcMetrics: technology '" + o + "' not recognized")
                }
              case Some(s) => s
            },
            nSampleMetricPcs = optionalInt(config = array, field = "nSampleMetricPcs", min = Some(0)),
            sampleMetricCovars = optionalStrList(config = array, field = "sampleMetricCovars") match { case Some(s) => Some(s.mkString("+")); case None => None },
            chrs = chrs,
            gqThreshold = optionalInt(config = array, field = "gqThreshold", min = Some(0)),
            ancestryOutliersKeep = optionalStr(config = array, field = "ancestryOutliersKeep"),
            duplicatesKeep = optionalStr(config = array, field = "duplicatesKeep"),
            famsizeKeep = optionalStr(config = array, field = "famsizeKeep"),
            sampleqcKeep = optionalStr(config = array, field = "sampleqcKeep"),
            sexcheckKeep = optionalStr(config = array, field = "sexcheckKeep"),
            qcVariantFilters = qcVariantFilters,
            qcVariantSampleN = optionalInt(config = array, field = "qcVariantSampleN", min = Some(1000)),
            qcVariantSampleSeed = optionalInt(config = array, field = "qcVariantSampleSeed", min = Some(0)),
            postQcSampleFilters = postQcSampleFilters,
            postQcVariantFilters = postQcVariantFilters,
            varUidMaxAlleleLen = requiredInt(config = array, field = "varUidMaxAlleleLen", default = Some(1000), min = Some(1), max = Some(10000)),
            exportCleanBgen = requiredBool(config = array, field = "exportCleanBgen")
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
  
      new ProjectConfig(
  
        loamstreamVersion = loamstreamVersion,
        pipelineVersion = pipelineVersion,
        projectId = projectId,
        hailCloud = hailCloud,
        hailVersion = hailVersion,
        tmpDir = tmpDir,
        cloudHome = cloudHome,
        cloudShare = cloudShare,
        referenceGenome = referenceGenome,
        regionsExclude = regionsExclude,
        kgPurcellVcf = kgPurcellVcf,
        dbSNPht = dbSNPht,
        kgSample = kgSample,
        kgSampleId = kgSampleId,
        kgSamplePop = kgSamplePop,
        kgSampleGroup = kgSampleGroup,
        kgVcf = kgVcf,
        kgIds = kgIds,
        humanReferenceWild = humanReferenceWild,
        fasta = fasta,
        vepCacheDir = vepCacheDir,
        vepPluginsDir = vepPluginsDir,
        dbNSFP = dbNSFP,
        vepConservation = vepConservation,
        vepGerpBW = vepGerpBW,
        gnomad = gnomad,
        sampleFile = sampleFile,
        sampleFileId = sampleFileId,
        sampleFileSrSex = sampleFileSrSex,
        sampleFileMaleCode = sampleFileMaleCode,
        sampleFileFemaleCode = sampleFileFemaleCode,
        sampleFileSrRace = sampleFileSrRace,
        sampleFileAFRCodes = sampleFileAFRCodes,
        sampleFileAMRCodes = sampleFileAMRCodes,
        sampleFileEURCodes = sampleFileEURCodes,
        sampleFileEASCodes = sampleFileEASCodes,
        sampleFileSASCodes = sampleFileSASCodes,
        authors = authors,
        email = email,
        organization = organization,
        acknowledgements = acknowledgements,
        nAncestryInferenceFeatures = nAncestryInferenceFeatures,
        ancestryInferenceFeatures = ancestryInferenceFeatures,
        cloudResources = cloudResources,
        resources = resources,
        nArrays = nArrays,
        numericVariantFilters =  numericVariantFilters,
        booleanVariantFilters = booleanVariantFilters,
        categoricalVariantFilters = categoricalVariantFilters,
        compoundVariantFilters = compoundVariantFilters,
        numericSampleFilters =  numericSampleFilters,
        booleanSampleFilters = booleanSampleFilters,
        categoricalSampleFilters = categoricalSampleFilters,
        compoundSampleFilters = compoundSampleFilters,
        Arrays = Arrays
        //Reports = Reports
      )
  
    }
  
    def parseUtils(config: loamstream.conf.DataConfig): Utils = {
  
      val imagesDir = path(checkPath(System.getProperty("imagesDir")))
      val scriptsDir = path(checkPath(System.getProperty("scriptsDir")))
  
      val image = Image(
        imgHail = path(s"${imagesDir}/hail-${projectConfig.hailVersion}.simg"),
        imgLocuszoom = path(s"${imagesDir}/locuszoom.simg"),
        imgPython2 = path(s"${imagesDir}/python2v2.simg"),
        imgR = path(s"${imagesDir}/r.simg"),
        imgTools = path(s"${imagesDir}/tools.simg"),
        imgKing = path(s"${imagesDir}/king-2.2.8.simg"),
        imgPlink2 = path(s"${imagesDir}/plink2_v2.3a.simg"),
        imgBgen = path(s"${imagesDir}/bgen_v1.1.4.simg"),
        imgTexLive = path(s"${imagesDir}/texlive.simg"),
        imgEnsemblVep = path(s"${imagesDir}/ensemblvep_r110.1.simg"),
        imgFlashPca = path(s"${imagesDir}/flashpca.simg"),
        imgImagemagick = path(s"${imagesDir}/imagemagick.simg")
      )
  
      val binary = Binary(
        binLiftOver = path("/usr/local/bin/liftOver"),
        binGenotypeHarmonizer = path("/usr/local/bin/GenotypeHarmonizer.jar"),
        binKing = path("/usr/local/bin/king"),
        binPlink = path("/usr/local/bin/plink"),
        binPlink2 = path("/usr/local/bin/plink2"),
        binBgenix = path("/usr/local/bin/bgenix"),
        binTabix = path("/usr/local/bin/tabix"),
        binBgzip = path("/usr/local/bin/bgzip"),
        binGhostscript = path("/usr/local/bin/gs"),
        binKlustakwik = path("/usr/local/bin/KlustaKwik"),
        binPython = path("/usr/local/bin/python"),
        binLocuszoom = path("/usr/local/bin/locuszoom"),
        binPdflatex = path("/usr/local/bin/pdflatex"),
        binRscript = path("/usr/local/bin/Rscript"),
        binFlashPca = path("/usr/local/bin/flashpca"),
        binConvert = path("/usr/bin/convert")
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
        pyHailExportVcf = path(s"${scriptsDir}/hail_export_vcf.py"),
        pyGenerateReportHeader = path(s"${scriptsDir}/generate_report_header.py"),
        pyGenerateQcReportIntro = path(s"${scriptsDir}/generate_qc_report_intro.py"),
        pyGenerateQcReportData = path(s"${scriptsDir}/generate_qc_report_data.py"),
        pyGenerateQcReportAncestry = path(s"${scriptsDir}/generate_qc_report_ancestry.py"),
        pyGenerateQcReportIbdSexcheck = path(s"${scriptsDir}/generate_qc_report_ibd_sexcheck.py"),
        pyGenerateQcReportSampleqc = path(s"${scriptsDir}/generate_qc_report_sampleqc.py"),
        pyGenerateQcReportVariantqc = path(s"${scriptsDir}/generate_qc_report_variantqc.py"),
        pyGenerateQcReportBibliography = path(s"${scriptsDir}/generate_qc_report_bibliography.py")
      )
  
      val bash = Bash(
        shFindPossibleDuplicateVariants = path(s"${scriptsDir}/find_possible_duplicate_variants.sh"),
        shExtractIndels = path(s"${scriptsDir}/extract_indels.sh"),
        shAnnotate = path(s"${scriptsDir}/annotate.sh"),
        shAnnotateResults = path(s"${scriptsDir}/annotate_results.sh"),
        shKing = path(s"${scriptsDir}/king.sh"),
        shPlinkPrepare = path(s"${scriptsDir}/plink_prepare.sh"),
        shPlinkToVcfNoHalfCalls = path(s"${scriptsDir}/plink_to_vcf_no_half_calls.sh"),
        shKlustakwikPca = path(s"${scriptsDir}/klustakwik_pca.sh"),
        shKlustakwikMetric = path(s"${scriptsDir}/klustakwik_metric.sh"),
        shTabixExtract = path(s"${scriptsDir}/tabix_extract.sh")
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
        rRawVariantsSummaryTable = path(s"${scriptsDir}/raw_variants_summary_table.r"),
        rSeqVariantsSummaryTable = path(s"${scriptsDir}/seq_variants_summary_table.r"),
        rVariantsExcludeSummaryTable = path(s"${scriptsDir}/variants_exclude_summary_table.r"),
        rAncestryClusterTable = path(s"${scriptsDir}/ancestry_cluster_table.r"),
        rMakeOutlierTable = path(s"${scriptsDir}/make_outlier_table.r"),
        rUpsetplotVariantSample = path(s"${scriptsDir}/upsetplot.variant.sample.r"),
        rMakeMetricDistPlot = path(s"${scriptsDir}/make_metric_dist_plot.r")
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

  val gsutilBinaryOpt: Option[Path] = projectContext.config.googleConfig.map(_.gsutilBinary)
  require(gsutilBinaryOpt.isDefined, "Couldn't find gsutil binary path; set loamstream.googlecloud.gsutilBinary in loamstream.conf")

}
