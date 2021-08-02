object ModelStores extends loamstream.LoamFile {

  import ProjectConfig._
  import StoreHelpers._
  import Stores._
  import Fxns._
  import DirTree._
  import Collections._
  import SchemaStores._

  final case class ModelResidualPlots(
    base: Path,
    resVsFit: Store,
    resVsLev: Store,
    sqrtresVsFit: Store,
    qq: Store
  )

  final case class ModelSingleSummary(
    qqPlot: Store,
    qqPlotLowMaf: Store,
    qqPlotMidMaf: Store,
    qqPlotHighMaf: Store,
    mhtPlot: Store,
    top1000Results: Store,
    top1000ResultsAnnot: Store,
    top20AnnotAlignedRisk: Store,
    sigRegions: Store,
    regPlotsBase: Path,
    regPlotsPdf: Store
  )

  final case class ModelGroupSummary(
    top20Results: Store,
    qqPlot: Store,
    mhtPlot: Store,
    minPVal: Option[Store]
  )
  
  final case class ModelHailAssocSingle(
    results: MultiStore,
    resultsTbi: Store,
    hailLog: MultiStore,
    summary: ModelSingleSummary
  )

  final case class ModelRegenieStep1(
    base: Path,
    log: Store,
    loco: Store,
    predList: Store
  )

  final case class ModelRegenieAssocSingleChr(
    base: Path,
    log: Store,
    results: Store
  )

  final case class ModelRegenieAssocSingle(
    base: Path,
    results: Store,
    resultsTbi: Store,
    chrs: Map[String, ModelRegenieAssocSingleChr],
    summary: ModelSingleSummary
  )
  
  final case class ModelEpactsAssocGroup(
    results: Store,
    groupFile: Store
  )

  final case class ModelRegenieAssocGroupChr(
    base: Path,
    log: Store,
    results: Store
  )

  final case class ModelRegenieAssocGroup(
    base: Path,
    results: Store,
    summary: ModelGroupSummary,
    chrs: Map[String, ModelRegenieAssocGroupChr]
  )
  
  final case class ModelAssocGroupBase(
    results: Store,
    summary: ModelGroupSummary,
    groups: Map[String, ModelEpactsAssocGroup]
  )

  final case class ModelHail(
    assocSingle: Map[String, ModelHailAssocSingle]
  )
  
  final case class ModelEpacts(
    ped: Store,
    modelVars: Store,
    assocGroup: Map[String, Map[MaskFilter, ModelAssocGroupBase]]
  )

  final case class ModelRegenie(
    pheno: Store,
    covars: Store,
    step1: ModelRegenieStep1,
    assocSingle: Map[String, ModelRegenieAssocSingle],
    assocGroup: Map[String, ModelRegenieAssocGroup]
  )
  
  final case class Model(
    sampleMap: Store,
    cohortMap: MultiStore,
    phenoPrelim: Store,
    samplesAvailable: Store,
    samplesAvailableLog: Store,
    phenoDistPlot: Store,
    pcaBase: Path,
    pcaScores: Store, 
    pcaEigenVecs: Store, 
    pcaLoadings: Store, 
    pcaEigenVals: Store, 
    pcaPve: Store, 
    pcaMeansd: Store, 
    outliers: Store, 
    pcaLog: Store,
    pheno: MultiStore,
    pcsInclude: MultiStore,
    residualPlots: ModelResidualPlots,
    hail: Option[ModelHail],
    epacts: Option[ModelEpacts],
    regenie: Option[ModelRegenie]

    //pedEpacts: Option[Store],
    //phenoRegenie: Option[Store],
    //covarsRegenie: Option[Store],
    //modelVarsEpacts: Option[Store],
    //assocSingleHail: Map[String, ModelAssocSingle],
    //assocSingleHailLog: Map[String, MultiStore],
    //assocGroupEpacts: Map[String, ModelAssocGroupBase],
    //assocMaskGroupEpacts: Map[String, Map[MaskFilter, ModelAssocGroupBase]]
  )
  
  val modelStores = (
    (for {
      x <- modelCollections
    } yield {
      (x.model, x.schema, x.cohorts, None)
    }) ++
    (for {
      x <- modelMetaCollections
    } yield {
      (x.model, x.schema, x.cohorts, Some(x.meta))
    })).map { sm =>
  
    val model = sm._1
    val schema = sm._2
    val cohorts = sm._3
    val meta: Option[ConfigMeta] = sm._4
  
    val array = projectConfig.Arrays.filter(e => e.id == cohorts.head.array).head
    val pheno = projectConfig.Phenos.filter(e => e.id == model.pheno).head
  
    val nullString = meta match {
      case Some(s) => s"${projectConfig.projectId}.${model.id}.${meta.get.id}"
      case None => s"${projectConfig.projectId}.${model.id}"
    }
  
    val baseString = schema.design match {
      case "full" => s"${nullString}"
      case "strat" => s"${nullString}.${cohorts.head.id}"
    }
  
    val local_dir = dirTree.analysisModelMap(model).local.get
  
    val cloud_dir = projectConfig.hailCloud match {
        case true => Some(dirTree.analysisModelMap(model).google.get)
        case false => None
    }
  
    val nmasks = schema.masks match {
      case Some(_) => schema.masks.get.size
      case None => 0
    }
  
    val modelSingleHailTests = model.tests.filter(e => e.split("\\.")(0) == "single" && e.split("\\.")(1) == "hail")
    val modelSingleRegenieTests = model.tests.filter(e => e.split("\\.")(0) == "single" && e.split("\\.")(1) == "regenie")
    val modelGroupEpactsTests = model.tests.filter(e => e.split("\\.")(0) == "group" && e.split("\\.")(1) == "epacts")
    val modelGroupRegenieTests = model.tests.filter(e => e.split("\\.")(0) == "group" && e.split("\\.")(1) == "regenie")

    var phenoMasksAvailable = Seq[MaskFilter]()
    schema.masks match {
      case Some(_) =>
        schemaStores((schema, cohorts)).epacts match {
          case Some(_) =>
            schemaStores((schema, cohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
              case true => 
                for {
                  sm <- schema.masks.get
                } yield {
                  try {
                    checkPath(s"""${schemaStores((schema, cohorts)).epacts.get.groupFile.phenos(pheno).masks(sm).local.get.toString.split("@")(1)}""")
                    phenoMasksAvailable = phenoMasksAvailable ++ Seq(sm)
                  }
                  catch {
                    case x: CfgException =>
                      println(s"""skipping split assoc test by group due to missing group file: ${schemaStores((schema, cohorts)).epacts.get.groupFile.phenos(pheno).masks(sm).local.get.toString.split("@")(1)}""")
                  }
                }
              case false => ()
            }
          case None => ()
        }
      case None => ()
    }
  
    var masksAvailable = Seq[MaskFilter]()
    schema.masks match {
      case Some(_) =>
        schemaStores((schema, cohorts)).epacts match {
          case Some(_) =>
            for {
              sm <- schema.masks.get
            } yield {
              try {
                checkPath(s"""${schemaStores((schema, cohorts)).epacts.get.groupFile.base.masks(sm).local.get.toString.split("@")(1)}""")
                masksAvailable = masksAvailable ++ Seq(sm)
              }
              catch {
                case x: CfgException =>
                  println(s"""skipping split assoc test by group due to missing group file: ${schemaStores((schema, cohorts)).epacts.get.groupFile.base.masks(sm).local.get.toString.split("@")(1)}""")
              }
            }
          case None => ()
        }
      case None => ()
    }
  
    sm -> Model(
      sampleMap = store(local_dir / s"${baseString}.sample.map.tsv"),
      cohortMap = MultiStore(
        local = Some(store(local_dir / s"${baseString}.cohort.map.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.cohort.map.tsv")); case false => None }
      ),
      phenoPrelim = store(local_dir / s"${baseString}.pheno.prelim.tsv"),
      samplesAvailable = store(local_dir / s"${baseString}.samples.available.txt"),
      samplesAvailableLog = store(local_dir / s"${baseString}.samples.available.log"),
      phenoDistPlot = store(local_dir / s"${baseString}.pheno.distplot.png"),
      pcaBase = local_dir / s"${baseString}.pca",
      pcaScores = store(local_dir / s"${baseString}.pca.scores.tsv"),
      pcaEigenVecs = store(local_dir / s"${baseString}.pca.eigenvecs.tsv"),
      pcaLoadings = store(local_dir / s"${baseString}.pca.loadings.tsv"),
      pcaEigenVals = store(local_dir / s"${baseString}.pca.eigenvals.txt"),
      pcaPve = store(local_dir / s"${baseString}.pca.pve.txt"),
      pcaMeansd = store(local_dir / s"${baseString}.pca.meansd.tsv"),
      outliers = store(local_dir / s"${baseString}.pca.outliers.txt"),
      pcaLog = store(local_dir / s"${baseString}.pca.log"),
      pheno = MultiStore(
        local = Some(store(local_dir / s"${baseString}.pheno.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.pheno.tsv")); case false => None }
      ),
      pcsInclude = MultiStore(
        local = Some(store(local_dir / s"${baseString}.pcs.include.txt")),
        google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.pcs.include.txt")); case false => None }
      ),
      residualPlots = ModelResidualPlots(
        base = local_dir / s"${baseString}.residuals",
        resVsFit = store(local_dir / s"${baseString}.residuals.res_vs_fit.png"),
        resVsLev = store(local_dir / s"${baseString}.residuals.res_vs_lev.png"),
        sqrtresVsFit = store(local_dir / s"${baseString}.residuals.sqrtres_vs_fit.png"),
        qq = store(local_dir / s"${baseString}.residuals.qq.png")
      ),
      hail = model.assocPlatforms.contains("hail") match {
        case true =>
          Some(ModelHail(
            assocSingle = model.runAssoc match {
              case true =>
                modelSingleHailTests.map { test =>
                  test -> 
                    ModelHailAssocSingle(
                      results = MultiStore(
                        local = Some(store(local_dir / s"${baseString}.${test}.results.tsv.bgz")),
                        google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${test}.results.tsv.bgz")); case false => None }
                      ),
                      resultsTbi = store(local_dir / s"${baseString}.${test}.results.tsv.bgz.tbi"),
                      hailLog = MultiStore(
                        local = Some(store(local_dir / s"${baseString}.${test}.results.hail.log")),
                        google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${test}.results.hail.log")); case false => None }
                      ),
                      summary = ModelSingleSummary(
                        qqPlot = store(local_dir / s"${baseString}.${test}.results.qqplot.png"),
                        qqPlotLowMaf = store(local_dir / s"${baseString}.${test}.results.qqplot.lowmaf.png"),
                        qqPlotMidMaf = store(local_dir / s"${baseString}.${test}.results.qqplot.midmaf.png"),
                        qqPlotHighMaf = store(local_dir / s"${baseString}.${test}.results.qqplot.highmaf.png"),
                        mhtPlot = store(local_dir / s"${baseString}.${test}.results.mhtplot.png"),
                        top1000Results = store(local_dir / s"${baseString}.${test}.results.top1000.tsv"),
                        top1000ResultsAnnot = store(local_dir / s"${baseString}.${test}.results.top1000.annot.tsv"),
                        top20AnnotAlignedRisk = store(local_dir / s"${baseString}.${test}.results.top20.annot.aligned_risk.tsv"),
                        sigRegions = store(local_dir / s"${baseString}.${test}.results.sig.regions.tsv"),
                        regPlotsBase = local_dir / s"${baseString}.${test}.results.sig.regplots",
                        regPlotsPdf = store(local_dir / s"${baseString}.${test}.results.sig.regplots.pdf")
                      )
                    )
                }.toMap
              case false => Map[String, ModelHailAssocSingle]()
            }
          ))
        case false => None
      },
      epacts = model.assocPlatforms.contains("epacts") match {
        case true =>
          Some(ModelEpacts(
            ped = store(local_dir / s"${baseString}.epacts.ped"),
            modelVars = store(local_dir / s"${baseString}.epacts.model.vars"),
            assocGroup = model.runAssoc match {
              case true =>
                masksAvailable.size match {
                  case 0 => Map[String, Map[MaskFilter, ModelAssocGroupBase]]()
                  case _ =>
                    schemaStores((schema, cohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
                      case true => 
                        modelGroupEpactsTests.map { test =>
                          test ->
                            phenoMasksAvailable.map { mask =>
                              val gFile = checkPath(s"""${schemaStores((schema, cohorts)).epacts.get.groupFile.phenos(pheno).masks(mask).local.get.toString.split("@")(1)}""")
                              val l = fileToList(gFile).map(e => e.split("\t")(0))
                              mask ->
                                ModelAssocGroupBase(
                                  results = store(local_dir / s"${baseString}.${test}.${mask.id}.results.tsv.bgz"),
                                  summary = ModelGroupSummary(
                                    top20Results = store(local_dir / s"${baseString}.${test}.${mask.id}.results.top20.tsv"),
                                    qqPlot = store(local_dir / s"${baseString}.${test}.${mask.id}.results.qqplot.png"),
                                    mhtPlot = store(local_dir / s"${baseString}.${test}.${mask.id}.results.mhtplot.png"),
                                    minPVal = None 
                                  ),
                                  groups = l.map { group =>
                                    group -> ModelEpactsAssocGroup(
                                      results = store(dirTree.analysisModelGroupsMap(group).local.get / s"${baseString}.${test}.${mask.id}.${group}.results.tsv.bgz"),
                                      groupFile = store(dirTree.analysisModelGroupsMap(group).local.get / s"${baseString}.${test}.${mask.id}.${group}.groupfile.tsv")
                                    )
                                  }.toMap
                                )
                            }.toMap
                        }.toMap
                      case false =>
                        modelGroupEpactsTests.map { test =>
                          test ->
                            masksAvailable.map { mask =>
                              val gFile = checkPath(s"""${schemaStores((schema, cohorts)).epacts.get.groupFile.base.masks(mask).local.get.toString.split("@")(1)}""")
                              val l = fileToList(gFile).map(e => e.split("\t")(0))
                              mask ->
                                ModelAssocGroupBase(
                                  results = store(local_dir / s"${baseString}.${test}.${mask.id}.results.tsv.bgz"),
                                  summary = ModelGroupSummary(
                                    top20Results = store(local_dir / s"${baseString}.${test}.${mask.id}.results.top20.tsv"),
                                    qqPlot = store(local_dir / s"${baseString}.${test}.${mask.id}.results.qqplot.png"),
                                    mhtPlot = store(local_dir / s"${baseString}.${test}.${mask.id}.results.mhtplot.png"),
                                    minPVal = None 
                                  
                                  ),
                                  groups = l.map { group =>
                                    group -> ModelEpactsAssocGroup(
                                      results = store(dirTree.analysisModelGroupsMap(group).local.get / s"${baseString}.${test}.${mask.id}.${group}.results.tsv.bgz"),
                                      groupFile = store(dirTree.analysisModelGroupsMap(group).local.get / s"${baseString}.${test}.${mask.id}.${group}.groupfile.tsv")
                                    )
                                  }.toMap
                                )
                            }.toMap
                        }.toMap
                    }
                }
              case false => Map[String, Map[MaskFilter, ModelAssocGroupBase]]()
            }
          ))
        case false => None
      },
      regenie = model.assocPlatforms.contains("regenie") match {
        case true => 
          Some(ModelRegenie(
            pheno = store(local_dir / s"${baseString}.regenie.pheno.tsv"),
            covars = store(local_dir / s"${baseString}.regenie.covars.tsv"),
            step1 = ModelRegenieStep1(
              base = local_dir / s"${baseString}.regenie.step1",
              log = store(local_dir / s"${baseString}.regenie.step1.log"),
              loco = store(local_dir / s"${baseString}.regenie.step1_1.loco"),
              predList = store(local_dir / s"${baseString}.regenie.step1_pred.list")
            ),
            assocSingle = model.runAssoc match {
              case true =>
                modelSingleRegenieTests.map { test =>
                  test -> 
                    ModelRegenieAssocSingle(
                      base = local_dir / s"${baseString}.${test}",
                      results = store(local_dir / s"${baseString}.${test}.results.tsv.bgz"),
                      resultsTbi = store(local_dir / s"${baseString}.${test}.results.tsv.bgz.tbi"),
                      chrs = expandChrList(array.chrs).map { chr =>
                        chr ->
                          ModelRegenieAssocSingleChr(
                            base = dirTree.analysisModelChrsMap(chr).local.get / s"${baseString}.${test}.chr${chr}",
                            log = store(dirTree.analysisModelChrsMap(chr).local.get / s"${baseString}.${test}.chr${chr}.log"),
                            results = store(dirTree.analysisModelChrsMap(chr).local.get / s"${baseString}.${test}.chr${chr}.results.tsv.bgz")
                          )
                      }.toMap,
                      summary = ModelSingleSummary(
                        qqPlot = store(local_dir / s"${baseString}.${test}.results.qqplot.png"),
                        qqPlotLowMaf = store(local_dir / s"${baseString}.${test}.results.qqplot.lowmaf.png"),
                        qqPlotMidMaf = store(local_dir / s"${baseString}.${test}.results.qqplot.midmaf.png"),
                        qqPlotHighMaf = store(local_dir / s"${baseString}.${test}.results.qqplot.highmaf.png"),
                        mhtPlot = store(local_dir / s"${baseString}.${test}.results.mhtplot.png"),
                        top1000Results = store(local_dir / s"${baseString}.${test}.results.top1000.tsv"),
                        top1000ResultsAnnot = store(local_dir / s"${baseString}.${test}.results.top1000.annot.tsv"),
                        top20AnnotAlignedRisk = store(local_dir / s"${baseString}.${test}.results.top20.annot.aligned_risk.tsv"),
                        sigRegions = store(local_dir / s"${baseString}.${test}.results.sig.regions.tsv"),
                        regPlotsBase = local_dir / s"${baseString}.${test}.results.sig.regplots",
                        regPlotsPdf = store(local_dir / s"${baseString}.${test}.results.sig.regplots.pdf")
                      )
                    )
                }.toMap
              case false => Map[String, ModelRegenieAssocSingle]()
            },
            assocGroup = model.runAssoc match {
              case true =>
                modelGroupRegenieTests.map { test =>
                  test -> 
                    ModelRegenieAssocGroup(
                      base = local_dir / s"${baseString}.${test}",
                      results = store(local_dir / s"${baseString}.${test}.results.tsv.bgz"),
                      summary = ModelGroupSummary(
                        top20Results = store(local_dir / s"${baseString}.${test}.results.top20.tsv"),
                        qqPlot = store(local_dir / s"${baseString}.${test}.results.qqplot.png"),
                        mhtPlot = store(local_dir / s"${baseString}.${test}.results.mhtplot.png"),
                        minPVal = Some(store(local_dir / s"${baseString}.${test}.results.minpval.tsv"))
                      ),
                      chrs = expandChrList(array.chrs).map { chr =>
                        chr ->
                          ModelRegenieAssocGroupChr(
                            base = dirTree.analysisModelChrsMap(chr).local.get / s"${baseString}.${test}.chr${chr}",
                            log = store(dirTree.analysisModelChrsMap(chr).local.get / s"${baseString}.${test}.chr${chr}.log"),
                            results = store(dirTree.analysisModelChrsMap(chr).local.get / s"${baseString}.${test}.chr${chr}.results.tsv.bgz")
                          )
                      }.toMap
                    )
                }.toMap
              case false => Map[String, ModelRegenieAssocGroup]()
            }
          ))
        case false => None
      }
    )
  }.toMap

}
