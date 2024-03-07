object ModelStores extends loamstream.LoamFile {

  import ProjectConfig._
  import StoreHelpers._
  import Stores._
  import Fxns._
  import DirTree._
  import Collections._
  import SchemaStores._

  //final case class ModelResidualPlots(
  //  base: Path,
  //  dist: Store,
  //  resVsFit: Store,
  //  resVsLev: Store,
  //  sqrtresVsFit: Store,
  //  qq: Store
  //)

  final case class ModelSingleSummary(
    qqPlot: Option[Store],
    qqPlotLowMaf: Option[Store],
    qqPlotMidMaf: Option[Store],
    qqPlotHighMaf: Option[Store],
    qqPlotLog: Option[Store],
    mhtPlot: Option[Store],
    mhtPlotLog: Option[Store],
    top1000Results: Option[Store],
    top1000ResultsAnnot: Option[Store],
    top20AnnotAlignedRisk: Option[Store],
    sigRegions: Option[Store],
    regPlotsBase: Option[Path],
    regPlotsPdf: Option[Store]
  )

  final case class ModelGroupSummary(
    top20Results: Option[Store],
    qqPlot: Option[Store],
    qqPlotLog: Option[Store],
    mhtPlot: Option[Store],
    mhtPlotLog: Option[Store],
    minPVal: Option[Store]
  )
  
  final case class ModelHailAssocSingle(
    results: MultiStore,
    resultsTbi: Store,
    hailLog: MultiStore,
    summary: ModelSingleSummary
  )

  final case class ModelRegenieStep0(
    base: Path,
    exclude: Store
  )

  final case class ModelRegenieStep1(
    base: Path,
    log: Store,
    loco: Seq[Store],
    predList: Store
  )

  final case class ModelRegenieAssocSingleChr(
    base: Path,
    log: Store,
    results: Map[ConfigPheno, Store]
  )

  final case class ModelRegenieAssocSingle(
    base: Path,
    results: Map[ConfigPheno, Store],
    resultsTbi: Map[ConfigPheno, Store],
    chrs: Map[String, ModelRegenieAssocSingleChr],
    summary: Map[ConfigPheno, ModelSingleSummary]
  )
  
  final case class ModelEpactsAssocGroup(
    results: Store,
    groupFile: Store
  )

  final case class ModelRegenieAssocGroupChr(
    base: Path,
    log: Store,
    results: Map[ConfigPheno, Store]
  )

  final case class ModelRegenieAssocGroup(
    base: Path,
    results: Map[ConfigPheno, Store],
    log: Store,
    summary: Map[ConfigPheno, ModelGroupSummary],
    chrs: Map[String, ModelRegenieAssocGroupChr]
  )
  
  final case class ModelAssocGroupBase(
    results: Store,
    summary: ModelGroupSummary,
    groups: Map[String, ModelEpactsAssocGroup]
  )

  final case class ModelHail(
    assocSingle: Map[ConfigTest, ModelHailAssocSingle]
  )
  
  //final case class ModelEpacts(
  //  ped: Store,
  //  modelVars: Store,
  //  assocGroup: Map[ConfigTest, Map[MaskFilter, ModelAssocGroupBase]]
  //)

  final case class ModelRegenie(
    pheno: Store,
    covars: Store,
    step0: ModelRegenieStep0,
    step1: ModelRegenieStep1,
    assocSingle: Map[ConfigTest, ModelRegenieAssocSingle],
    assocGroup: Map[ConfigTest, Map[MaskFilter, ModelRegenieAssocGroup]]
  )
  
  final case class Model(
    sampleMap: Store,
    cohortMap: MultiStore,
    phenoTable: MultiStore,
    phenoPrelim: Store,
    samplesAvailable: Store,
    samplesAvailableLog: Store,
    //phenoDistPlot: Store,
    modelVarsSummary: Store,
    variantStats: Map[ConfigPheno, MultiStore],
    variantStatsHailLog: Map[ConfigPheno, MultiStore],
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
    phenoList: MultiStore,
    pcsInclude: MultiStore,
    //residualPlots: Option[ModelResidualPlots],
    hail: Option[ModelHail],
    //epacts: Option[ModelEpacts],
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
    val phenos = projectConfig.Phenos.filter(e => model.pheno.contains(e.id))
    val tests = projectConfig.Tests.filter(e => model.tests.get.contains(e.id))
  
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

    //var phenoMasksAvailable = Seq[MaskFilter]()
    //schema.masks match {
    //  case Some(_) =>
    //    schemaStores((schema, cohorts)).epacts match {
    //      case Some(_) =>
    //        schemaStores((schema, cohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
    //          case true => 
    //            for {
    //              sm <- schema.masks.get
    //            } yield {
    //              try {
    //                checkPath(s"""${schemaStores((schema, cohorts)).epacts.get.groupFile.phenos(pheno).masks(sm).local.get.toString.split("@")(1)}""")
    //                phenoMasksAvailable = phenoMasksAvailable ++ Seq(sm)
    //              }
    //              catch {
    //                case x: CfgException =>
    //                  println(s"""skipping split assoc test by group due to missing group file: ${schemaStores((schema, cohorts)).epacts.get.groupFile.phenos(pheno).masks(sm).local.get.toString.split("@")(1)}""")
    //              }
    //            }
    //          case false => ()
    //        }
    //      case None => ()
    //    }
    //  case None => ()
    //}
  
    //var masksAvailable = Seq[MaskFilter]()
    //schema.masks match {
    //  case Some(_) =>
    //    schemaStores((schema, cohorts)).epacts match {
    //      case Some(_) =>
    //        for {
    //          sm <- schema.masks.get
    //        } yield {
    //          try {
    //            checkPath(s"""${schemaStores((schema, cohorts)).epacts.get.groupFile.base.masks(sm).local.get.toString.split("@")(1)}""")
    //            masksAvailable = masksAvailable ++ Seq(sm)
    //          }
    //          catch {
    //            case x: CfgException =>
    //              println(s"""skipping split assoc test by group due to missing group file: ${schemaStores((schema, cohorts)).epacts.get.groupFile.base.masks(sm).local.get.toString.split("@")(1)}""")
    //          }
    //        }
    //      case None => ()
    //    }
    //  case None => ()
    //}

    val variantStats = model.methods match {
      case Some(s) =>
        s.contains("variant.stats") match {
          case true =>
            phenos.map  { pheno =>
              pheno ->
			    MultiStore(
                  local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant_stats.tsv.bgz")),
                  google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant_stats.tsv.bgz")); case false => None }
                )
            }.toMap
          case false => Map[ConfigPheno, MultiStore]()
        }
      case None => Map[ConfigPheno, MultiStore]()
    }

    val variantStatsHailLog = model.methods match {
      case Some(s) =>
        s.contains("variant.stats") match {
          case true =>
            phenos.map  { pheno =>
              pheno ->
			    MultiStore(
                  local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant_stats.hail.log")),
                  google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant_stats.hail.log")); case false => None }
                )
            }.toMap
          case false => Map[ConfigPheno, MultiStore]()
        }
      case None => Map[ConfigPheno, MultiStore]()
    }
  
    sm -> Model(
      sampleMap = store(local_dir / s"${baseString}.sample.map.tsv"),
      cohortMap = MultiStore(
        local = Some(store(local_dir / s"${baseString}.cohort.map.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.cohort.map.tsv")); case false => None }
      ),
      phenoTable = MultiStore(
        local = Some(store(local_dir / s"${baseString}.pheno.table.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.pheno.table.tsv")); case false => None }
      ),
      phenoPrelim = store(local_dir / s"${baseString}.pheno.prelim.tsv"),
      samplesAvailable = store(local_dir / s"${baseString}.samples.available.txt"),
      samplesAvailableLog = store(local_dir / s"${baseString}.samples.available.log"),
      //phenoDistPlot = store(local_dir / s"${baseString}.pheno.distplot.png"),
      modelVarsSummary = store(local_dir / s"${baseString}.model.vars_summary.tsv"),
      variantStats = variantStats,
      variantStatsHailLog = variantStatsHailLog,
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
      phenoList = MultiStore(
        local = Some(store(local_dir / s"${baseString}.phenotype_list.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.phenotype_list.tsv")); case false => None }
      ),
      pcsInclude = MultiStore(
        local = Some(store(local_dir / s"${baseString}.pcs.include.txt")),
        google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.pcs.include.txt")); case false => None }
      ),
      //residualPlots = pheno.binary match {
      //  case true => None
      //  case false => Some(ModelResidualPlots(
      //    base = local_dir / s"${baseString}.residuals",
      //    dist = store(local_dir / s"${baseString}.residuals.dist.png"),
      //    resVsFit = store(local_dir / s"${baseString}.residuals.res_vs_fit.png"),
      //    resVsLev = store(local_dir / s"${baseString}.residuals.res_vs_lev.png"),
      //    sqrtresVsFit = store(local_dir / s"${baseString}.residuals.sqrtres_vs_fit.png"),
      //    qq = store(local_dir / s"${baseString}.residuals.qq.png")
      //  ))
      //},
      hail = model.tests match {
        case Some(_) =>
          model.assocPlatforms.get.contains("hail") match {
            case true =>
              Some(ModelHail(
                assocSingle = tests.filter(e => (e.grouped == false && e.platform == "hail")).map { test =>
                  test -> 
                    ModelHailAssocSingle(
                      results = MultiStore(
                        local = Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.tsv.bgz")),
                        google = projectConfig.hailCloud match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).google.get / s"${baseString}.${test.id}.results.tsv.bgz")); case false => None }
                      ),
                      resultsTbi = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.tsv.bgz.tbi"),
                      hailLog = MultiStore(
                        local = Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.hail.log")),
                        google = projectConfig.hailCloud match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).google.get / s"${baseString}.${test.id}.results.hail.log")); case false => None }
                      ),
                      summary = ModelSingleSummary(
                        qqPlot = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.qqplot.png")); case false => None },
                        qqPlotLowMaf = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.qqplot.lowmaf.png")); case false => None },
                        qqPlotMidMaf = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.qqplot.midmaf.png")); case false => None },
                        qqPlotHighMaf = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.qqplot.highmaf.png")); case false => None },
                        qqPlotLog = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.qqplot.log")); case false => None },
                        mhtPlot = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.mhtplot.png")); case false => None },
                        mhtPlotLog = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.mhtplot.log")); case false => None },
                        top1000Results = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.top1000.tsv")); case false => None },
                        top1000ResultsAnnot = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.top1000.annot.tsv")); case false => None },
                        top20AnnotAlignedRisk = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.top20.annot.aligned_risk.tsv")); case false => None },
                        sigRegions = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.sig.regions.tsv")); case false => None },
                        regPlotsBase = model.summarize match { case true => Some(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.sig.regplots"); case false => None },
                        regPlotsPdf = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.results.sig.regplots.pdf")); case false => None }
                      )
                    )
                }.toMap
              ))
            case false => None
          }
        case None => None
      },
      //epacts = model.tests match {
      //  case Some(_) =>
      //    model.assocPlatforms.get.contains("epacts") match {
      //      case true =>
      //        Some(ModelEpacts(
      //          ped = store(local_dir / s"${baseString}.epacts.ped"),
      //          modelVars = store(local_dir / s"${baseString}.epacts.model.vars"),
      //          assocGroup = masksAvailable.size match {
      //            case 0 => Map[ConfigTest, Map[MaskFilter, ModelAssocGroupBase]]()
      //            case _ =>
      //              schemaStores((schema, cohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
      //                case true =>
      //                  tests.filter(e => (e.grouped == true && e.platform == "epacts")).map { test => 
      //                    test ->
      //                      phenoMasksAvailable.map { mask =>
      //                        val gFile = checkPath(s"""${schemaStores((schema, cohorts)).epacts.get.groupFile.phenos(pheno).masks(mask).local.get.toString.split("@")(1)}""")
      //                        val l = fileToList(gFile).map(e => e.split("\t")(0))
      //                        mask ->
      //                          ModelAssocGroupBase(
      //                            results = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.results.tsv.bgz"),
      //                            summary = ModelGroupSummary(
      //                              top20Results = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.results.top20.tsv"),
      //                              qqPlot = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.results.qqplot.png"),
      //                              mhtPlot = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.results.mhtplot.png"),
      //                              minPVal = None 
      //                            ),
      //                            groups = l.map { group =>
      //                              group -> ModelEpactsAssocGroup(
      //                                results = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.${group}.results.tsv.bgz"),
      //                                groupFile = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.${group}.groupfile.tsv")
      //                              )
      //                            }.toMap
      //                          )
      //                      }.toMap
      //                  }.toMap
      //                case false =>
      //                  tests.filter(e => (e.grouped == true && e.platform == "epacts")).map { test => 
      //                    test ->
      //                      masksAvailable.map { mask =>
      //                        val gFile = checkPath(s"""${schemaStores((schema, cohorts)).epacts.get.groupFile.base.masks(mask).local.get.toString.split("@")(1)}""")
      //                        val l = fileToList(gFile).map(e => e.split("\t")(0))
      //                        mask ->
      //                          ModelAssocGroupBase(
      //                            results = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.results.tsv.bgz"),
      //                            summary = ModelGroupSummary(
      //                              top20Results = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.results.top20.tsv"),
      //                              qqPlot = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.results.qqplot.png"),
      //                              mhtPlot = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.results.mhtplot.png"),
      //                              minPVal = None 
      //                            
      //                            ),
      //                            groups = l.map { group =>
      //                              group -> ModelEpactsAssocGroup(
      //                                results = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.${group}.results.tsv.bgz"),
      //                                groupFile = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${mask.id}.${group}.groupfile.tsv")
      //                              )
      //                            }.toMap
      //                          )
      //                      }.toMap
      //                  }.toMap
      //              }
      //          }
      //        ))
      //      case false => None
      //    }
      //  case None => None
      //},
      regenie = model.tests match {
        case Some(_) =>
          model.assocPlatforms.get.contains("regenie") match {
            case true => 
              Some(ModelRegenie(
                pheno = store(local_dir / s"${baseString}.regenie.pheno.tsv"),
                covars = store(local_dir / s"${baseString}.regenie.covars.tsv"),
                step0 = ModelRegenieStep0(
                  base = local_dir / s"${baseString}.regenie.step0",
                  exclude = store(local_dir / s"${baseString}.regenie.step0.zero_variance_exclude.txt")
                ),
                step1 = ModelRegenieStep1(
                  base = local_dir / s"${baseString}.regenie.step1",
                  log = store(local_dir / s"${baseString}.regenie.step1.log"),
                  loco = for {
                    i <- 1 to phenos.size
                  } yield {
                    store(local_dir / s"${baseString}.regenie.step1_${i}.loco")
                  },
                  predList = store(local_dir / s"${baseString}.regenie.step1_pred.list")
                ),
                assocSingle = tests.filter(e => (e.grouped == false && e.platform == "regenie")).map { test => 
                  test -> 
                    ModelRegenieAssocSingle(
                      base = dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}",
                      results = phenos.map { pheno =>
                        pheno -> store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.tsv.bgz")
                      }.toMap,
                      resultsTbi = phenos.map { pheno =>
                        pheno -> store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.tsv.bgz.tbi")
                      }.toMap,
                      chrs = expandChrList(array.chrs).map { chr =>
                        chr ->
                          ModelRegenieAssocSingleChr(
                            base = dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.chr${chr}",
                            log = store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.chr${chr}.log"),
                            results = phenos.map { pheno =>
                              pheno -> store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.chr${chr}.${pheno.id}.results.tsv.bgz")
                            }.toMap
                          )
                      }.toMap,
                      summary = phenos.map { pheno =>
                        pheno ->
                          ModelSingleSummary(
                            qqPlot = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.qqplot.png")); case false => None },
                            qqPlotLowMaf = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.qqplot.lowmaf.png")); case false => None },
                            qqPlotMidMaf = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.qqplot.midmaf.png")); case false => None },
                            qqPlotHighMaf = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.qqplot.highmaf.png")); case false => None },
                            qqPlotLog = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.qqplot.log")); case false => None },
                            mhtPlot = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.mhtplot.png")); case false => None },
                            mhtPlotLog = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.mhtplot.log")); case false => None },
                            top1000Results = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.top1000.tsv")); case false => None },
                            top1000ResultsAnnot = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.top1000.annot.tsv")); case false => None },
                            top20AnnotAlignedRisk = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.top20.annot.aligned_risk.tsv")); case false => None },
                            sigRegions = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.sig.regions.tsv")); case false => None },
                            regPlotsBase = model.summarize match { case true => Some(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.sig.regplots"); case false => None },
                            regPlotsPdf = model.summarize match { case true => Some(store(dirTree.analysisModelTestMap(model)(test).local.get / s"${baseString}.${test.id}.${pheno.id}.results.sig.regplots.pdf")); case false => None }
                          )
                      }.toMap
                    )
                }.toMap,
                assocGroup = schema.masks match {
                  case Some(_) =>
                    tests.filter(e => (e.grouped == true && e.platform == "regenie")).map { test => 
                      test ->
                        schema.masks.get.map { mask =>
                          mask ->
                            ModelRegenieAssocGroup(
                              base = dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}",
                              results = phenos.map { pheno =>
                                pheno -> store(dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}.${pheno.id}.results.tsv.bgz")
                              }.toMap,
                              log = store(dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}.log"),
                              summary = phenos.map { pheno =>
                                pheno ->
                                  ModelGroupSummary(
                                    top20Results = model.summarize match { case true => Some(store(dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}.${pheno.id}.results.top20.tsv")); case false => None },
                                    qqPlot = model.summarize match { case true => Some(store(dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}.${pheno.id}.results.qqplot.png")); case false => None },
                                    qqPlotLog = model.summarize match { case true => Some(store(dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}.${pheno.id}.results.qqplot.log")); case false => None },
                                    mhtPlot = model.summarize match { case true => Some(store(dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}.${pheno.id}.results.mhtplot.png")); case false => None },
                                    mhtPlotLog = model.summarize match { case true => Some(store(dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}.${pheno.id}.results.mhtplot.log")); case false => None },
                                    minPVal = model.summarize match { case true => Some(store(dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}.${pheno.id}.results.minpval.tsv")); case false => None }
                                  )
                              }.toMap,
                              chrs = expandChrList(array.chrs).map { chr =>
                                chr ->
                                  ModelRegenieAssocGroupChr(
                                    base = dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}.chr${chr}",
                                    log = store(dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}.chr${chr}.log"),
                                    results = phenos.map { pheno =>
                                      pheno -> store(dirTree.analysisModelTestMaskMap(model)(test)(mask).local.get / s"${baseString}.${test.id}.${mask.id}.chr${chr}.${pheno.id}.results.tsv.bgz")
                                    }.toMap
                                  )
                              }.toMap
                            )
                        }.toMap
                    }.toMap
                  case None => Map[ConfigTest, Map[MaskFilter, ModelRegenieAssocGroup]]()
                }
              ))
            case false => None
          }
        case None => None
      }
    )
  }.toMap

}
