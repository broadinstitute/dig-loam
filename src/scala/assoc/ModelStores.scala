object ModelStores extends loamstream.LoamFile {

  import ProjectConfig._
  import StoreHelpers._
  import Stores._
  import Fxns._
  import DirTree._
  import Collections._
  import SchemaStores._
  
  //final case class ModelBaseCohortStore(
  //  base: MultiStore,
  //  cohorts: Map[ConfigCohort, MultiStore])
  //
  //final case class ModelBaseMaskStore(
  //  base: MultiStore,
  //  masks: Map[MaskFilter, MultiStore])
  
  //final case class ModelGroupResult(
  //  results: Store,
  //  groupFile: Store)
  
  //final case class ModelResult(
  //  results: Option[MultiStore],
  //  resultsHailLog: Option[MultiStore],
  //  filteredResults: Option[Store],
  //  groups: Map[String, ModelGroup]
  //)
  
  final case class ModelAssocSingle(
    results: MultiStore,
    resultsTbi: Store,
    //filteredResults: Store,
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
  
  final case class ModelAssocGroup(
    results: Store,
    groupFile: Store
  )
  
  final case class ModelAssocGroupBase(
    results: Store,
    top20Results: Store,
    //groupFile: Store,
    qqPlot: Store,
    mhtPlot: Store,
    groups: Map[String, ModelAssocGroup]
  )
  
  //final case class ModelTest(
  //  base: Map[String, ModelResult],
  //  masks: Map[String, Map[MaskFilter, ModelResult]])
  
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
    pedEpacts: Option[Store],
    phenoRegenie: Option[Store],
    covarsRegenie: Option[Store],
    modelVarsEpacts: Option[Store],
    pcsInclude: MultiStore,
    assocSingleHail: Map[String, ModelAssocSingle],
    assocSingleHailLog: Map[String, MultiStore],
    assocGroupEpacts: Map[String, ModelAssocGroupBase],
    assocMaskGroupEpacts: Map[String, Map[MaskFilter, ModelAssocGroupBase]]
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
    val modelGroupEpactsTests = model.tests.filter(e => e.split("\\.")(0) == "group" && e.split("\\.")(1) == "epacts")

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
      phenoDistPlot = store(local_dir / s"${baseString}.pheno.distplot.pdf"),
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
      pedEpacts = model.assocPlatforms.contains("epacts") match { case true => Some(store(local_dir / s"${baseString}.epacts.ped")); case false => None },
      phenoRegenie = model.assocPlatforms.contains("regenie") match { case true => Some(store(local_dir / s"${baseString}.regenie.pheno.tsv")); case false => None },
      covarsRegenie = model.assocPlatforms.contains("regenie") match { case true => Some(store(local_dir / s"${baseString}.regenie.covars.tsv")); case false => None },
      modelVarsEpacts = model.assocPlatforms.contains("epacts") match { case true => Some(store(local_dir / s"${baseString}.epacts.model.vars")); case false => None },
      pcsInclude = MultiStore(
        local = Some(store(local_dir / s"${baseString}.pcs.include.txt")),
        google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.pcs.include.txt")); case false => None }
      ),
      assocSingleHail = model.runAssoc match {
        case true =>
          modelSingleHailTests.map { test =>
            test -> ModelAssocSingle(
              results = MultiStore(
                local = Some(store(local_dir / s"${baseString}.${test}.results.tsv.bgz")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${test}.results.tsv.bgz")); case false => None }
              ),
              resultsTbi = store(local_dir / s"${baseString}.${test}.results.tsv.bgz.tbi"),
              //filteredResults = store(local_dir / s"${baseString}.${test}.results.filtered.tsv.bgz"),
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
          }.toMap
        case false => Map[String, ModelAssocSingle]()
      },
      assocSingleHailLog = model.runAssoc match {
        case true =>
          modelSingleHailTests.map { test =>
            test -> MultiStore(
                local = Some(store(local_dir / s"${baseString}.${test}.results.hail.log")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${test}.results.hail.log")); case false => None }
              )
          }.toMap
        case _ => Map[String, MultiStore]()
      },
      assocGroupEpacts = model.runAssoc match {
        case true =>
          nmasks match {
            case 0 =>
              val gFile = schemaStores((schema, cohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
                case true => checkPath(s"""${schemaStores((schema, cohorts)).epacts.get.groupFile.phenos(pheno).base.local.get.toString.split("@")(1)}""")
                case false => checkPath(s"""${schemaStores((schema, cohorts)).epacts.get.groupFile.base.base.local.get.toString.split("@")(1)}""")
              }
              try {
                val l = fileToList(gFile).map(e => e.split("\t")(0))
                modelGroupEpactsTests.map { test =>
                  test -> ModelAssocGroupBase(
                    results = store(local_dir  / s"${baseString}.${test}.results.tsv.bgz"),
                    top20Results = store(local_dir  / s"${baseString}.${test}.results.top20.tsv"),
                    //groupFile = store(local_dir  / s"${baseString}.${test}.groupfile.tsv"),
                    qqPlot = store(local_dir / s"${baseString}.${test}.results.qqplot.png"),
                    mhtPlot = store(local_dir / s"${baseString}.${test}.results.mhtplot.png"),
                    groups = l.map { group =>
                      group -> ModelAssocGroup(
                        results = store(dirTree.analysisModelGroupsMap(group).local.get / s"${baseString}.${test}.${group}.results.tsv.bgz"),
                        groupFile = store(dirTree.analysisModelGroupsMap(group).local.get / s"${baseString}.${test}.${group}.groupfile.tsv")
                      )
                    }.toMap
                  )
                }.toMap
              }
              catch {
                case x: CfgException =>
                  println(s"""skipping split assoc test by group due to missing group file: ${gFile}""")
                  Map[String, ModelAssocGroupBase]()
              }
            case _ => Map[String, ModelAssocGroupBase]()
          }
        case false => Map[String, ModelAssocGroupBase]()
      },
      assocMaskGroupEpacts = model.runAssoc match {
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
                            top20Results = store(local_dir / s"${baseString}.${test}.${mask.id}.results.top20.tsv"),
                            //groupFile = store(local_dir / s"${baseString}.${test}.${mask.id}.groupfile.tsv"),
                            qqPlot = store(local_dir / s"${baseString}.${test}.${mask.id}.results.qqplot.png"),
                            mhtPlot = store(local_dir / s"${baseString}.${test}.${mask.id}.results.mhtplot.png"),
                            groups = l.map { group =>
                              group -> ModelAssocGroup(
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
                            top20Results = store(local_dir / s"${baseString}.${test}.${mask.id}.results.top20.tsv"),
                            //groupFile = store(local_dir / s"${baseString}.${test}.${mask.id}.groupfile.tsv"),
                            qqPlot = store(local_dir / s"${baseString}.${test}.${mask.id}.results.qqplot.png"),
                            mhtPlot = store(local_dir / s"${baseString}.${test}.${mask.id}.results.mhtplot.png"),
                            groups = l.map { group =>
                              group -> ModelAssocGroup(
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
    )
  }.toMap

}
