object ProjectStores extends loamstream.LoamFile {

  import ProjectConfig._
  import Fxns._
  import DirTree._
  import Stores._
  
  final case class Known(
    data: Store,
    hiLd: MultiStore
  )
  
  final case class ProjectStores(
    tmpDir: Store,
    kgVcf: Map[String, Store],
    kgIds: Map[String, Store],
    humanReference: Map[String, Store],
    hailUtils: MultiStore,
    regionsExclude: MultiStore,
    kgPurcellVcf: MultiStore,
    dbSNPht: MultiStore,
    kgSample: MultiStore,
    fasta: Store,
    vepCacheDir: Store,
    vepPluginsDir: Store,
    dbNSFP: Store,
    vepConservation: Store,
    vepGerpBW: Option[Store],
    vepGerpFile: Option[Store],
    gnomad: Store,
    sampleFile: MultiStore,
    ancestryInferredGmm: MultiStore,
    ancestryInferredKnn: MultiStore,
    ancestryOutliersGmm: Store)
  
  val projectStores = {
  
    val chrsAll = projectConfig.Arrays.map(e => expandChrList(e.chrs)).flatten.distinct
  
    val kgVcf = projectConfig.Arrays.filter(e => (e.technology == "gwas") && (e.format != "mt")).size match {

      case n if n > 0 =>

        chrsAll.map { chr =>
	    
          val vcf = store(checkPath(projectConfig.kgVcf.get.replace("[CHROMOSOME]", s"$chr"))).asInput
        
          chr -> vcf
        
        }.toMap

      case _ => Map[String, Store]()

    }
    
    val kgIds = projectConfig.Arrays.filter(e => (e.technology == "gwas") && (e.format != "mt")).size match {

      case n if n > 0 =>

        chrsAll.map { chr =>
        
          val ids = store(checkPath(projectConfig.kgIds.get.replace("[CHROMOSOME]", s"$chr"))).asInput
        
          chr -> ids
        
        }.toMap

      case _ => Map[String, Store]()

    }
    
    val humanReference = chrsAll.map { chr =>
    
      val ref = store(checkPath(projectConfig.humanReferenceWild.replace("[CHROMOSOME]", s"$chr"))).asInput
    
      chr -> ref
    
    }.toMap
  
    ProjectStores(
      tmpDir = store(path(checkPath(projectConfig.tmpDir))),
      kgVcf = kgVcf,
      kgIds = kgIds,
      humanReference = humanReference,
      hailUtils = MultiStore(
        local = projectConfig.hailCloud match { case true => Some(store(path(checkPath(utils.python.pyHailUtils.toString()))).asInput); case false => None },
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.base.google.get / s"${utils.python.pyHailUtils}".split("/").last)); case false => None }
      ),
      regionsExclude = MultiStore(
        local = Some(store(path(checkPath(projectConfig.regionsExclude))).asInput),
        google = projectConfig.hailCloud match { case true => Some(store(projectConfig.cloudShare.get / s"${projectConfig.regionsExclude}".split("/").last)); case false => None }
      ),
      kgPurcellVcf = MultiStore(
        local = Some(store(path(checkPath(projectConfig.kgPurcellVcf))).asInput),
        google = projectConfig.hailCloud match { case true => Some(store(projectConfig.cloudShare.get / s"${projectConfig.kgPurcellVcf}".split("/").last)); case false => None }
      ),
      kgSample = MultiStore(
        local = Some(store(path(checkPath(projectConfig.kgSample))).asInput),
        google = projectConfig.hailCloud match { case true => Some(store(projectConfig.cloudShare.get / s"${projectConfig.kgSample}".split("/").last)); case false => None }
      ),
      dbSNPht = MultiStore(
        local = Some(store(path(checkPath(projectConfig.dbSNPht))).asInput),
        google = projectConfig.hailCloud match { case true => Some(store(projectConfig.cloudShare.get / s"${projectConfig.dbSNPht}".split("/").last)); case false => None }
      ),
      fasta = store(path(checkPath(projectConfig.fasta))).asInput,
      vepCacheDir = store(path(checkPath(projectConfig.vepCacheDir))).asInput,
      vepPluginsDir = store(path(checkPath(projectConfig.vepPluginsDir))).asInput,
      dbNSFP = store(path(checkPath(projectConfig.dbNSFP))).asInput,
      vepConservation = store(path(checkPath(projectConfig.vepConservation))).asInput,
      vepGerpBW = projectConfig.vepGerpBW match {
        case Some(s) => Some(store(path(checkPath(s))).asInput)
        case None => None
      },
      vepGerpFile = projectConfig.vepGerpFile match {
        case Some(s) => Some(store(path(checkPath(s))).asInput)
        case None => None
      },
      gnomad = store(path(checkPath(projectConfig.gnomad))).asInput,
      sampleFile = MultiStore(
        local = Some(store(path(projectConfig.sampleFile)).asInput),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / s"${projectConfig.sampleFile}".split("/").last)); case false => None }
      ),
      ancestryInferredGmm = MultiStore(
        local = Some(store(dirTree.dataGlobalGmm.local.get / s"${projectConfig.projectId}.ancestry.gmm.inferred.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobalGmm.google.get / s"${projectConfig.projectId}.ancestry.gmm.inferred.tsv")); case false => None }
      ),
      ancestryInferredKnn = MultiStore(
        local = Some(store(dirTree.dataGlobalKnn.local.get / s"${projectConfig.projectId}.ancestry.knn.inferred.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobalKnn.google.get / s"${projectConfig.projectId}.ancestry.knn.inferred.tsv")); case false => None }
      ),
      ancestryOutliersGmm = store(dirTree.dataGlobalGmm.local.get / s"${projectConfig.projectId}.ancestry.gmm.inferred.outliers.tsv")
    )
  
  }

}
