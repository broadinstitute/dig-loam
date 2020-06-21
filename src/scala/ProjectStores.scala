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
    kgVcf: Map[String, Store],
    kgIds: Map[String, Store],
    humanReference: Map[String, Store],
    knownStores: Map[ConfigKnown, Known],
    hailUtils: MultiStore,
    regionsExclude: MultiStore,
    geneIdMap: MultiStore,
    kgPurcellVcf: MultiStore,
    kgSample: MultiStore,
    fasta: Store,
    vepCacheDir: Store,
    vepPluginsDir: Store,
    dbNSFP: Store,
    sampleFile: MultiStore,
    phenoFile: MultiStore,
    ancestryInferred: MultiStore,
    ancestryOutliers: Store)
  
  val projectStores = {
  
    val chrsAll = projectConfig.Arrays.map(e => expandChrList(e.chrs)).flatten.distinct
  
    val kgVcf = chrsAll.map { chr =>
    
      val vcf = store(checkPath(projectConfig.kgVcf.replace("[CHROMOSOME]", s"$chr"))).asInput
    
      chr -> vcf
    
    }.toMap
    
    val kgIds = chrsAll.map { chr =>
    
      val ids = store(checkPath(projectConfig.kgIds.replace("[CHROMOSOME]", s"$chr"))).asInput
    
      chr -> ids
    
    }.toMap
    
    val humanReference = chrsAll.map { chr =>
    
      val ref = store(checkPath(projectConfig.humanReferenceWild.replace("[CHROMOSOME]", s"$chr"))).asInput
    
      chr -> ref
    
    }.toMap
  
    val knownStores = projectConfig.Knowns.filter(e => projectConfig.Models.filter(e => e.knowns.isDefined).map(e => e.knowns.get).flatten.toSeq.distinct.contains(e.id)).map { known =>
  
      known -> Known(
        data = store(path(checkPath(known.data))).asInput,
        hiLd = MultiStore(
          local = Some(store(path(checkPath(known.hiLd))).asInput),
          google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / s"${known.id}." + s"${known.hiLd}".split("/").last)); case false => None }
        )
      )
  
    }.toMap
  
    ProjectStores(
      kgVcf = kgVcf,
      kgIds = kgIds,
      humanReference = humanReference,
      knownStores = knownStores,
      hailUtils = MultiStore(
        local = projectConfig.hailCloud match { case true => Some(store(path(checkPath(utils.python.pyHailUtils.toString()))).asInput); case false => None },
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.base.google.get / s"${utils.python.pyHailUtils}".split("/").last)); case false => None }
      ),
      regionsExclude = MultiStore(
        local = Some(store(path(checkPath(projectConfig.regionsExclude))).asInput),
        google = projectConfig.hailCloud match { case true => Some(store(projectConfig.cloudShare.get / s"${projectConfig.regionsExclude}".split("/").last)); case false => None }
      ),
      geneIdMap = MultiStore(
        local = Some(store(path(checkPath(projectConfig.geneIdMap))).asInput),
        google = projectConfig.hailCloud match { case true => Some(store(projectConfig.cloudShare.get / s"${projectConfig.geneIdMap}".split("/").last)); case false => None }
      ),
      kgPurcellVcf = MultiStore(
        local = Some(store(path(checkPath(projectConfig.kgPurcellVcf))).asInput),
        google = projectConfig.hailCloud match { case true => Some(store(projectConfig.cloudShare.get / s"${projectConfig.kgPurcellVcf}".split("/").last)); case false => None }
      ),
      kgSample = MultiStore(
        local = Some(store(path(checkPath(projectConfig.kgSample))).asInput),
        google = projectConfig.hailCloud match { case true => Some(store(projectConfig.cloudShare.get / s"${projectConfig.kgSample}".split("/").last)); case false => None }
      ),
      fasta = store(path(checkPath(projectConfig.fasta))).asInput,
      vepCacheDir = store(path(checkPath(projectConfig.vepCacheDir))).asInput,
      vepPluginsDir = store(path(checkPath(projectConfig.vepPluginsDir))).asInput,
      dbNSFP = store(path(checkPath(projectConfig.dbNSFP))).asInput,
      sampleFile = MultiStore(
        local = Some(store(path(projectConfig.sampleFile)).asInput),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / s"${projectConfig.sampleFile}".split("/").last)); case false => None }
      ),
      phenoFile = MultiStore(
        local = projectConfig.phenoFile match { case "" => None; case _ => Some(store(path(checkPath(projectConfig.phenoFile))).asInput) },
        google = projectConfig.phenoFile match { case "" => None; case _ => projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / s"${projectConfig.phenoFile}".split("/").last)); case false => None } }
      ),
      ancestryInferred = MultiStore(
        local = Some(store(dirTree.dataGlobalAncestry.local.get / s"${projectConfig.projectId}.ancestry.inferred.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobalAncestry.google.get / s"${projectConfig.projectId}.ancestry.inferred.tsv")); case false => None }
      ),
      ancestryOutliers = store(dirTree.dataGlobalAncestry.local.get / s"${projectConfig.projectId}.ancestry.inferred.outliers.tsv")
    )
  
  }

}
