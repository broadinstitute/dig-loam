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
    knownStores: Map[ConfigKnown, Known],
    hailUtils: MultiStore,
    geneIdMap: MultiStore,
    fasta: Store,
    vepCacheDir: Store,
    vepPluginsDir: Store,
    dbNSFP: Store)
  
  val projectStores = {
  
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
      knownStores = knownStores,
      hailUtils = MultiStore(
        local = projectConfig.hailCloud match { case true => Some(store(path(checkPath(utils.python.pyHailUtils.toString()))).asInput); case false => None },
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.base.google.get / s"${utils.python.pyHailUtils}".split("/").last)); case false => None }
      ),
      geneIdMap = MultiStore(
        local = Some(store(path(checkPath(projectConfig.geneIdMap))).asInput),
        google = projectConfig.hailCloud match { case true => Some(store(projectConfig.cloudShare.get / s"${projectConfig.geneIdMap}".split("/").last)); case false => None }
      ),
      fasta = store(path(checkPath(projectConfig.fasta))).asInput,
      vepCacheDir = store(path(checkPath(projectConfig.vepCacheDir))).asInput,
      vepPluginsDir = store(path(checkPath(projectConfig.vepPluginsDir))).asInput,
      dbNSFP = store(path(checkPath(projectConfig.dbNSFP))).asInput
    )
  
  }

}
