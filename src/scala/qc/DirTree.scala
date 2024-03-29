object DirTree extends loamstream.LoamFile {

  import java.io.{File, BufferedWriter, FileWriter}
  import ProjectConfig._
  import Collections._
  import Stores._
  import Fxns._
  
  final case class DirTreeDataArray(
    base: MultiPath,
    prepare: MultiPath,
    harmonize: MultiPath,
    annotate: MultiPath,
    impute: MultiPath,
    sampleqc: MultiPath,
    kinship: MultiPath,
    ancestry: MultiPath,
    gmm: MultiPath,
    knn: MultiPath,
    pca: MultiPath,
    sexcheck: MultiPath,
    metrics: MultiPath,
    filter: MultiPath,
    filterQc: MultiPath,
    filterPostQc: MultiPath,
    results: MultiPath) extends Debug
  
  final case class DirTree(
    base: MultiPath,
    data: MultiPath,
    dataArray: MultiPath,
    dataArrayMap: Map[ConfigArray, DirTreeDataArray],
    dataGlobal: MultiPath,
    dataGlobalAncestry: MultiPath,
    dataGlobalGmm: MultiPath,
    dataGlobalKnn: MultiPath,
    report: MultiPath,
    reportQc: MultiPath
    ) extends Debug
  
  def appendSubDir(msp: MultiPath, name: String): MultiPath = {
    MultiPath(
      local = Some(path(initDir(s"${msp.local.get}" + "/" + name))),
      google = projectConfig.hailCloud match {
        case true => Some(msp.google.get / name)
        case false => None
      }
    )
  }
  
  object DirTree {
  
    def initDirTree(cfg: ProjectConfig): DirTree = {
  
      val base = MultiPath(
        local = Some(path(initDir("loam_out"))),
        google = cfg.hailCloud match {
          case true => Some(cfg.cloudHome.get / "loam_out")
          case false => None
        }
      )
  
      val data = appendSubDir(base, "data")
      val dataArray = appendSubDir(data, "array")
      val dataGlobal = appendSubDir(data, "global")
      val dataGlobalAncestry = appendSubDir(dataGlobal, "ancestry")
      val dataGlobalGmm = appendSubDir(dataGlobalAncestry, "gmm")
      val dataGlobalKnn = appendSubDir(dataGlobalAncestry, "knn")
      val report = appendSubDir(base, "report")
      val reportQc = appendSubDir(report, "qc")
  
      val dataArrayMap = cfg.Arrays.map { array =>
        val base = appendSubDir(dataArray, array.id)
        val sampleqc = appendSubDir(base, "sampleqc")
        val ancestry = appendSubDir(sampleqc, "ancestry")
        val filter = appendSubDir(base, "filter")
        array -> DirTreeDataArray(
          base = base,
          prepare = appendSubDir(base, "prepare"),
          harmonize = appendSubDir(base, "harmonize"),
          annotate = appendSubDir(base, "annotate"),
          impute = appendSubDir(base, "impute"),
          sampleqc = sampleqc,
          kinship = appendSubDir(sampleqc, "kinship"),
          ancestry = ancestry,
          gmm = appendSubDir(ancestry, "gmm"),
          knn = appendSubDir(ancestry, "knn"),
          pca = appendSubDir(sampleqc, "pca"),
          sexcheck = appendSubDir(sampleqc, "sexcheck"),
          metrics = appendSubDir(sampleqc, "metrics"),
          filter = filter,
          filterQc = appendSubDir(filter, "qc"),
          filterPostQc = appendSubDir(filter, "postqc"),
          results = appendSubDir(base, "results")
        )
      }.toMap
  
      new DirTree(
        base = base,
        data = data,
        dataArray = dataArray,
        dataArrayMap = dataArrayMap,
        dataGlobal = dataGlobal,
        dataGlobalAncestry = dataGlobalAncestry,
        dataGlobalGmm = dataGlobalGmm,
        dataGlobalKnn = dataGlobalKnn,
        report = report,
        reportQc = reportQc
      )
  
    }
  
  }
  
  println("Initializing Directory Tree ...")
  val dirTree = DirTree.initDirTree(projectConfig)
  println("... Finished Initializing Directory Tree")

}
