object DirTree extends loamstream.LoamFile {

  import java.io.{File, BufferedWriter, FileWriter}
  import ProjectConfig._
  import Collections._
  import Stores._
  import Fxns._
  
  final case class DirTree(
    base: MultiPath,
    data: MultiPath,
    dataGlobal: MultiPath,
    dataGlobalKinship: MultiPath,
    dataGlobalKinshipMap: Map[ConfigMeta, MultiPath],
    analysis: MultiPath,
    analysisSchema: MultiPath,
    analysisSchemaMap: Map[ConfigSchema, MultiPath],
    analysisModel: MultiPath,
    analysisModelGroups: MultiPath,
    analysisModelGroupsMap: scala.collection.mutable.Map[String, MultiPath],
    analysisModelRegions: MultiPath,
    analysisModelRegionsMap: scala.collection.mutable.Map[String, MultiPath],
    analysisModelMap: Map[ConfigModel, MultiPath],
    report: MultiPath,
    //reportAnalysis: MultiPath,
    //reportAnalysisMap: Map[ConfigReport, MultiPath]
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
      val dataGlobal = appendSubDir(data, "global")
      val dataGlobalKinship = appendSubDir(dataGlobal, "kinship")
      val analysis = appendSubDir(base, "analysis")
      val report = appendSubDir(base, "report")
      //val reportAnalysis = appendSubDir(report, "analysis")
  
      val dataGlobalKinshipMap = projectConfig.Metas.map { meta =>
        meta -> appendSubDir(dataGlobalKinship, meta.id)
      }.toMap
  
      val analysisSchema = appendSubDir(analysis, "schema")
      val analysisSchemaMap = projectConfig.Schemas.map { schema =>
        schema -> appendSubDir(analysisSchema, schema.id)
      }.toMap
      val analysisModel = appendSubDir(analysis, "model")
      val analysisModelGroups = appendSubDir(analysisModel, "groups")
      val analysisModelRegions = appendSubDir(analysisModel, "regions")
      val analysisModelRegionsMap = scala.collection.mutable.Map[String, MultiPath]()
      val analysisModelMap = projectConfig.Models.map { model =>
        model -> appendSubDir(analysisModel, model.id)
      }.toMap
      
      //val reportAnalysisMap = cfg.Reports.map { report =>
      //  report -> appendSubDir(reportAnalysis, report.id)
      //}.toMap
  
      new DirTree(
        base = base,
        data = data,
        dataGlobal = dataGlobal,
        dataGlobalKinship = dataGlobalKinship,
        dataGlobalKinshipMap = dataGlobalKinshipMap,
        analysis = analysis,
        analysisSchema = analysisSchema,
        analysisSchemaMap = analysisSchemaMap,
        analysisModel = analysisModel,
        analysisModelGroups = analysisModelGroups,
        analysisModelGroupsMap = scala.collection.mutable.Map[String, MultiPath](),
        analysisModelRegions = analysisModelRegions,
        analysisModelRegionsMap = scala.collection.mutable.Map[String, MultiPath](),
        analysisModelMap = analysisModelMap,
        report = report
        //reportAnalysis = reportAnalysis,
        //reportAnalysisMap = reportAnalysisMap
      )
  
    }
  
  }
  
  println("Initializing Directory Tree ...")
  val dirTree = DirTree.initDirTree(projectConfig)
  println("... Finished Initializing Directory Tree")

}
