object DirTree extends loamstream.LoamFile {

  import java.io.{File, BufferedWriter, FileWriter}
  import ProjectConfig._
  import Collections._
  import Stores._
  import Fxns._

  final case class DirTreeDataArray(
    base: MultiPath,
    harmonize: MultiPath,
    annotate: MultiPath) extends Debug
  
  final case class DirTree(
    base: MultiPath,
    data: MultiPath,
    dataArray: MultiPath,
    dataArrayMap: Map[ConfigArray, DirTreeDataArray],
    dataGlobal: MultiPath,
    dataGlobalKinship: MultiPath,
    dataGlobalKinshipMap: Map[ConfigMeta, MultiPath],
    analysis: MultiPath,
    analysisSchema: MultiPath,
    analysisSchemaMap: Map[ConfigSchema, MultiPath],
    analysisSchemaMaskMap: Map[ConfigSchema, Map[MaskFilter, MultiPath]],
    analysisModel: MultiPath,
    analysisModelMap: Map[ConfigModel, MultiPath],
    analysisModelBatchMap: Map[ConfigModel, Map[Int, MultiPath]],
    analysisModelBatchTestMap: Map[ConfigModel, Map[Int, Map[ConfigTest, MultiPath]]],
    analysisModelBatchTestMaskMap: Map[ConfigModel, Map[Int, Map[ConfigTest, Map[String, MultiPath]]]],
    analysisModelBatchTestChrMap: Map[ConfigModel, Map[Int, Map[ConfigTest, Map[String, MultiPath]]]],
    //analysisModelTestMaskMap: Map[ConfigModel, Map[ConfigTest, Map[MaskFilter, MultiPath]]],
    analysisModelBatchTestMaskChrMap: Map[ConfigModel, Map[Int, Map[ConfigTest, Map[String, Map[String, MultiPath]]]]],
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
      val dataArray = appendSubDir(data, "array")
      val dataGlobal = appendSubDir(data, "global")
      val dataGlobalKinship = appendSubDir(dataGlobal, "kinship")
      val analysis = appendSubDir(base, "analysis")
      val report = appendSubDir(base, "report")
      //val reportAnalysis = appendSubDir(report, "analysis")

      val dataArrayMap = cfg.Arrays.map { array =>
        val base = appendSubDir(dataArray, array.id)
        array -> DirTreeDataArray(
          base = base,
          harmonize = appendSubDir(base, "harmonize"),
          annotate = appendSubDir(base, "annotate")
        )
      }.toMap
  
      val dataGlobalKinshipMap = projectConfig.Metas.map { meta =>
        meta -> appendSubDir(dataGlobalKinship, meta.id)
      }.toMap
  
      val analysisSchema = appendSubDir(analysis, "schema")
      val analysisSchemaMap = projectConfig.Schemas.map { schema =>
        schema -> appendSubDir(analysisSchema, schema.id)
      }.toMap

      val analysisSchemaMaskMap = projectConfig.Schemas.filter(e => ! e.masks.isEmpty).distinct.map { schema =>
        schema -> 
          schema.masks.get.map { mask =>
            mask -> appendSubDir(analysisSchemaMap(schema), mask.id)
          }.toMap
      }.toMap

      val analysisModel = appendSubDir(analysis, "model")
      val analysisModelMap = projectConfig.Models.map { model =>
        model -> appendSubDir(analysisModel, model.id)
      }.toMap

      val analysisModelBatchMap = projectConfig.Models.filter(e => ! e.tests.isEmpty).map { model =>
        model ->
          model.batchList.map { i =>
            i -> appendSubDir(analysisModelMap(model), "batch_" + i)
          }.toMap
      }.toMap

      val analysisModelBatchTestMap = projectConfig.Models.filter(e => ! e.tests.isEmpty).map { model =>
        model ->
          model.batchList.map { i =>
            i -> 
              projectConfig.Tests.filter(e => model.tests.get.contains(e.id)).map { test =>
                test -> appendSubDir(analysisModelBatchMap(model)(i), test.id)
              }.toMap
          }.toMap
      }.toMap

      val analysisModelBatchTestChrMap = projectConfig.Models.filter(e => ! e.tests.isEmpty && e.splitChr).map { model =>
        model ->
          model.batchList.map { i =>
            i -> 
              projectConfig.Tests.filter(e => (model.tests.get.contains(e.id)) && (e.grouped == false)).map { test =>
                test -> 
                  expandChrList(projectConfig.Arrays.filter(e => e.id == projectConfig.Cohorts.filter(e => projectConfig.Schemas.filter(e => e.id == model.schema).head.cohorts.contains(e.id)).head.array).head.chrs).map { chr =>
                    chr -> appendSubDir(analysisModelBatchTestMap(model)(i)(test), "chr" + chr)
                  }.toMap
              }.toMap
          }.toMap
      }.toMap

      val analysisModelBatchTestMaskMap = projectConfig.Models.filter(e => ! e.tests.isEmpty).map { model =>
        model ->
          model.batchList.map { i =>
            i -> 
              projectConfig.Tests.filter(e => model.tests.get.contains(e.id) && (e.grouped)).map { test =>
                test -> 
                  projectConfig.Schemas.filter(e => (e.id == model.schema) && (! e.masks.isEmpty)).map(e => e.masks.get).flatten.distinct.map { mask =>
                    mask.id -> appendSubDir(analysisModelBatchTestMap(model)(i)(test), mask.id)
                  }.toMap
              }.toMap
          }.toMap
      }.toMap

      val analysisModelBatchTestMaskChrMap = projectConfig.Models.filter(e => ! e.tests.isEmpty && e.splitChr).map { model =>
        model ->
          model.batchList.map { i =>
            i -> 
              projectConfig.Tests.filter(e => model.tests.get.contains(e.id) && (e.grouped)).map { test =>
                test ->
                  projectConfig.Schemas.filter(e => (e.id == model.schema) && (! e.masks.isEmpty)).map(e => e.masks.get).flatten.distinct.map { mask =>
                    mask.id ->
                      expandChrList(projectConfig.Arrays.filter(e => e.id == projectConfig.Cohorts.filter(e => projectConfig.Schemas.filter(e => e.id == model.schema).head.cohorts.contains(e.id)).head.array).head.chrs).map { chr =>
                        chr -> appendSubDir(analysisModelBatchTestMaskMap(model)(i)(test)(mask.id), "chr" + chr)
                      }.toMap
                  }.toMap
              }.toMap
          }.toMap
      }.toMap

      //val analysisModelTestMaskMap = projectConfig.Models.filter(e => ! e.tests.isEmpty).map { model =>
      //  model ->
      //    projectConfig.Tests.filter(e => (model.tests.get.contains(e.id)) && (e.grouped)).map { test =>
      //      test ->
      //        projectConfig.Schemas.filter(e => (e.id == model.schema) && (! e.masks.isEmpty)).map(e => e.masks.get).flatten.distinct.map { mask =>
      //          mask -> appendSubDir(analysisModelTestMap(model)(test), mask.id)
      //        }.toMap
      //    }.toMap
      //}.toMap

      //val reportAnalysisMap = cfg.Reports.map { report =>
      //  report -> appendSubDir(reportAnalysis, report.id)
      //}.toMap
  
      new DirTree(
        base = base,
        data = data,
        dataArray = dataArray,
        dataArrayMap = dataArrayMap,
        dataGlobal = dataGlobal,
        dataGlobalKinship = dataGlobalKinship,
        dataGlobalKinshipMap = dataGlobalKinshipMap,
        analysis = analysis,
        analysisSchema = analysisSchema,
        analysisSchemaMap = analysisSchemaMap,
        analysisSchemaMaskMap = analysisSchemaMaskMap,
        analysisModel = analysisModel,
        analysisModelMap = analysisModelMap,
        analysisModelBatchMap = analysisModelBatchMap,
        analysisModelBatchTestMap = analysisModelBatchTestMap,
        analysisModelBatchTestMaskMap = analysisModelBatchTestMaskMap,
        analysisModelBatchTestChrMap = analysisModelBatchTestChrMap,
        analysisModelBatchTestMaskChrMap = analysisModelBatchTestMaskChrMap,
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
