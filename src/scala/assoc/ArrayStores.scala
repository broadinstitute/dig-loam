object ArrayStores extends loamstream.LoamFile {

  import java.io.{File, BufferedWriter, FileWriter}
  import ProjectConfig._
  import StoreHelpers._
  import Stores._
  import Fxns._
  import DirTree._
  
  final case class Array(
    cleanVcf: Option[MultiPathVcf],
    refMt: MultiStore,
    filteredPlink: MultiPathPlink,
    phenoFile: Store,
    sampleFile: Store,
    ancestryMap: Store,
    kin0: Store,
    sampleQcStats: MultiStore,
    qcSamplesExclude: MultiStore,
    postQcSamplesExclude: MultiStore,
    variantsExclude: MultiStore)
  
  val arrayStores = projectConfig.Arrays.map { arrayCfg =>

    val qcConfig = loadConfig(checkPath(arrayCfg.qc.config))

    val qcProjectId = requiredStr(config = qcConfig, field = "projectId")
    val qcCloudHome = optionalStr(config = qcConfig, field = "cloudHome") match { case Some(s) => Some(uri(s)); case None => None }
    val hailCloud = requiredBool(config = qcConfig, field = "hailCloud")

    val cleanVcf = requiredBool(config = qcConfig, field = "exportCleanVcf") match {
      case true =>
        MultiPathVcf(
          base = MultiPath(
            local = Some(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/clean/${qcProjectId}.${arrayCfg.qc.arrayId}.clean"),
            google = projectConfig.hailCloud match { case true => Some(qcCloudHome / s"data/${arrayCfg.qc.arrayId}/clean/${qcProjectId}.${arrayCfg.qc.arrayId}.clean"); case false => None }
          ),
          data = MultiStore(
            local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/clean/${qcProjectId}.${arrayCfg.qc.arrayId}.clean.vcf.bgz")).asInput),
            google = projectConfig.hailCloud match { case true => Some(store(checkUri(qcCloudHome / s"data/${arrayCfg.qc.arrayId}/clean/${qcProjectId}.${arrayCfg.qc.arrayId}.clean.vcf.bgz")).asInput); case false => None }
          ),
          tbi = MultiStore(local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/clean/${qcProjectId}.${arrayCfg.qc.arrayId}.clean.vcf.bgz.tbi")).asInput), google = None)
        )
      case _ => None
    }

    val refMt = MultiStore(
      local = hailCloud match { case false => Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/harmonize/${qcProjectId}.${arrayCfg.qc.arrayId}.ref.mt")).asInput); case true => None },
      google = hailCloud match { case true => Some(store(checkUri(qcCloudHome / s"data/${arrayCfg.qc.arrayId}/harmonize/${qcProjectId}.${arrayCfg.qc.arrayId}.ref.mt")).asInput); case false => None }
    )

    val filteredPlink = MultiPathPlink(
      base = MultiPath(
        local = Some(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered"),
        google = projectConfig.hailCloud match { case true => Some(qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered"); case false => None }
      ),
      data = MultiSeqStore(
        local = Some(Seq(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bed")).asInput,store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bim")).asInput,store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.fam")).asInput)),
        google = projectConfig.hailCloud match { case true => Some(Seq(store(checkUri(qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bed")).asInput,store(checkUri(qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bim")).asInput,store(checkUri(qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.fam")).asInput)); case false => None }
      )
    )

    val sampleQcStats = MultiStore(
      local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/metrics/${qcProjectId}.${arrayCfg.qc.arrayId}.sampleqc.stats.tsv")).asInput),
      google = projectConfig.hailCloud match { case true => Some(store(checkUri(qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/metrics/${qcProjectId}.${arrayCfg.qc.arrayId}.sampleqc.stats.tsv")).asInput); case false => None }
    )

    val qcSamplesExclude = MultiStore(
      local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/filter/qc/${qcProjectId}.${arrayCfg.qc.arrayId}.qc.samples.exclude.txt")).asInput),
      google = projectConfig.hailCloud match { case true => Some(store(checkUri(qcCloudHome / s"data/${arrayCfg.qc.arrayId}/filter/qc/${qcProjectId}.${arrayCfg.qc.arrayId}.qc.samples.exclude.txt")).asInput); case false => None }
    )

    val postQcSamplesExclude = MultiStore(
      local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/filter/postqc/${qcProjectId}.${arrayCfg.qc.arrayId}.postqc.samples.exclude.txt")).asInput),
      google = projectConfig.hailCloud match { case true => Some(store(checkUri(qcCloudHome / s"data/${arrayCfg.qc.arrayId}/filter/postqc/${qcProjectId}.${arrayCfg.qc.arrayId}.postqc.samples.exclude.txt")).asInput); case false => None }
    )

    val variantsExclude = MultiStore(
      local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/filter/postqc/${qcProjectId}.${arrayCfg.qc.arrayId}.postqc.variants.exclude.txt")).asInput),
      google = projectConfig.hailCloud match { case true => Some(store(checkUri(qcCloudHome / s"data/${arrayCfg.qc.arrayId}/filter/postqc/${qcProjectId}.${arrayCfg.qc.arrayId}.postqc.variants.exclude.txt")).asInput); case false => None }
    )

    arrayCfg -> Array(
      cleanVcf = cleanVcf,
      refMt = refMt,
      filteredPlink = filteredPlink,
      FIGURE OUT HOW TO GET FROM qc.config!!! phenoFile = store(checkPath(arrayCfg.phenoFile)).asInput,
      FIGURE OUT HOW TO GET FROM qc.config!!! sampleFile = store(checkPath(arrayCfg.sampleFile)).asInput,
      FIGURE OUT HOW TO GET FROM qc.config!!! ancestryMap = store(checkPath(arrayCfg.ancestryMap)).asInput,
      FIGURE OUT HOW TO GET FROM qc.config!!! kin0 = store(checkPath(arrayCfg.kin0)).asInput,
      sampleQcStats = sampleQcStats,
      qcSamplesExclude = qcSamplesExclude,
      postQcSamplesExclude = postQcSamplesExclude,
      variantsExclude = variantsExclude
    )
  
  }.toMap

}
