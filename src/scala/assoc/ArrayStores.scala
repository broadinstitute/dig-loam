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
    refSitesVcf: MultiStore,
    filteredPlink: MultiPathPlink,
    prunedPlink: MultiPathPlink,
    phenoFile: MultiStore,
    sampleFile: MultiStore,
    ancestryMap: MultiStore,
    kin0: Store,
    sampleQcStats: MultiStore,
    qcSamplesExclude: MultiStore,
    postQcSamplesExclude: MultiStore,
    variantsExclude: MultiStore,
    refAnnotationsHt: MultiStore)
  
  val arrayStores = projectConfig.Arrays.map { arrayCfg =>

    val cleanVcf = arrayCfg.exportCleanVcf match {
      case true =>
        MultiPathVcf(
          base = MultiPath(
            local = Some(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean"),
            google = arrayCfg.qcHailCloud match { case true => Some(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean"); case false => None }
          ),
          data = MultiStore(
            local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean.vcf.bgz")).asInput),
            google = arrayCfg.qcHailCloud match { case true => Some(store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean.vcf.bgz")).asInput); case false => None }
          ),
          tbi = MultiStore(local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean.vcf.bgz.tbi")).asInput), google = None)
        )
      case _ => None
    }

    val refMt = MultiStore(
      local = arrayCfg.qcHailCloud match { case false => Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/harmonize/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.mt")).asInput); case true => None },
      google = arrayCfg.qcHailCloud match { case true => Some(store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/harmonize/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.mt")).asInput); case false => None }
    )

    val refSitesVcf = MultiStore(
      local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/annotate/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.sites_only.vcf.bgz"))),
      google = arrayCfg.qcHailCloud match { case true => Some(store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/annotate/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.sites_only.vcf.bgz"))); case false => None }
    )

    val filteredPlink = MultiPathPlink(
      base = MultiPath(
        local = Some(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered"),
        google = arrayCfg.qcHailCloud match { case true => Some(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered"); case false => None }
      ),
      data = MultiSeqStore(
        local = Some(Seq(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bed")).asInput,store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bim")).asInput,store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.fam")).asInput)),
        google = arrayCfg.qcHailCloud match { case true => Some(Seq(store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bed")).asInput,store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bim")).asInput,store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.fam")).asInput)); case false => None }
      )
    )

    val prunedPlink = MultiPathPlink(
      base = MultiPath(
        local = Some(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned"),
        google = arrayCfg.qcHailCloud match { case true => Some(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned"); case false => None }
      ),
      data = MultiSeqStore(
        local = Some(Seq(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned.bed")).asInput,store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned.bim")).asInput,store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned.fam")).asInput)),
        google = arrayCfg.qcHailCloud match { case true => Some(Seq(store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned.bed")).asInput,store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned.bim")).asInput,store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned.fam")).asInput)); case false => None }
      )
    )

    val phenoFile = MultiStore(
      local = Some(store(checkPath(arrayCfg.qcPhenoFile)).asInput),
      google = arrayCfg.qcHailCloud match { case true => Some(store(checkUri(arrayCfg.qcCloudHome / s"data/global/${arrayCfg.qcPhenoFile}".split("/").last)).asInput); case false => None }
    )

    val sampleFile = MultiStore(
      local = Some(store(checkPath(arrayCfg.qcSampleFile)).asInput),
      google = arrayCfg.qcHailCloud match { case true => Some(store(checkUri(arrayCfg.qcCloudHome / s"data/global/${arrayCfg.qcSampleFile}".split("/").last)).asInput); case false => None }
    )

    val ancestryMap = MultiStore(
      local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/global/ancestry/${arrayCfg.qcProjectId}.ancestry.inferred.tsv")).asInput),
      google = arrayCfg.qcHailCloud match { case true => Some(store(checkUri(arrayCfg.qcCloudHome / s"data/global/ancestry/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ancestry.inferred.tsv".split("/").last)).asInput); case false => None }
    )

    val sampleQcStats = MultiStore(
      local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/metrics/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.sampleqc.stats.tsv")).asInput),
      google = arrayCfg.qcHailCloud match { case true => Some(store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/sampleqc/metrics/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.sampleqc.stats.tsv")).asInput); case false => None }
    )

    val qcSamplesExclude = MultiStore(
      local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/filter/qc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.qc.samples.exclude.txt")).asInput),
      google = arrayCfg.qcHailCloud match { case true => Some(store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/filter/qc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.qc.samples.exclude.txt")).asInput); case false => None }
    )

    val postQcSamplesExclude = MultiStore(
      local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/filter/postqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.postqc.samples.exclude.txt")).asInput),
      google = arrayCfg.qcHailCloud match { case true => Some(store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/filter/postqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.postqc.samples.exclude.txt")).asInput); case false => None }
    )

    val variantsExclude = MultiStore(
      local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/filter/postqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.postqc.variants.exclude.txt")).asInput),
      google = arrayCfg.qcHailCloud match { case true => Some(store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/filter/postqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.postqc.variants.exclude.txt")).asInput); case false => None }
    )

    val refAnnotationsHt = MultiStore(
      local = Some(store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/annotate/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.annotations.ht")).asInput),
      google = arrayCfg.qcHailCloud match { case true => Some(store(checkUri(arrayCfg.qcCloudHome / s"data/${arrayCfg.qc.arrayId}/annotate/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.annotations.ht"))); case false => None }
    )

    arrayCfg -> Array(
      cleanVcf = cleanVcf,
      refMt = refMt,
      refSitesVcf = refSitesVcf,
      filteredPlink = filteredPlink,
      prunedPlink = prunedPlink,
      phenoFile = phenoFile,
      sampleFile = sampleFile,
      ancestryMap = ancestryMap,
      kin0 = store(checkPath(arrayCfg.qc.baseDir / s"data/${arrayCfg.qc.arrayId}/sampleqc/kinship/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.kinship.kin0")).asInput,
      sampleQcStats = sampleQcStats,
      qcSamplesExclude = qcSamplesExclude,
      postQcSamplesExclude = postQcSamplesExclude,
      variantsExclude = variantsExclude,
      refAnnotationsHt = refAnnotationsHt
    )
  
  }.toMap

}
