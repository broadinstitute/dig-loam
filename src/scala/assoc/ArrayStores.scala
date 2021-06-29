object ArrayStores extends loamstream.LoamFile {

  import java.io.{File, BufferedWriter, FileWriter}
  import ProjectConfig._
  import StoreHelpers._
  import Stores._
  import Fxns._
  import DirTree._
  import Collections._
  
  final case class Array(
    cleanVcf: MultiPathVcf,
    cleanBgen: Option[MultiPathBgen],
    refMtOrig: MultiStore,
    refMt: MultiStore,
    refSitesVcf: Store,
    filteredPlink: MultiPathPlink,
    prunedPlink: Plink,
    phenoFile: MultiStore,
    sampleFile: Store,
    ancestryMap: Store,
    kin0: Store,
    sampleQcStats: Store,
    qcSamplesExclude: Store,
    postQcSamplesExclude: Store,
    variantsExcludeOrig: MultiStore,
    variantsExclude: MultiStore,
    refAnnotationsHtOrig: MultiStore,
    refAnnotationsHt: MultiStore)

  val arrayStores = projectConfig.Arrays.filter(e => usedArrays.contains(e.id)).map { arrayCfg =>

    val qcBaseDir = path(checkPath(arrayCfg.qc.baseDir))
    val qcCloudHome = arrayCfg.qcHailCloud match { case true => Some(uri(checkURI(arrayCfg.qcCloudHome.get))); case false => None }

    val cleanVcf = MultiPathVcf(
      base = MultiPath(
        local = Some(qcBaseDir / s"loam_out/data/array/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean"),
        google = None
      ),
      data = MultiStore(
        local = Some(store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean.vcf.bgz"))).asInput),
        google = None
      ),
      tbi = MultiStore(local = Some(store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean.vcf.bgz.tbi"))).asInput), google = None)
    )

    val cleanBgen = arrayCfg.exportCleanBgen match {
      case true =>
        Some(MultiPathBgen(
          base = MultiPath(
            local = Some(qcBaseDir / s"loam_out/data/array/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean"),
            google = None
          ),
          data = MultiStore(
            local = Some(store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean.bgen"))).asInput),
            google = None
          ),
          sample = MultiStore(
            local = Some(store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean.sample"))).asInput),
            google = None
          ),
          bgi = MultiStore(
            local = Some(store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/clean/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.clean.bgen.bgi"))).asInput),
            google = None
          )
        ))
      case _ => None
    }

    val refMtOrig = MultiStore(
      local = arrayCfg.qcHailCloud match {
        case true => None
        case false => Some(store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/harmonize/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.mt"))).asInput)
      },
      google = arrayCfg.qcHailCloud match {
        case true => Some(store(uri(checkURI(s"${qcCloudHome.get}/loam_out/data/array/${arrayCfg.qc.arrayId}/harmonize/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.mt"))).asInput)
        case false => None
      }
    )

    val refMt = MultiStore(
      local = projectConfig.hailCloud match {
        case true => None
        case false =>
          arrayCfg.qcHailCloud match {
            case true => Some(store(path(initDir(s"${dirTree.dataArrayMap(arrayCfg).harmonize.local.get}/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.mt")))) // copy refMtOrig.google.get to new dir refMt.local.get
            case false => Some(refMtOrig.local.get) // use original
          }
      },
      google = projectConfig.hailCloud match {
        case true =>
          arrayCfg.qcHailCloud match {
            case true => Some(refMtOrig.google.get) // use original
            case false => Some(store(uri(s"${projectConfig.cloudHome.get}/loam_out/data/array/${arrayCfg.id}/harmonize/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.mt"))) // copy refMtOrig.local.get to new google bucket refMt.google.get
          }
        case false => None
      }
    )

    val filteredPlink = MultiPathPlink(
      base = MultiPath(
        local = Some(qcBaseDir / s"loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered"),
        google = arrayCfg.qcHailCloud match { case true => Some(qcCloudHome.get / s"loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered"); case false => None }
      ),
      data = MultiSeqStore(
        local = Some(Seq(store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bed"))).asInput,store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bim"))).asInput,store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.fam"))).asInput)),
        google = arrayCfg.qcHailCloud match { case true => Some(Seq(store(uri(checkURI(s"${qcCloudHome.get}/loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bed"))).asInput,store(uri(checkURI(s"${qcCloudHome.get}/loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.bim"))).asInput,store(uri(checkURI(s"${qcCloudHome.get}/loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.fam"))).asInput)); case false => None }
      )
    )

    val prunedPlink = Plink(
      base = qcBaseDir / s"loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned",
      data = Seq(store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned.bed"))).asInput,store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned.bim"))).asInput,store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.filtered.pruned.fam"))).asInput)
    )

    val variantsExcludeOrig = MultiStore(
      local = Some(store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/filter/postqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.postqc.variants.exclude.txt"))).asInput),
      google = arrayCfg.qcHailCloud match {
        case true => Some(store(uri(checkURI(s"${qcCloudHome.get}/loam_out/data/array/${arrayCfg.qc.arrayId}/filter/postqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.postqc.variants.exclude.txt"))).asInput)
        case false => None
      }
    )

    val variantsExclude = MultiStore(
      local = Some(variantsExcludeOrig.local.get), // use original
      google = projectConfig.hailCloud match {
        case true =>
          arrayCfg.qcHailCloud match {
            case true => Some(variantsExcludeOrig.google.get) // use original
            case false => Some(store(uri(s"${projectConfig.cloudHome.get}/loam_out/data/array/${arrayCfg.id}/annotate/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.postqc.variants.exclude.txt"))) // copy variantsExcludeOrig.local.get to variantsExclude.google.get
          }
        case false => None
      }
    )

    val phenoFile = MultiStore(
      local = Some(store(path(checkPath(arrayCfg.phenoFile))).asInput), // use original
      google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / s"${arrayCfg.phenoFile}".split("/").last)); case false => None }
    )

    val refAnnotationsHtOrig = MultiStore(
      local = arrayCfg.qcHailCloud match {
        case true => None
        case false => Some(store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/annotate/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.annotations.ht"))).asInput)
      },
      google = arrayCfg.qcHailCloud match {
        case true => Some(store(uri(checkURI(s"${qcCloudHome.get}/loam_out/data/array/${arrayCfg.qc.arrayId}/annotate/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.annotations.ht"))).asInput)
        case false => None
      }
    )

    val refAnnotationsHt = MultiStore(
      local = projectConfig.hailCloud match {
        case true => None
        case false =>
          arrayCfg.qcHailCloud match {
            case true => Some(store(path(initDir(s"${dirTree.dataArrayMap(arrayCfg).annotate.local.get}/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.annotations.ht")))) // copy refAnnotationsHtOrig.google.get to new dir refAnnotationsHt.local.get
            case false => Some(refAnnotationsHtOrig.local.get) // use original
          }
      },
      google = projectConfig.hailCloud match {
        case true =>
          arrayCfg.qcHailCloud match {
            case true => Some(refAnnotationsHtOrig.google.get) // use original
            case false => Some(store(uri(s"${projectConfig.cloudHome.get}/loam_out/data/array/${arrayCfg.id}/annotate/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.annotations.ht"))) // copy refAnnotationsHtOrig.local.get to new google bucket refAnnotationsHt.google.get
          }
        case false => None
      }
    )

    arrayCfg -> Array(
      cleanVcf = cleanVcf,
      cleanBgen = cleanBgen,
      refMtOrig = refMtOrig,
      refMt = refMt,
      refSitesVcf = store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/annotate/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.ref.sites_only.vcf.bgz"))).asInput,
      filteredPlink = filteredPlink,
      prunedPlink = prunedPlink,
      phenoFile = phenoFile,
      sampleFile = store(path(checkPath(arrayCfg.qcSampleFile))).asInput,
      ancestryMap = store(path(checkPath(s"${qcBaseDir}/loam_out/data/global/ancestry/${arrayCfg.qcProjectId}.ancestry.inferred.tsv"))).asInput,
      kin0 = store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/kinship/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.kinship.kin0"))).asInput,
      sampleQcStats = store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/sampleqc/metrics/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.sampleqc.stats.tsv"))).asInput,
      qcSamplesExclude = store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/filter/qc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.qc.samples.exclude.txt"))).asInput,
      postQcSamplesExclude = store(path(checkPath(s"${qcBaseDir}/loam_out/data/array/${arrayCfg.qc.arrayId}/filter/postqc/${arrayCfg.qcProjectId}.${arrayCfg.qc.arrayId}.postqc.samples.exclude.txt"))).asInput,
      variantsExcludeOrig = variantsExcludeOrig,
      variantsExclude = variantsExclude,
      refAnnotationsHtOrig = refAnnotationsHtOrig,
      refAnnotationsHt = refAnnotationsHt
    )
  
  }.toMap

}
