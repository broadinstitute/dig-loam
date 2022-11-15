object ArrayStores extends loamstream.LoamFile {

  import java.io.{File, BufferedWriter, FileWriter}
  import ProjectConfig._
  import StoreHelpers._
  import Stores._
  import Fxns._
  import DirTree._
  import Collections._
  
  final case class Array(
    vcf: Option[MultiPathVcf],
    bgen: Option[MultiPathBgen],
    mt: Option[MultiStore],
    refSitesVcf: Store,
    filteredPlink: Plink,
    prunedPlink: Plink,
    phenoFile: MultiStore,
    ancestryMap: Store,
    kin0: Store,
    sampleQcStats: Store,
    samplesExclude: MultiStore,
    variantsExclude: MultiStore,
    annotationsHt: MultiStore)

  val arrayStores = projectConfig.Arrays.filter(e => usedArrays.contains(e.id)).map { arrayCfg =>

    val vcf = arrayCfg.vcf match {
      case Some(_) =>
        Some(MultiPathVcf(
          base = MultiPath(local = Some(path(arrayCfg.vcf.get.replaceAll(".vcf.gz$|.vcf.bgz$|.gz$|.bgz$",""))), google = None),
          data = MultiStore(local = Some(store(checkPath(arrayCfg.vcf.get)).asInput), google = None),
          tbi = MultiStore(local = Some(store(checkPath(arrayCfg.vcf.get + s".tbi")).asInput), google = None)
        ))
      case None => None
    }

    val bgen = arrayCfg.bgen match {
      case Some(_) => 
        Some(MultiPathBgen(
          base = MultiPath(local = Some(path(arrayCfg.bgen.get.replaceAll(".bgen$",""))), google = None),
          data = MultiStore(local = Some(store(checkPath(arrayCfg.bgen.get)).asInput), google = None),
          sample = MultiStore(local = Some(store(checkPath(arrayCfg.bgen.get.replaceAll(".bgen$",".sample"))).asInput), google = None),
          bgi = MultiStore(local = Some(store(checkPath(arrayCfg.bgen.get + s".bgi")).asInput), google = None)
        ))
      case None => None
    }

    val mt = arrayCfg.mt match {
      case Some(_) =>
        Some(MultiStore(
          local = Some(store(checkPath(arrayCfg.mt.get)).asInput),
          google = projectConfig.hailCloud match {
            case true => Some(store(dirTree.dataGlobal.google.get / s"${arrayCfg.mt.get}".split("/").last))
            case false => None
          }
        ))
      case None => None
    }

    val filteredPlink = Plink(
      base = path(arrayCfg.filteredPlink),
      data = Seq(store(path(checkPath(s"${arrayCfg.filteredPlink}.bed"))).asInput,store(path(checkPath(s"${arrayCfg.filteredPlink}.bim"))).asInput,store(path(checkPath(s"${arrayCfg.filteredPlink}.fam"))).asInput)
    )

    val prunedPlink = Plink(
      base = path(arrayCfg.prunedPlink),
      data = Seq(store(path(checkPath(s"${arrayCfg.prunedPlink}.bed"))).asInput,store(path(checkPath(s"${arrayCfg.prunedPlink}.bim"))).asInput,store(path(checkPath(s"${arrayCfg.prunedPlink}.fam"))).asInput)
    )

    val variantsExclude = MultiStore(
      local = Some(store(path(checkPath(arrayCfg.variantsExclude))).asInput),
      google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / s"${arrayCfg.variantsExclude}".split("/").last)); case false => None }
    )

    val samplesExclude = MultiStore(
      local = Some(store(path(checkPath(arrayCfg.samplesExclude))).asInput),
      google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / s"${arrayCfg.samplesExclude}".split("/").last)); case false => None }
    )

    val phenoFile = MultiStore(
      local = Some(store(path(checkPath(arrayCfg.phenoFile))).asInput), // use original
      google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / s"${arrayCfg.phenoFile}".split("/").last)); case false => None }
    )

    val annotationsHt = MultiStore(
      local = Some(store(checkPath(arrayCfg.annotationsHt)).asInput),
      google = projectConfig.hailCloud match {
        case true => Some(store(dirTree.dataGlobal.google.get / s"${arrayCfg.annotationsHt}".split("/").last))
        case false => None
      }
    )

    arrayCfg -> Array(
      vcf = vcf,
      bgen = bgen,
      mt = mt,
      refSitesVcf = store(path(checkPath(arrayCfg.refSitesVcf))).asInput,
      filteredPlink = filteredPlink,
      prunedPlink = prunedPlink,
      phenoFile = phenoFile,
      ancestryMap = store(path(checkPath(arrayCfg.ancestryMap))).asInput,
      kin0 = store(path(checkPath(arrayCfg.kin0))).asInput,
      sampleQcStats = store(path(checkPath(arrayCfg.sampleQcStats))).asInput,
      samplesExclude = samplesExclude,
      variantsExclude = variantsExclude,
      annotationsHt = annotationsHt
    )
  
  }.toMap

}
