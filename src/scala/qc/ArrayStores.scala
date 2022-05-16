object ArrayStores extends loamstream.LoamFile {

  import java.io.{File, BufferedWriter, FileWriter}
  import ProjectConfig._
  import StoreHelpers._
  import Stores._
  import Fxns._
  import DirTree._
  
  final case class RawData(
    plink: Option[Plink],
    vcf: Option[MultiPathVcf],
    rawBase: Option[Path],
    unplaced: Option[Store],
    unique: Option[Store],
    indel: Option[Store],
    lmiss: Option[Store],
    imiss: Option[Store],
    imissRemove: Option[Store],
    freq: Option[Store],
    mono: Option[Store],
    uids: Option[Store],
    possDupVars: Option[Store],
    possDupPlink: Option[Plink],
    possDupBase: Option[Path],
    possDupFreq: Option[Store],
    possDupLmiss: Option[Store],
    dupVarsRemove: Option[Store])
  
  final case class PreparedData(
    plink: Plink,
    multiallelic: Store,
    chain: Option[Store] = None,
    bed: Option[Store] = None,
    lifted: Option[Store] = None,
    unlifted: Option[Store] = None,
    liftedUpdate: Option[Store] = None,
    liftedExtract: Option[Store] = None)
  
  final case class AnnotatedData(
    plink: Plink,
    snplist: Store)
  
  final case class AnnotatedChrData(
    snpsPlink: Plink,
    otherPlink: Option[Plink],
    mergedKgPlink: Plink,
    mergedKgHuRefPlink: Plink,
    otherHuRefPlink: Option[Plink],
    refPlink: Plink,
    mergedKgNonKgBase: Path,
    otherNonKgBase: Option[Path],
    nonKgRemove: Store,
    nonKgIgnore: Store,
    nonKgMono: Store,
    nonKgNomatch: Store,
    nonKgFlip: Store,
    nonKgForceA1: Store,
    otherRemove: Option[Store],
    otherIgnore: Option[Store],
    otherMono: Option[Store],
    otherNomatch: Option[Store],
    otherFlip: Option[Store],
    otherForceA1: Option[Store],
    mergedKgVarIdUpdate: Store,
    mergedKgVarSnpLog: Store,
    forceA2: Store,
    harmonizedVcf: MultiPathVcf,
    harmonizedBim: Store,
    harmonizedFam: Store)
  
  //final case class HarmonizedData(
  //  plink: Plink,
  //  mergeList: Store,
  //  nonKgRemove: Store,
  //  nonKgIgnore: Store,
  //  nonKgMono: Store,
  //  nonKgNomatch: Store,
  //  nonKgFlip: Store,
  //  nonKgForceA1: Store,
  //  mergedKgVarIdUpdate: Store,
  //  mergedKgVarSnpLog: Store,
  //  forceA2: Store)
  
  final case class RefData(
    //plink: Option[MultiPathPlink],
    //vcf: Option[MultiPathVcf],
    vcf: Seq[MultiPathVcf],
    bim: Seq[Store],
    fam: Seq[Store],
    vcfGlob: MultiPath,
    mtCheckpoint: MultiStore,
    mt: MultiStore,
    hailLog: MultiStore,
    variantMetrics: MultiStore,
    sitesVcf: MultiStore,
    sitesVcfTbi: Store,
    annotations: MultiStore,
    annotationsHt: MultiStore,
    annotationsHailLog: MultiStore,
    annotationWarnings: Store,
    annotationHeader: Store)
  
  final case class ImputeData(
    data: Seq[Store],
    base: Path)
  
  final case class FilteredData(
    variantFilters: MultiStore,
    variantMetrics: MultiStore,
    plink: MultiPathPlink,
    hailLog: MultiStore,
    pruneIn: Store)
  
  final case class PrunedData(
    plink: Plink,
    bimGoogle: Option[Store])
  
  final case class KinshipData(
    base: Path,
    log: Store,
    kin0: Store,
    famSizes: Store)
  
  final case class Ref1kgData(
    plink: MultiPathPlink,
    hailLog: MultiStore,
    kgSamples: MultiStore)
  
  final case class AncestryData(
    inferred: Store)
  
  final case class AncestryPcaData(
    base: Path,
    log: Store,
    scores: Store,
    eigenVecs: Store,
    loadings: Store,
    eigenVals: Store,
    pve: Store,
    meansd: Store,
    plots: Store,
    plotsPc1Pc2Png: Store,
    plotsPc2Pc3Png: Store)
  
  final case class AncestryClusterData(
    base: Path,
    log: Store,
    fet: Store,
    clu: Store,
    klg: Store,
    plots: Store,
    plotsPc1Pc2Png: Store,
    plotsPc2Pc3Png: Store,
    centerPlots: Store,
    no1kgPlots: Store,
    xtab: Store,
    groups: Store)
  
  final case class PcaData(
    log: Store,
    scores: Store,
    eigenVecs: Store,
    loadings: Store,
    eigenVals: Store,
    pve: Store,
    meansd: Store)
  
  final case class SexcheckData(
    sexcheck: MultiStore,
    problems: MultiStore)
  
  final case class SampleQcData(
    stats: MultiStore,
    hailLog: MultiStore,
    statsAdj: Store,
    corrPlots: Store,
    boxPlots: Store,
    discreteness: Store,
    pcaLoadings: Store,
    pcaPlots: Store,
    pcaScores: Store,
    outliers: Store,
    incompleteObs: Store,
    metricPlots: Store,
    metricPlotsPng: Store)
  
  final case class SampleQcPcaClusterData(
    base: Path,
    fet: Store,
    clu: Store,
    klg: Store,
    log: Store,
    outliers: Store,
    plots: Store,
    xtab: Store)
  
  final case class SampleQcMetricClusterData(
    base: Path,
    fet: Store,
    clu: Store,
    klg: Store,
    log: Store)
  
  final case class FilterQc(
    samplesExclude: MultiStore,
    samplesRestore: Store)
  
  final case class FilterPostQc(
    samplesStats: MultiStore,
    samplesExclude: MultiStore,
    variantsStats: MultiStore,
    vFilters: MultiStore,
    sFilters: MultiStore,
    variantsExclude: MultiStore,
    hailLog: MultiStore)
  
  final case class CleanVcf(
    vcf: MultiPathVcf,
    hailLog: MultiStore)
  
  final case class Array(
    rawData: RawData,
    preparedData: Option[PreparedData],
    annotatedData: Option[AnnotatedData],
    annotatedChrData: Option[Map[String, AnnotatedChrData]],
    //harmonizedData: Option[HarmonizedData],
    refData: RefData,
    imputeData: ImputeData,
    filteredData: FilteredData,
    prunedData: PrunedData,
    kinshipData: KinshipData,
    ref1kgData: Ref1kgData,
    ancestryData: AncestryData,
    ancestryPcaData: AncestryPcaData,
    ancestryClusterData: AncestryClusterData,
    pcaData: PcaData,
    sexcheckData: SexcheckData,
    sampleQcData: SampleQcData,
    sampleQcPcaClusterData: SampleQcPcaClusterData,
    sampleQcMetricClusterData: Map[String, SampleQcMetricClusterData],
    filterQc: FilterQc,
    filterPostQc: FilterPostQc,
    ancestryOutliersKeep: Option[Store],
    duplicatesKeep: Option[Store],
    famsizeKeep: Option[Store],
    sampleqcKeep: Option[Store],
    sexcheckKeep: Option[Store],
    cleanVcf: CleanVcf,
    cleanBgen: Option[MultiPathBgen])
  
  val arrayStores = projectConfig.Arrays.map { arrayCfg =>
  
    val rawBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.raw"
    val rawPossibleDupBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.raw.possible.dup"
    val preparedBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.prepared"
    val annotatedBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.prepared.annotated"
    val harmonizedBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.harmonized"
    val refBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.ref"
    val imputeBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.impute"
    val filteredBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.ref.filtered"
    val prunedBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.ref.filtered.pruned"
    val kinshipBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.kinship"
    val ref1kgBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.ref1kg"
    val ancestryBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.ancestry"
    val ancestryPcaBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.ancestry.pca"
    val ancestryClusterBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.ancestry.cluster"
    val pcaBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.pca"
    val sampleQcBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.sampleqc"
    val filterQcBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.qc"
    val filterPostQcBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.postqc"
    val cleanBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.clean"
  
    val baseName = arrayCfg.filename.split("/").last
  
    val plink = (arrayCfg.technology, arrayCfg.format) match {
      case (m,n) if inputTypesPlink.contains((m,n)) =>
        Some(Plink(
          base = path(arrayCfg.filename),
          data = Seq(store(path(checkPath(arrayCfg.filename + s".bed"))).asInput,store(path(checkPath(arrayCfg.filename + s".bim"))).asInput,store(path(checkPath(arrayCfg.filename + s".fam"))).asInput),
        ))
      case (o,p) if inputTypesGwasVcf.contains((o,p)) =>
        Some(Plink(
          base = dirTree.base.local.get / rawBaseString,
          data = bedBimFam(path(s"${dirTree.base.local.get}" + s"/${rawBaseString}"))
        ))
      case (q,r) if inputTypesSeqVcf.contains((q,r)) => None
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }
  
    val vcf = (arrayCfg.technology, arrayCfg.format) match {
      case (m, n) if inputTypesGwasVcf.contains((m,n)) =>
        Some(MultiPathVcf(
          base = MultiPath(local = Some(path(arrayCfg.filename.replaceAll(".vcf.gz$|.vcf.bgz$|.gz$|.bgz$",""))), google = None),
          data = MultiStore(local = Some(store(checkPath(arrayCfg.filename)).asInput), google = None),
          tbi = MultiStore(local = Some(store(checkPath(arrayCfg.filename + s".tbi")).asInput), google = None)
        ))
      case (o, p) if inputTypesSeqVcf.contains((o,p)) => None
        Some(MultiPathVcf(
          base = MultiPath(local = Some(path(arrayCfg.filename.replaceAll(".vcf.gz$|.vcf.bgz$|.gz$|.bgz$",""))), google = projectConfig.hailCloud match { case true => Some(dirTree.dataGlobal.google.get / baseName.replaceAll(".vcf.gz$|.vcf.bgz$|.gz$|.bgz$","")); case false => None }),
          data = MultiStore(local = Some(store(checkPath(arrayCfg.filename)).asInput), google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / baseName)); case false => None }),
          tbi = MultiStore(local = Some(store(checkPath(arrayCfg.filename + s".tbi")).asInput), google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / s"${baseName}.tbi")); case false => None })
        ))
      case (q,r) if inputTypesPlink.contains((q,r)) => None
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }
  
    val rawData = RawData(
      plink = plink,
      vcf = vcf,
      rawBase = gwasTech.contains(arrayCfg.technology) match { case true => Some(dirTree.dataArrayMap(arrayCfg).prepare.local.get / rawBaseString); case false => None },
      unplaced = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawBaseString}.unplaced")); case false => None },
      unique = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawBaseString}.unique")); case false => None },
      indel = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawBaseString}.indel")); case false => None },
      lmiss = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawBaseString}.missing.lmiss")); case false => None },
      imiss = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawBaseString}.missing.imiss")); case false => None },
      imissRemove = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawBaseString}.missing.imiss.remove")); case false => None },
      freq = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawBaseString}.freq.frq")); case false => None },
      mono = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawBaseString}.mono")); case false => None },
      uids = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawBaseString}.uids")); case false => None },
      possDupVars = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawPossibleDupBaseString}.vars")); case false => None },
      possDupPlink = gwasTech.contains(arrayCfg.technology) match { case true => Some(Plink(base = dirTree.dataArrayMap(arrayCfg).prepare.local.get / rawPossibleDupBaseString, data = bedBimFam(path(s"${dirTree.dataArrayMap(arrayCfg).prepare.local.get}" + s"/${rawPossibleDupBaseString}")))); case false => None },
      possDupBase = gwasTech.contains(arrayCfg.technology) match { case true => Some(dirTree.dataArrayMap(arrayCfg).prepare.local.get / rawPossibleDupBaseString); case false => None },
      possDupFreq = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawPossibleDupBaseString}.freq.frq")); case false => None },
      possDupLmiss = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawPossibleDupBaseString}.missing.lmiss")); case false => None },
      dupVarsRemove = gwasTech.contains(arrayCfg.technology) match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${rawBaseString}.dup.vars.remove")); case false => None }
    )
  
    val preparedData = gwasTech.contains(arrayCfg.technology) match {
  
      case true =>
  
        Some(PreparedData(
          plink = Plink(base = dirTree.dataArrayMap(arrayCfg).prepare.local.get / preparedBaseString, data = bedBimFam(dirTree.dataArrayMap(arrayCfg).prepare.local.get / preparedBaseString)),
          multiallelic = store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${preparedBaseString}.multiallelic"),
          chain = arrayCfg.liftOver.map(s => store(path(checkPath(s))).asInput),
          bed = arrayCfg.liftOver.map(s => store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${preparedBaseString}.liftover.bed")),
          lifted = arrayCfg.liftOver.map(s => store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${preparedBaseString}.lifted")),
          unlifted = arrayCfg.liftOver.map(s => store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${preparedBaseString}.unlifted")),
          liftedUpdate = arrayCfg.liftOver.map(s => store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${preparedBaseString}.lifted.update")),
          liftedExtract = arrayCfg.liftOver.map(s => store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${preparedBaseString}.lifted.extract"))))
  
      case false => None
  
    }
  
    val annotatedData = gwasTech.contains(arrayCfg.technology) match {
  
      case true =>
  
        Some(AnnotatedData(
          plink = arrayCfg.liftOver match {
            case Some(s) => Plink(base = dirTree.dataArrayMap(arrayCfg).prepare.local.get / annotatedBaseString, data = bedBimFam(dirTree.dataArrayMap(arrayCfg).prepare.local.get / annotatedBaseString))
            case None => preparedData.get.plink
          },
          snplist = arrayCfg.liftOver match {
            case Some(s) => store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${annotatedBaseString}.variants.snplist")
            case None => store(dirTree.dataArrayMap(arrayCfg).prepare.local.get / s"${preparedBaseString}.variants.snplist")
          }
        ))
  
      case false => None
  
    }
  
    val annotatedChrData = gwasTech.contains(arrayCfg.technology) match {
  
      case true =>
  
        Some(expandChrList(arrayCfg.chrs).map { chr =>
  
          val chrSnpsBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.chr${chr}.snps"
          val chrOtherBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.chr${chr}.other"
          val mergedKgBaseString = s"${chrSnpsBaseString}.harmkg"
          val mergedKgHuRefBaseString = s"${chrSnpsBaseString}.huref"
          val mergedKgNonKgBaseString = s"${chrSnpsBaseString}.nonkg"
          val otherHuRefBaseString = s"${chrOtherBaseString}.huref"
          val otherNonKgBaseString = s"${chrOtherBaseString}.nonkg"
          val refBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.chr${chr}.ref"
          val harmonizedBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.chr${chr}.harmonized"

          val mergedKgHuRefPlink = Plink(
            base = dirTree.dataArrayMap(arrayCfg).harmonize.local.get / mergedKgHuRefBaseString,
            data = bedBimFam(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / mergedKgHuRefBaseString)
          )

          val refPlink = arrayCfg.keepIndels match {
            case true =>
              Plink(
                base = dirTree.dataArrayMap(arrayCfg).harmonize.local.get / refBaseString,
                data = bedBimFam(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / refBaseString)
              )
            case false => mergedKgHuRefPlink
          }

          val harmonizedVcf = MultiPathVcf(
            base = MultiPath(
              local = Some(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / harmonizedBaseString),
              google = projectConfig.hailCloud match {
                case true => Some(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / harmonizedBaseString)
                case false => None
              }
            ),
            data = MultiStore(
              local = Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.vcf.bgz")),
              google = projectConfig.hailCloud match {
                case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / s"${harmonizedBaseString}.vcf.bgz"))
                case false => None
              }
            ),
            tbi = MultiStore(
              local = Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.vcf.bgz.tbi")),
              google = projectConfig.hailCloud match {
                case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / s"${harmonizedBaseString}.vcf.bgz.tbi"))
                case false => None
              }
            )
          )
          
          chr -> AnnotatedChrData(
            snpsPlink = Plink(base = dirTree.dataArrayMap(arrayCfg).harmonize.local.get / chrSnpsBaseString, data = bedBimFam(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / chrSnpsBaseString)),
            otherPlink = arrayCfg.keepIndels match {
              case true =>
                Some(Plink(
                  base = dirTree.dataArrayMap(arrayCfg).harmonize.local.get / chrOtherBaseString,
                  data = bedBimFam(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / chrOtherBaseString)
                ))
              case false => None
            },
            mergedKgPlink = Plink(base = dirTree.dataArrayMap(arrayCfg).harmonize.local.get / mergedKgBaseString, data = bedBimFam(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / mergedKgBaseString)),
            mergedKgHuRefPlink = mergedKgHuRefPlink,
            otherHuRefPlink = arrayCfg.keepIndels match { case true => Some(Plink(base = dirTree.dataArrayMap(arrayCfg).harmonize.local.get / otherHuRefBaseString, data = bedBimFam(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / otherHuRefBaseString))); case false => None },
            refPlink = refPlink,
            mergedKgNonKgBase = dirTree.dataArrayMap(arrayCfg).harmonize.local.get / mergedKgNonKgBaseString,
            otherNonKgBase = arrayCfg.keepIndels match { case true => Some(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / otherNonKgBaseString); case false => None },
            nonKgRemove = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${mergedKgNonKgBaseString}.remove"),
            nonKgIgnore = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${mergedKgNonKgBaseString}.ignore"),
            nonKgMono = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${mergedKgNonKgBaseString}.mono"),
            nonKgNomatch = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${mergedKgNonKgBaseString}.nomatch"),
            nonKgFlip = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${mergedKgNonKgBaseString}.flip"),
            nonKgForceA1 = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${mergedKgNonKgBaseString}.force_a1"),
            otherRemove = arrayCfg.keepIndels match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${otherNonKgBaseString}.remove")); case false => None },
            otherIgnore = arrayCfg.keepIndels match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${otherNonKgBaseString}.ignore")); case false => None },
            otherMono = arrayCfg.keepIndels match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${otherNonKgBaseString}.mono")); case false => None },
            otherNomatch = arrayCfg.keepIndels match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${otherNonKgBaseString}.nomatch")); case false => None },
            otherFlip = arrayCfg.keepIndels match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${otherNonKgBaseString}.flip")); case false => None },
            otherForceA1 = arrayCfg.keepIndels match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${otherNonKgBaseString}.force_a1")); case false => None },
            mergedKgVarIdUpdate = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${mergedKgBaseString}_idUpdates.txt"),
            mergedKgVarSnpLog = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${mergedKgBaseString}_snpLog.log"),
            forceA2 = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.force_a2.txt"),
            harmonizedVcf = harmonizedVcf,
            harmonizedBim = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.bim"),
            harmonizedFam = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.fam"))
  
        }.toMap)
  
      case false => None
  
    }
  
    //val harmonizedData = gwasTech.contains(arrayCfg.technology) match {
    //
    //  case true =>
    //
    //    Some(HarmonizedData(
    //      plink = Plink(base = dirTree.dataArrayMap(arrayCfg).harmonize.local.get / harmonizedBaseString, data = bedBimFam(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / harmonizedBaseString)),
    //      mergeList = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.merge.txt"),
    //      nonKgRemove = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.nonkg.remove"),
    //      nonKgIgnore = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.nonkg.ignore"),
    //      nonKgMono = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.nonkg.mono"),
    //      nonKgNomatch = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.nonkg.nomatch"),
    //      nonKgFlip = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.nonkg.flip"),
    //      nonKgForceA1 = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.nonkg.force_a1"),
    //      mergedKgVarIdUpdate = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}_idUpdates.txt"),
    //      mergedKgVarSnpLog = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}_snpLog.log"),
    //      forceA2 = store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${harmonizedBaseString}.force_a2.txt")))
    //
    //  case false => None
    //
    //}

    val refVcf = (arrayCfg.technology, arrayCfg.format) match {
      case (m,n) if inputTypesSeqVcf.contains((m,n)) => Seq(rawData.vcf.get)
      case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) =>
        annotatedChrData.get.values.map(e => e.harmonizedVcf).toSeq
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }

    val bim = (arrayCfg.technology, arrayCfg.format) match {
      case (m,n) if inputTypesSeqVcf.contains((m,n)) => Seq(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${projectConfig.projectId}.${arrayCfg.id}.harmonized.bim"))
      case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) =>
        annotatedChrData.get.values.map(e => e.harmonizedBim).toSeq
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }

    val fam = (arrayCfg.technology, arrayCfg.format) match {
      case (m,n) if inputTypesSeqVcf.contains((m,n)) => Seq(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${projectConfig.projectId}.${arrayCfg.id}.harmonized.fam"))
      case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) =>
        annotatedChrData.get.values.map(e => e.harmonizedFam).toSeq
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }

    val vcfGlob = (arrayCfg.technology, arrayCfg.format) match {
      case (m,n) if inputTypesSeqVcf.contains((m,n)) =>
        MultiPath(
          local = Some(path(s"""${rawData.vcf.get.data.local.get.toString.split("@")(1)}""")),
          google = projectConfig.hailCloud match { case true => Some(uri(s"""${rawData.vcf.get.data.google.get.toString.split("@")(1)}""")); case false => None }
        )
      case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) =>
        MultiPath(
          local = Some(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${projectConfig.projectId}.${arrayCfg.id}.chr*.harmonized.vcf.bgz"),
          google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / s"${projectConfig.projectId}.${arrayCfg.id}.chr*.harmonized.vcf.bgz"); case false => None }
        )
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }
  
    //val refPlink = (arrayCfg.technology, arrayCfg.format) match {
    //  case (m,n) if gwasTech.contains(m) =>
    //    Some(MultiPathPlink(
    //      base = MultiPath(
    //        local = Some(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / refBaseString),
    //        google = projectConfig.hailCloud match {
    //          case true => Some(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / refBaseString)
    //          case false => None
    //        }
    //      ),
    //      data = MultiSeqStore(
    //        local = Some(bedBimFam(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / refBaseString)),
    //        google = projectConfig.hailCloud match {
    //          case true => Some(bedBimFam(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / refBaseString))
    //          case false => None
    //        }
    //      )
    //    ))
    //  case (o,p) if inputTypesSeqPlink.contains((o,p)) => 
    //    Some(MultiPathPlink(
    //      base = MultiPath(
    //        local = Some(rawData.plink.get.base),
    //        google = projectConfig.hailCloud match {
    //          case true => Some(dirTree.dataGlobal.google.get / baseName)
    //          case false => None
    //        }
    //      ),
    //      data = MultiSeqStore(
    //        local = Some(rawData.plink.get.data),
    //        google = projectConfig.hailCloud match {
    //          case true => Some(bedBimFam(dirTree.dataGlobal.google.get / baseName))
    //          case false => None
    //        }
    //      )
    //    ))
    //  case (q,r) if inputTypesSeqVcf.contains((q,r)) => None
    //  case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    //}
  
    val refData = RefData(
      //plink = refPlink,
      vcf = refVcf,
      bim = bim,
      fam = fam,
      vcfGlob = vcfGlob,
      mtCheckpoint = MultiStore(
        local = projectConfig.hailCloud match { case false => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${refBaseString}.mt.checkpoint")); case true => None },
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / s"${refBaseString}.mt.checkpoint")); case false => None }
      ),
      mt = MultiStore(
        local = projectConfig.hailCloud match { case false => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${refBaseString}.mt")); case true => None },
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / s"${refBaseString}.mt")); case false => None }
      ),
      hailLog = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${refBaseString}.hail.log")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / s"${refBaseString}.hail.log")); case false => None }
      ),
      variantMetrics = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${refBaseString}.variant.metrics.tsv.bgz")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / s"${refBaseString}.variant.metrics.tsv.bgz")); case false => None }
      ),
      sitesVcf = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.sites_only.vcf.bgz")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).annotate.google.get / s"${refBaseString}.sites_only.vcf.bgz")); case false => None }
      ),
      sitesVcfTbi = store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.sites_only.vcf.bgz.tbi"),
      annotations = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.annotations.bgz")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).annotate.google.get / s"${refBaseString}.annotations.bgz")); case false => None }
      ),
      annotationsHt = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.annotations.ht")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).annotate.google.get / s"${refBaseString}.annotations.ht")); case false => None }
      ),
      annotationsHailLog = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.annotations.hail.log")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).annotate.google.get / s"${refBaseString}.annotations.hail.log")); case false => None }
      ),
      annotationWarnings = store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.annotations.warnings"),
      annotationHeader = store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.annotations.header"))
  
    val imputeData = ImputeData(
      data = bedBimFam(dirTree.dataArrayMap(arrayCfg).impute.local.get / imputeBaseString),
      base = dirTree.dataArrayMap(arrayCfg).impute.local.get / imputeBaseString)
  
    val filteredData = FilteredData(
      variantFilters = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).sampleqc.local.get / s"${filteredBaseString}.variant.filters.txt")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).sampleqc.google.get / s"${filteredBaseString}.variant.filters.txt")); case false => None }
      ),
      variantMetrics = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).sampleqc.local.get / s"${filteredBaseString}.variant.metrics.tsv.bgz")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).sampleqc.google.get / s"${filteredBaseString}.variant.metrics.tsv.bgz")); case false => None }
      ),
      plink = MultiPathPlink(
        base = MultiPath(local = Some(dirTree.dataArrayMap(arrayCfg).sampleqc.local.get / filteredBaseString),
        google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).sampleqc.google.get / filteredBaseString); case false => None }),
        data = MultiSeqStore(local = Some(bedBimFam(dirTree.dataArrayMap(arrayCfg).sampleqc.local.get / filteredBaseString)), google = projectConfig.hailCloud match { case true => Some(bedBimFam(dirTree.dataArrayMap(arrayCfg).sampleqc.google.get / filteredBaseString)); case false => None })
      ),
      hailLog = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).sampleqc.local.get / s"${filteredBaseString}.hail.log")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).sampleqc.google.get / s"${filteredBaseString}.hail.log")); case false => None }
      ),
      pruneIn = store(dirTree.dataArrayMap(arrayCfg).sampleqc.local.get / s"${filteredBaseString}.prune.in"))
  
    val prunedData = PrunedData(
      plink = Plink(base = dirTree.dataArrayMap(arrayCfg).sampleqc.local.get / prunedBaseString, data = bedBimFam(dirTree.dataArrayMap(arrayCfg).sampleqc.local.get / prunedBaseString)),
      bimGoogle = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).sampleqc.google.get / s"${prunedBaseString}.bim")); case false => None })
  
    val kinshipData = KinshipData(
      base = dirTree.dataArrayMap(arrayCfg).kinship.local.get / kinshipBaseString,
      log = store(dirTree.dataArrayMap(arrayCfg).kinship.local.get / s"${kinshipBaseString}.log"),
      kin0 = store(dirTree.dataArrayMap(arrayCfg).kinship.local.get / s"${kinshipBaseString}.kin0"),
      famSizes = store(dirTree.dataArrayMap(arrayCfg).kinship.local.get / s"${kinshipBaseString}.famsizes.tsv"))
  
    val ref1kgData = Ref1kgData(
      plink = MultiPathPlink(base = MultiPath(local = Some(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / ref1kgBaseString), google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).ancestry.google.get / ref1kgBaseString); case false => None }), data = MultiSeqStore(local = Some(bedBimFam(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / ref1kgBaseString)), google = projectConfig.hailCloud match { case true => Some(bedBimFam(dirTree.dataArrayMap(arrayCfg).ancestry.google.get / ref1kgBaseString)); case false => None })),
      hailLog = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ref1kgBaseString}.hail.log")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).ancestry.google.get / s"${ref1kgBaseString}.hail.log")); case false => None }
      ),
      kgSamples = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ref1kgBaseString}.kgsamples.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).ancestry.google.get / s"${ref1kgBaseString}.kgsamples.tsv")); case false => None }
      ))
  
    val ancestryData = AncestryData(
      inferred = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryBaseString}.inferred.tsv"))
  
    val ancestryPcaData = AncestryPcaData(
      base = dirTree.dataArrayMap(arrayCfg).ancestry.local.get / ancestryPcaBaseString,
      log = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryPcaBaseString}.log"),
      scores = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryPcaBaseString}.scores.tsv"),
      eigenVecs = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryPcaBaseString}.eigenvecs.tsv"),
      loadings = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryPcaBaseString}.loadings.tsv"),
      eigenVals = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryPcaBaseString}.eigenvals.txt"),
      pve = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryPcaBaseString}.pve.txt"),
      meansd = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryPcaBaseString}.meansd.tsv"),
      plots = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryPcaBaseString}.plots.pdf"),
      plotsPc1Pc2Png = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryPcaBaseString}.plots.pc1pc2.png"),
      plotsPc2Pc3Png = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryPcaBaseString}.plots.pc2pc3.png"))
    
    val ancestryClusterData = AncestryClusterData(
      base = dirTree.dataArrayMap(arrayCfg).ancestry.local.get / ancestryClusterBaseString,
      log = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryClusterBaseString}.log"),
      fet = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryClusterBaseString}.fet.1"),
      clu = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryClusterBaseString}.clu.1"),
      klg = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryClusterBaseString}.klg.1"),
      plots = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryClusterBaseString}.plots.pdf"),
      plotsPc1Pc2Png = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryClusterBaseString}.plots.pc1pc2.png"),
      plotsPc2Pc3Png = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryClusterBaseString}.plots.pc2pc3.png"),
      centerPlots = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryClusterBaseString}.plots.centers.pdf"),
      no1kgPlots = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryClusterBaseString}.plots.no1kg.pdf"),
      xtab = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryClusterBaseString}.xtab"),
      groups = store(dirTree.dataArrayMap(arrayCfg).ancestry.local.get / s"${ancestryClusterBaseString}.groups.tsv"))
  
    val pcaData = PcaData(
      log = store(dirTree.dataArrayMap(arrayCfg).pca.local.get / s"${pcaBaseString}.log"),
      scores = store(dirTree.dataArrayMap(arrayCfg).pca.local.get / s"${pcaBaseString}.scores.tsv"),
      eigenVecs = store(dirTree.dataArrayMap(arrayCfg).pca.local.get / s"${pcaBaseString}.eigenvecs.tsv"),
      loadings = store(dirTree.dataArrayMap(arrayCfg).pca.local.get / s"${pcaBaseString}.loadings.tsv"),
      eigenVals = store(dirTree.dataArrayMap(arrayCfg).pca.local.get / s"${pcaBaseString}.eigenvals.txt"),
      pve = store(dirTree.dataArrayMap(arrayCfg).pca.local.get / s"${pcaBaseString}.pve.txt"),
      meansd = store(dirTree.dataArrayMap(arrayCfg).pca.local.get / s"${pcaBaseString}.meansd.tsv"))
  
    val sexcheckData = SexcheckData(
      sexcheck = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).sexcheck.local.get / s"${sampleQcBaseString}.sexcheck.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).sexcheck.google.get / s"${sampleQcBaseString}.sexcheck.tsv")); case false => None }
      ),
      problems = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).sexcheck.local.get / s"${sampleQcBaseString}.sexcheck.problems.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).sexcheck.google.get / s"${sampleQcBaseString}.sexcheck.problems.tsv")); case false => None }
      ))
  
    val sampleQcData = SampleQcData(
      stats = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.stats.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).metrics.google.get / s"${sampleQcBaseString}.stats.tsv")); case false => None }
      ),
      hailLog = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.stats.hail.log")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).metrics.google.get / s"${sampleQcBaseString}.stats.hail.log")); case false => None }
      ),
      statsAdj = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.stats.adj.tsv"),
      corrPlots = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.corr.pdf"),
      boxPlots = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.boxplots.pdf"),
      discreteness = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.discreteness.txt"),
      pcaLoadings = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.pca.loadings.tsv"),
      pcaPlots = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.pca.plots.pdf"),
      pcaScores = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.pca.scores.tsv"),
      outliers = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.outliers.tsv"),
      incompleteObs = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.incomplete_obs.tsv"),
      metricPlots = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.metricplots.pdf"),
      metricPlotsPng = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.metricplots.png"))
  
    val sampleQcPcaClusterData = SampleQcPcaClusterData(
      base = dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.pca.cluster",
      fet = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.pca.cluster.fet.1"),
      clu = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.pca.cluster.clu.1"),
      klg = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.pca.cluster.klg.1"),
      log = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.pca.cluster.log"),
      outliers = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.pca.cluster.outliers.tsv"),
      plots = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.pca.cluster.plots.pdf"),
      xtab = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.pca.cluster.xtab"))
  
    val sampleQcMetricClusterData = arrayCfg.sampleQcMetrics.map { metric =>
    
        metric -> SampleQcMetricClusterData(
          base = dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.${metric}.cluster",
          fet = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.${metric}.cluster.fet.1"),
          clu = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.${metric}.cluster.clu.1"),
          klg = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.${metric}.cluster.klg.1"),
          log = store(dirTree.dataArrayMap(arrayCfg).metrics.local.get / s"${sampleQcBaseString}.${metric}.cluster.log"))
  
    }.toMap
  
    val filterQc = FilterQc(
      samplesExclude = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).filterQc.local.get / s"${filterQcBaseString}.samples.exclude.txt")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).filterQc.google.get / s"${filterQcBaseString}.samples.exclude.txt")); case false => None }
      ),
  	samplesRestore = store(dirTree.dataArrayMap(arrayCfg).filterQc.local.get / s"${filterQcBaseString}.samples.restore.tbl"))
  
    val filterPostQc = FilterPostQc(
      samplesStats = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.local.get / s"${filterPostQcBaseString}.sample.stats.tsv.bgz")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.google.get / s"${filterPostQcBaseString}.sample.stats.tsv.bgz")); case false => None }
      ),
      samplesExclude = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.local.get / s"${filterPostQcBaseString}.samples.exclude.txt")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.google.get / s"${filterPostQcBaseString}.samples.exclude.txt")); case false => None }
      ),
      variantsStats = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.local.get / s"${filterPostQcBaseString}.variant.stats.tsv.bgz")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.google.get / s"${filterPostQcBaseString}.variant.stats.tsv.bgz")); case false => None }
      ),
      vFilters = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.local.get / s"${filterPostQcBaseString}.variant.filters.txt")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.google.get / s"${filterPostQcBaseString}.variant.filters.txt")); case false => None }
      ),
      sFilters = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.local.get / s"${filterPostQcBaseString}.sample.filters.txt")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.google.get / s"${filterPostQcBaseString}.sample.filters.txt")); case false => None }
      ),
      variantsExclude = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.local.get / s"${filterPostQcBaseString}.variants.exclude.txt")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.google.get / s"${filterPostQcBaseString}.variants.exclude.txt")); case false => None }
      ),
      hailLog = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.local.get / s"${filterPostQcBaseString}.hail.log")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).filterPostQc.google.get / s"${filterPostQcBaseString}.hail.log")); case false => None }
      ))
  
    val cleanVcf = CleanVcf(
      vcf = MultiPathVcf(
        base = MultiPath(
          local = Some(dirTree.dataArrayMap(arrayCfg).clean.local.get / cleanBaseString),
          google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).clean.google.get / cleanBaseString); case false => None }
        ),
        data = MultiStore(
          local = Some(store(dirTree.dataArrayMap(arrayCfg).clean.local.get / s"${cleanBaseString}.vcf.bgz")),
          google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).clean.google.get / s"${cleanBaseString}.vcf.bgz")); case false => None }
        ),
        tbi = MultiStore(local = Some(store(dirTree.dataArrayMap(arrayCfg).clean.local.get / s"${cleanBaseString}.vcf.bgz.tbi")), google = None)
      ),
      hailLog = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).clean.local.get / s"${cleanBaseString}.hail.log")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).clean.google.get / s"${cleanBaseString}.hail.log")); case false => None }
      )
    )

    val cleanBgen = arrayCfg.exportCleanBgen match {

      case true =>

        Some(MultiPathBgen(
          base = MultiPath(
            local = Some(dirTree.dataArrayMap(arrayCfg).clean.local.get / cleanBaseString),
            google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).clean.google.get / cleanBaseString); case false => None }
          ),
          data = MultiStore(
            local = Some(store(dirTree.dataArrayMap(arrayCfg).clean.local.get / s"${cleanBaseString}.bgen")),
            google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).clean.google.get / s"${cleanBaseString}.bgen")); case false => None }
          ),
          sample = MultiStore(
            local = Some(store(dirTree.dataArrayMap(arrayCfg).clean.local.get / s"${cleanBaseString}.sample")),
            google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).clean.google.get / s"${cleanBaseString}.sample")); case false => None }
          ),
          bgi = MultiStore(local = Some(store(dirTree.dataArrayMap(arrayCfg).clean.local.get / s"${cleanBaseString}.bgen.bgi")), google = None)
        ))

      case false => None

    }
  
    arrayCfg -> Array(
      rawData = rawData,
      preparedData = preparedData,
      annotatedData = annotatedData,
      annotatedChrData = annotatedChrData,
      //harmonizedData = harmonizedData,
      refData = refData,
      imputeData = imputeData,
      filteredData = filteredData,
      prunedData = prunedData,
      kinshipData = kinshipData,
      ref1kgData = ref1kgData,
      ancestryData = ancestryData,
      ancestryPcaData = ancestryPcaData,
      ancestryClusterData = ancestryClusterData,
      pcaData = pcaData,
      sexcheckData = sexcheckData,
      sampleQcData = sampleQcData,
      sampleQcPcaClusterData = sampleQcPcaClusterData,
      sampleQcMetricClusterData = sampleQcMetricClusterData,
      filterQc = filterQc,
      filterPostQc = filterPostQc,
      ancestryOutliersKeep = arrayCfg.ancestryOutliersKeep match { case Some(s) => Some(store(path(checkPath(s))).asInput); case None => None },
      duplicatesKeep = arrayCfg.duplicatesKeep match { case Some(s) => Some(store(path(checkPath(s))).asInput); case None => None },
      famsizeKeep = arrayCfg.famsizeKeep match { case Some(s) => Some(store(path(checkPath(s))).asInput); case None => None },
      sampleqcKeep = arrayCfg.sampleqcKeep match { case Some(s) => Some(store(path(checkPath(s))).asInput); case None => None },
      sexcheckKeep = arrayCfg.sexcheckKeep match { case Some(s) => Some(store(path(checkPath(s))).asInput); case None => None },
      cleanVcf = cleanVcf,
      cleanBgen = cleanBgen)
  
  }.toMap

}
