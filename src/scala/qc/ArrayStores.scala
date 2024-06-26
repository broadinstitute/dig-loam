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
    mt: Option[MultiPathMT],
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

  final case class RefData(
    vcf: Option[Seq[MultiPathVcf]],
    varList: MultiStore,
    sampleList: MultiStore,
    vcfGlob: Option[MultiPath],
    rawMt: Option[MultiPathMT],
    mt: MultiStore,
    hailLog: MultiStore,
    variantMetrics: MultiStore,
    sitesVcf: MultiStore,
    sitesVcfChr: Map[String, Store],
    sitesVcfTbi: Store,
    sitesVcfTbiChr: Map[String, Store],
    annotations: Map[String, MultiStore],
    annotationsHt: MultiStore,
    annotationsHailLog: MultiStore,
    annotationWarnings: Map[String, Store],
    annotationHeader: Map[String, Store])
  
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
    inferredGmm: Store,
    inferredKnn: Store)
  
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
  
  final case class AncestryGmmData(
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

  final case class AncestryKnnData(
    predictions: Store,
    plots: Store,
    plotsPc1Pc2Png: Store,
    plotsPc2Pc3Png: Store)
  
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
    samplesExclude: MultiStore)
  
  final case class FilterPostQc(
    samplesStats: MultiStore,
    samplesExclude: MultiStore,
    variantsStats: MultiStore,
    vFilters: MultiStore,
    sFilters: MultiStore,
    variantsExclude: MultiStore,
    hailLog: MultiStore)
  
  final case class ExportedVcf(
    vcf: MultiPathVcf,
    hailLog: MultiStore)

  final case class ExportedStats(
    base: Path,
    freq: Store,
    majorAlleles: Store)

  final case class ExportedBgen(
    bgen: MultiPathBgen,
    bgenAlignedMaf: Option[MultiPathBgen],
    stats: Option[ExportedStats])
  
  final case class Array(
    rawData: RawData,
    preparedData: Option[PreparedData],
    annotatedData: Option[AnnotatedData],
    annotatedChrData: Option[Map[String, AnnotatedChrData]],
    refData: RefData,
    imputeData: ImputeData,
    filteredData: FilteredData,
    prunedData: PrunedData,
    kinshipData: KinshipData,
    ref1kgData: Ref1kgData,
    ancestryData: AncestryData,
    ancestryPcaData: AncestryPcaData,
    ancestryGmmData: AncestryGmmData,
    ancestryKnnData: AncestryKnnData,
    pcaData: PcaData,
    sexcheckData: SexcheckData,
    sampleQcData: SampleQcData,
    sampleQcPcaClusterData: SampleQcPcaClusterData,
    sampleQcMetricClusterData: Map[String, SampleQcMetricClusterData],
    filterQc: FilterQc,
    filterPostQc: FilterPostQc,
	unfilteredVcf: ExportedVcf,
    filteredVcf: Option[ExportedVcf],
	unfilteredBgen: ExportedBgen,
    filteredBgen: Option[ExportedBgen])
  
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
    val ancestryGmmBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.ancestry.gmm"
    val ancestryKnnBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.ancestry.knn"
    val pcaBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.pca"
    val sampleQcBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.sampleqc"
    val filterQcBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.qc"
    val filterPostQcBaseString = s"${projectConfig.projectId}.${arrayCfg.id}.postqc"
    val resultsBaseString = s"${projectConfig.projectId}.${arrayCfg.id}"
  
    val baseName = arrayCfg.filename.split("/").last
  
    val plink = (arrayCfg.technology, arrayCfg.format) match {
      case (m,n) if (inputTypesPlink ++ inputTypesSeqPlink).contains((m,n)) =>
        Some(Plink(
          base = path(arrayCfg.filename),
          data = Seq(store(path(checkPath(arrayCfg.filename + s".bed"))).asInput,store(path(checkPath(arrayCfg.filename + s".bim"))).asInput,store(path(checkPath(arrayCfg.filename + s".fam"))).asInput),
        ))
      case (o,p) if inputTypesGwasVcf.contains((o,p)) =>
        Some(Plink(
          base = dirTree.base.local.get / rawBaseString,
          data = bedBimFam(path(s"${dirTree.base.local.get}" + s"/${rawBaseString}"))
        ))
      case (q,r) if (inputTypesSeqVcf ++ inputTypesSeqMT ++ inputTypesGwasMT).contains((q,r)) => None
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }
  
    val vcf = (arrayCfg.technology, arrayCfg.format) match {
      case (m,n) if inputTypesGwasVcf.contains((m,n)) =>
        Some(MultiPathVcf(
          base = MultiPath(local = Some(path(arrayCfg.filename.replaceAll(".vcf.gz$|.vcf.bgz$|.gz$|.bgz$",""))), google = None),
          data = MultiStore(local = Some(store(checkPath(arrayCfg.filename)).asInput), google = None),
          tbi = MultiStore(local = Some(store(checkPath(arrayCfg.filename + s".tbi")).asInput), google = None)
        ))
      case (o,p) if inputTypesSeqVcf.contains((o,p)) =>
        Some(MultiPathVcf(
          base = MultiPath(local = Some(path(arrayCfg.filename.replaceAll(".vcf.gz$|.vcf.bgz$|.gz$|.bgz$",""))), google = projectConfig.hailCloud match { case true => Some(dirTree.dataGlobal.google.get / baseName.replaceAll(".vcf.gz$|.vcf.bgz$|.gz$|.bgz$","")); case false => None }),
          data = MultiStore(local = Some(store(checkPath(arrayCfg.filename)).asInput), google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / baseName)); case false => None }),
          tbi = MultiStore(local = Some(store(checkPath(arrayCfg.filename + s".tbi")).asInput), google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / s"${baseName}.tbi")); case false => None })
        ))
      case (q,r) if (inputTypesPlink ++ inputTypesSeqMT ++ inputTypesSeqPlink ++ inputTypesGwasMT).contains((q,r)) => None
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }

    val mt = (arrayCfg.technology, arrayCfg.format) match {
      case (m,n) if (inputTypesSeqMT ++ inputTypesGwasMT).contains((m,n)) =>
        Some(MultiPathMT(
          base = MultiPath(local = Some(path(arrayCfg.filename)), google = projectConfig.hailCloud match { case true => Some(dirTree.dataGlobal.google.get / baseName); case false => None }),
          data = MultiStore(local = Some(store(checkPath(arrayCfg.filename)).asInput), google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataGlobal.google.get / baseName)); case false => None })
        ))
      case (o,p) if (inputTypesPlink ++ inputTypesGwasVcf ++ inputTypesSeqPlink ++ inputTypesSeqVcf).contains((o,p)) => None
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }
  
    val rawData = RawData(
      plink = plink,
      vcf = vcf,
      mt = mt,
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

    val refVcf = (arrayCfg.technology, arrayCfg.format) match {
      case (m,n) if inputTypesSeqVcf.contains((m,n)) => Some(Seq(rawData.vcf.get))
      case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) =>
        Some(annotatedChrData.get.values.map(e => e.harmonizedVcf).toSeq)
      case (q,r) if (inputTypesSeqMT ++ inputTypesGwasMT).contains((q,r)) => None
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }

    val refRawMt = (arrayCfg.technology, arrayCfg.format) match {
      case (m,n) if (inputTypesSeqMT ++ inputTypesGwasMT).contains((m,n)) => rawData.mt
      case (o,p) if (inputTypesSeqVcf ++ inputTypesGwasVcf ++ inputTypesPlink ++ inputTypesSeqPlink).contains((o,p)) => None
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }

    val vcfGlob = (arrayCfg.technology, arrayCfg.format) match {
      case (m,n) if inputTypesSeqVcf.contains((m,n)) =>
        Some(MultiPath(
          local = Some(path(s"""${rawData.vcf.get.data.local.get.toString.split("@")(1)}""")),
          google = projectConfig.hailCloud match { case true => Some(uri(s"""${rawData.vcf.get.data.google.get.toString.split("@")(1)}""")); case false => None }
        ))
      case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) =>
        Some(MultiPath(
          local = Some(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${projectConfig.projectId}.${arrayCfg.id}.chr*.harmonized.vcf.bgz"),
          google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / s"${projectConfig.projectId}.${arrayCfg.id}.chr*.harmonized.vcf.bgz"); case false => None }
        ))
      case (q,r) if (inputTypesSeqMT ++ inputTypesGwasMT).contains((q,r)) => None
      case _ => throw new CfgException("invalid technology and format combination: " + arrayCfg.technology + ", " + arrayCfg.format)
    }
  
    val refData = RefData(
      vcf = refVcf,
      varList = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${refBaseString}.variants.txt")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / s"${refBaseString}.variants.txt")); case false => None }
      ),
      sampleList = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${refBaseString}.samples.txt")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.google.get / s"${refBaseString}.samples.txt")); case false => None }
      ),
      vcfGlob = vcfGlob,
      rawMt = refRawMt,
      mt = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).harmonize.local.get / s"${refBaseString}.mt")),
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
      sitesVcfChr = expandChrList(arrayCfg.chrs).map { chr =>
        chr -> store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.sites_only.chr${chr}.vcf.bgz")
      }.toMap,
      sitesVcfTbi = store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.sites_only.vcf.bgz.tbi"),
      sitesVcfTbiChr = expandChrList(arrayCfg.chrs).map { chr =>
        chr -> store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.sites_only.chr${chr}.vcf.bgz.tbi")
      }.toMap,
      annotations = expandChrList(arrayCfg.chrs).map { chr =>
        chr -> MultiStore(
          local = Some(store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.chr${chr}.annotations.bgz")),
          google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).annotate.google.get / s"${refBaseString}.chr${chr}.annotations.bgz")); case false => None }
        )
      }.toMap,
      annotationsHt = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.annotations.ht")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).annotate.google.get / s"${refBaseString}.annotations.ht")); case false => None }
      ),
      annotationsHailLog = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.annotations.hail.log")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).annotate.google.get / s"${refBaseString}.annotations.hail.log")); case false => None }
      ),
      annotationWarnings = expandChrList(arrayCfg.chrs).map { chr =>
        chr -> store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.chr${chr}.annotations.warnings")
      }.toMap,
      annotationHeader = expandChrList(arrayCfg.chrs).map { chr =>
        chr -> store(dirTree.dataArrayMap(arrayCfg).annotate.local.get / s"${refBaseString}.chr${chr}.annotations.header")
      }.toMap)

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
      inferredGmm = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.inferred.tsv"),
      inferredKnn = store(dirTree.dataArrayMap(arrayCfg).knn.local.get / s"${ancestryKnnBaseString}.inferred.tsv"))
  
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
    
    val ancestryGmmData = AncestryGmmData(
      base = dirTree.dataArrayMap(arrayCfg).gmm.local.get / ancestryGmmBaseString,
      log = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.log"),
      fet = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.fet.1"),
      clu = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.clu.1"),
      klg = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.klg.1"),
      plots = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.plots.pdf"),
      plotsPc1Pc2Png = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.plots.pc1pc2.png"),
      plotsPc2Pc3Png = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.plots.pc2pc3.png"),
      centerPlots = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.plots.centers.pdf"),
      no1kgPlots = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.plots.no1kg.pdf"),
      xtab = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.xtab"),
      groups = store(dirTree.dataArrayMap(arrayCfg).gmm.local.get / s"${ancestryGmmBaseString}.groups.tsv"))

    val ancestryKnnData = AncestryKnnData(
      predictions = store(dirTree.dataArrayMap(arrayCfg).knn.local.get / s"${ancestryKnnBaseString}.predictions.tsv"),
      plots = store(dirTree.dataArrayMap(arrayCfg).knn.local.get / s"${ancestryKnnBaseString}.plots.pdf"),
      plotsPc1Pc2Png = store(dirTree.dataArrayMap(arrayCfg).knn.local.get / s"${ancestryKnnBaseString}.plots.pc1pc2.png"),
      plotsPc2Pc3Png = store(dirTree.dataArrayMap(arrayCfg).knn.local.get / s"${ancestryKnnBaseString}.plots.pc2pc3.png"))
  
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
      ))
  
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

    val unfilteredVcf = ExportedVcf(
      vcf = MultiPathVcf(
        base = MultiPath(
          local = Some(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered"),
          google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.unfiltered"); case false => None }
        ),
        data = MultiStore(
          local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.vcf.bgz")),
          google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.unfiltered.vcf.bgz")); case false => None }
        ),
        tbi = MultiStore(local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.vcf.bgz.tbi")), google = None)
      ),
      hailLog = MultiStore(
        local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.hail.log")),
        google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.unfiltered.hail.log")); case false => None }
      )
    )
  
    val filteredVcf = arrayCfg.exportFiltered match {
      case true =>
        Some(ExportedVcf(
          vcf = MultiPathVcf(
            base = MultiPath(
              local = Some(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered"),
              google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.filtered"); case false => None }
            ),
            data = MultiStore(
              local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.vcf.bgz")),
              google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.filtered.vcf.bgz")); case false => None }
            ),
            tbi = MultiStore(local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.vcf.bgz.tbi")), google = None)
          ),
          hailLog = MultiStore(
            local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.hail.log")),
            google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.filtered.hail.log")); case false => None }
          )
        ))
      case false => None
    }

    val unfilteredBgen = ExportedBgen(
      bgen = MultiPathBgen(
        base = MultiPath(
          local = Some(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered"),
          google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.unfiltered"); case false => None }
        ),
        data = MultiStore(
          local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.bgen")),
          google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.unfiltered.bgen")); case false => None }
        ),
        sample = MultiStore(
          local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.sample")),
          google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.unfiltered.sample")); case false => None }
        ),
        bgi = MultiStore(local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.bgen.bgi")), google = None)
      ),
      bgenAlignedMaf = arrayCfg.exportBgenAlignedMinor match {
        case true =>
          Some(MultiPathBgen(
            base = MultiPath(
              local = Some(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.aligned_maf"),
              google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.unfiltered.aligned_maf"); case false => None }
            ),
            data = MultiStore(
              local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.aligned_maf.bgen")),
              google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.unfiltered.aligned_maf.bgen")); case false => None }
            ),
            sample = MultiStore(
              local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.aligned_maf.sample")),
              google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.unfiltered.aligned_maf.sample")); case false => None }
            ),
            bgi = MultiStore(local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.aligned_maf.bgen.bgi")), google = None)
          ))
        case false => None
      },
      stats = arrayCfg.exportBgenAlignedMinor match {
        case true =>
          Some(ExportedStats(
            base = dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.bgen.stats",
            freq = store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.bgen.stats.afreq"),
            majorAlleles = store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.unfiltered.bgen.stats.major_alleles.tsv")
          ))
        case false => None
      }
    )

    val filteredBgen = arrayCfg.exportFiltered match {
      case true =>
        Some(ExportedBgen(
          bgen = MultiPathBgen(
            base = MultiPath(
              local = Some(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered"),
              google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.filtered"); case false => None }
            ),
            data = MultiStore(
              local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.bgen")),
              google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.filtered.bgen")); case false => None }
            ),
            sample = MultiStore(
              local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.sample")),
              google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.filtered.sample")); case false => None }
            ),
            bgi = MultiStore(local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.bgen.bgi")), google = None)
          ),
          bgenAlignedMaf = arrayCfg.exportBgenAlignedMinor match {
            case true =>
              Some(MultiPathBgen(
                base = MultiPath(
                  local = Some(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.aligned_maf"),
                  google = projectConfig.hailCloud match { case true => Some(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.filtered.aligned_maf"); case false => None }
                ),
                data = MultiStore(
                  local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.aligned_maf.bgen")),
                  google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.filtered.aligned_maf.bgen")); case false => None }
                ),
                sample = MultiStore(
                  local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.aligned_maf.sample")),
                  google = projectConfig.hailCloud match { case true => Some(store(dirTree.dataArrayMap(arrayCfg).results.google.get / s"${resultsBaseString}.filtered.aligned_maf.sample")); case false => None }
                ),
                bgi = MultiStore(local = Some(store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.aligned_maf.bgen.bgi")), google = None)
              ))
            case false => None
          },
          stats = arrayCfg.exportBgenAlignedMinor match {
            case true =>
              Some(ExportedStats(
                base = dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.bgen.stats",
                freq = store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.bgen.stats.afreq"),
                majorAlleles = store(dirTree.dataArrayMap(arrayCfg).results.local.get / s"${resultsBaseString}.filtered.bgen.stats.major_alleles.tsv")
              ))
            case false => None
          }
        ))
      case false => None
    }

    arrayCfg -> Array(
      rawData = rawData,
      preparedData = preparedData,
      annotatedData = annotatedData,
      annotatedChrData = annotatedChrData,
      refData = refData,
      imputeData = imputeData,
      filteredData = filteredData,
      prunedData = prunedData,
      kinshipData = kinshipData,
      ref1kgData = ref1kgData,
      ancestryData = ancestryData,
      ancestryPcaData = ancestryPcaData,
      ancestryGmmData = ancestryGmmData,
      ancestryKnnData = ancestryKnnData,
      pcaData = pcaData,
      sexcheckData = sexcheckData,
      sampleQcData = sampleQcData,
      sampleQcPcaClusterData = sampleQcPcaClusterData,
      sampleQcMetricClusterData = sampleQcMetricClusterData,
      filterQc = filterQc,
      filterPostQc = filterPostQc,
      unfilteredVcf = unfilteredVcf,
      filteredVcf = filteredVcf,
      unfilteredBgen = unfilteredBgen,
      filteredBgen = filteredBgen)
  
  }.toMap

}
