object MetaStores extends loamstream.LoamFile {

  import java.io.{File, BufferedWriter, FileWriter}
  import Fxns._
  import ProjectConfig._
  import DirTree._
  import Collections._
  import StoreHelpers._
  import Stores._
  
  final case class MetaCohort(
    data: Seq[Store],
    base: Path,
    samples: Store,
    famMerge: Store,
    kinshipSamplesExclude: Store)
  
  final case class MetaKinship(
    commonVariants: Store,
    mergeList: Store,
    dataFiltered: Seq[Store],
    baseFiltered: Path,
    baseKinship: Path,
    log: Store,
    kin0: Store,
    metaCohort: Map[ConfigCohort, MetaCohort])
  
  //final case class Meta(
  //  results: Store,
  //  resultsGoogle: Option[Store],
  //  hailLog: Store,
  //  hailLogGoogle: Option[Store],
  //  tbi: Store)
  
  //final case class Merge(
  //  results: Store,
  //  resultsGoogle: Option[Store],
  //  hailLog: Store,
  //  hailLogGoogle: Option[Store],
  //  tbi: Store)
  
  val metaKinshipStores = projectConfig.Metas.map { meta =>
  
    val baseString = s"${projectConfig.projectId}.${meta.id}"
    val baseRefFilteredString = s"${baseString}.ref.filtered"
    val baseKinshipString = s"${baseString}.kinship"
  
    val local_dir = dirTree.dataGlobalKinshipMap(meta).local.get
  
    val commonVariants = store(local_dir / s"${baseRefFilteredString}.common_variants.txt")
  
    val metaCohort = projectConfig.Cohorts.filter(e => meta.cohorts contains e.id).map { cohort =>
  
      val cohortBaseString = s"${baseString}.${cohort.id}"
      val cohortRefBaseString = s"${cohortBaseString}.ref"
      val cohortKinshipBaseString = s"${cohortBaseString}.kinship"
  
      cohort -> MetaCohort(
        data = bedBimFam(local_dir / cohortRefBaseString),
        base = local_dir / cohortRefBaseString,
        samples = store(local_dir / s"${cohortBaseString}.samples"),
        famMerge = store(local_dir / s"${cohortRefBaseString}.fam.merge"),
        kinshipSamplesExclude = store(local_dir / s"${cohortKinshipBaseString}.samples.exclude"))
  
    }.toMap
  
    meta -> MetaKinship(
      commonVariants = commonVariants,
      mergeList= store(local_dir / s"${baseRefFilteredString}.merge_list.txt"),
      dataFiltered = bedBimFam(local_dir / baseRefFilteredString),
      baseFiltered = local_dir / baseRefFilteredString,
      baseKinship = local_dir / baseKinshipString,
      log = store(local_dir / s"${baseKinshipString}.log"),
      kin0 = store(local_dir / s"${baseKinshipString}.kin0"),
      metaCohort = metaCohort)
  
  }.toMap
  
  //val metaStores = (
  //    (
  //      for {
  //        x <- modelMetas
  //      } yield {
  //        (x.model, x.meta)
  //      }
  //    )
  //  ).map { modelMeta =>
  //
  //  val model = modelMeta._1
  //  val meta = modelMeta._2
  //
  //  val baseString = s"${projectConfig.projectId}.${meta.id}.${model.id}"
  //
  //  val local_dir = dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).metas(meta).local.get
  //
  //  val cloud_dir = projectConfig.hailCloud match { case true => Some(dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).metas(meta).google.get); case false => None }
  //
  //  modelMeta -> Meta(
  //    results = store(local_dir / s"${baseString}.results.tsv.bgz"),
  //    resultsGoogle = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.results.tsv.bgz")); case false => None },
  //    hailLog = store(local_dir / s"${baseString}.results.hail.log"),
  //    hailLogGoogle = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.results.hail.log")); case false => None },
  //    tbi = store(local_dir / s"${baseString}.results.tsv.bgz.tbi")
  //  )
  //
  //}.toMap
  
  //val knownMetaStores = (
  //    (
  //      for {
  //        x <- modelMetaKnowns
  //      } yield {
  //        (x.model, x.meta, x.known)
  //      }
  //    )
  //  ).map { modelMetaKnown =>
  //
  //  val model = modelMetaKnown._1
  //  val meta = modelMetaKnown._2
  //  val known = modelMetaKnown._3
  //
  //  val prefix = s"${projectConfig.projectId}.${meta.id}.${model.id}.${known.id}"
  //
  //  val local_dir = dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).metas(meta).local.get
  //
  //  val cloud_dir = projectConfig.hailCloud match { case true => Some(dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).metas(meta).google.get); case false => None }
  //
  //  modelMetaKnown -> Meta(
  //      results = store(local_dir / s"${prefix}.results.tsv.bgz"),
  //      resultsGoogle = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${prefix}.results.tsv.bgz")); case false => None },
  //      hailLog = store(local_dir / s"${prefix}.results.hail.log"),
  //      hailLogGoogle = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${prefix}.results.hail.log")); case false => None },
  //      tbi = store(local_dir / s"${prefix}.results.tsv.bgz.tbi")
  //  )
  //
  //}.toMap
  
  //val mergeStores = (
  //    (
  //      for {
  //        x <- modelMerges
  //      } yield {
  //        (x.model, x.merge)
  //      }
  //    )
  //  ).map { modelMerge =>
  //
  //  val model = modelMerge._1
  //  val merge = modelMerge._2
  //
  //  val baseString = s"${projectConfig.projectId}.${merge.id}.${model.id}"
  //
  //  val local_dir = dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).merges(merge).local.get
  //
  //  val cloud_dir = projectConfig.hailCloud match { case true => Some(dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).merges(merge).google.get); case false => None }
  //
  //  modelMerge -> Merge(
  //    results = store(local_dir / s"${baseString}.results.tsv.bgz"),
  //    resultsGoogle = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.results.tsv.bgz")); case false => None },
  //    hailLog = store(local_dir / s"${baseString}.results.hail.log"),
  //    hailLogGoogle = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.results.hail.log")); case false => None },
  //    tbi = store(local_dir / s"${baseString}.results.tsv.bgz.tbi")
  //  )
  //
  //}.toMap
  
  //val knownMergeStores = (
  //    (
  //      for {
  //        x <- modelMergeKnowns
  //      } yield {
  //        (x.model, x.merge, x.known)
  //      }
  //    )
  //  ).map { modelMergeKnown =>
  //
  //  val model = modelMergeKnown._1
  //  val merge = modelMergeKnown._2
  //  val known = modelMergeKnown._3
  //
  //  val prefix = s"${projectConfig.projectId}.${merge.id}.${model.id}.${known.id}"
  //
  //  val local_dir = dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).merges(merge).local.get
  //
  //  val cloud_dir = projectConfig.hailCloud match { case true => Some(dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).merges(merge).google.get); case false => None }
  //
  //  modelMergeKnown -> Merge(
  //      results = store(local_dir / s"${prefix}.results.tsv.bgz"),
  //      resultsGoogle = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${prefix}.results.tsv.bgz")); case false => None },
  //      hailLog = store(local_dir / s"${prefix}.results.hail.log"),
  //      hailLogGoogle = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${prefix}.results.hail.log")); case false => None },
  //      tbi = store(local_dir / s"${prefix}.results.tsv.bgz.tbi")
  //  )
  //
  //}.toMap

}
