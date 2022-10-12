object Stores extends loamstream.LoamFile {

  import loamstream.model.Store
  
  final case class MultiPath(
    local: Option[Path],
    google: Option[URI])
  
  final case class MultiStore(
    local: Option[Store],
    google: Option[Store])
  
  final case class MultiSeqStore(
    local: Option[Seq[Store]],
    google: Option[Seq[Store]])
  
  final case class Vcf(
    base: Path,
    data: Store,
    tbi: Store)
  
  final case class MultiPathVcf(
    base: MultiPath,
    data: MultiStore,
    tbi: MultiStore)

  final case class MultiPathMT(
    base: MultiPath,
    data: MultiStore)

  final case class MultiPathBgen(
    base: MultiPath,
    data: MultiStore,
    sample: MultiStore,
    bgi: MultiStore)
  
  final case class Plink(
    base: Path,
    data: Seq[Store])
  
  final case class MultiPathPlink(
    base: MultiPath,
    data: MultiSeqStore)

}
