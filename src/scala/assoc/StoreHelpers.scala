object StoreHelpers extends loamstream.LoamFile {

  import loamstream.model.Store
  import loamstream.util.Uris.Implicits._
  
  def outputStoreName(name: String)(extension: String): String = s"${name}.${extension}"
  def outputStore(name: String): String => Store = extension => store(outputStoreName(name)(extension))
  
  def withExtensions(extensions: String*)(makeStore: String => Store): Seq[Store] = {
    extensions.map(ex => makeStore(ex))
  }
  
  val (bed, bim, fam) = ("bed", "bim", "fam")
  def bedBimFam(makeStore: String => Store): Seq[Store] = withExtensions(bed, bim, fam)(makeStore)
  def bedBimFam(prefix: String): Seq[Store] = bedBimFam(extension => store(s"${prefix}.${extension}"))
  def bedBimFam(prefix: Path): Seq[Store] = bedBimFam(prefix.toString)
  def bedBimFam(prefix: URI): Seq[Store] = bedBimFam(extension => store(prefix + s".${extension}"))

}
