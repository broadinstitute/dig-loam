import scala.concurrent.Future
import scala.util.Success


import loamstream.loam.LoamScriptContext
import loamstream.loam.NativeTool
import loamstream.util.S3Client
import loamstream.util.HttpClient.Auth
import loamstream.loam.intake.dga.AnnotationType
import loamstream.util.Loggable
import loamstream.loam.intake.dga.AnnotationsSupport


/**
 * @author clint
 * Feb 25, 2021
 */
object DgaIntake extends loamstream.LoamFile with Loggable {
  import loamstream.loam.intake.dga.DgaSyntax._
  
  private final case class Params(
      url: String = AnnotationsSupport.Defaults.url.toString,
      s3Bucket: String = "dig-analysis-data",
      dgaUsername: String,
      dgaPassword: String,
      annotationTypes: Seq[String]) {
    
    def auth: Auth = Auth(username = dgaUsername, password = dgaPassword)
    
    //Fail fast if any of these are unknown
    def annotationTypesToIngest: Set[AnnotationType] = annotationTypes.map(AnnotationType.tryFromString(_).get).toSet
  }
  
  private val params: Params = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    
    val key = "loamstream.aggregator.intake.dga"
    
    loadConfig("INTAKE_CONF", "").config.as[Params](key)
  }
  
  val auth: Auth = params.auth
  
  val annotationTypesToIngest: Set[AnnotationType] = params.annotationTypesToIngest
  
  require(annotationTypesToIngest.nonEmpty, s"Expected at least one annotation type")

  info(s"S3 BUCKET: '${params.s3Bucket}'")
  
  info(s"Will ingest the following annotation types: ${annotationTypesToIngest}")
    
  val s3Client: S3Client = new S3Client.Default(params.s3Bucket)
    
  val annotations = Dga.Annotations.downloadAnnotations(url = new URI(params.url))
        
  val ignoredAnnotationsStore = store("./out/ignored-annotations")
  val failedToParseAnnotationsStore = store("./out/bad-annotations")
  val badBedRowsStore = store("./out/bad-bed-rows")
  val missingBedStore = store("./out/missing-bed-files")
    
  val uploadableAnnotations = annotations.
    filter(Dga.Annotations.Predicates.succeeded(failedToParseAnnotationsStore, append = true)).
    collect { case Success(a) => a }.
    filter(Dga.Annotations.Predicates.isUploadable(ignoredAnnotationsStore, append = true)).
    filter(
        Dga.Annotations.Predicates.hasAnnotationType(annotationTypesToIngest, ignoredAnnotationsStore, append = true))
    //Skip this last filter until getting file sizes of remote .beds works again.
    //filter(Dga.Annotations.Predicates.hasAnyBeds(logTo = missingBedStore, append = true, auth = Some(auth))).
  
  val parallelism: Int = System.getProperty("DGA_INTAKE_PARALLELISM", "1").toInt
  
  val tool: Tool = doLocally {
    val firstN = uploadableAnnotations.records.sliding(parallelism, parallelism).foreach { annotations =>
      import scala.concurrent.ExecutionContext.Implicits.global
      
      val fs = for {
        annotation <- annotations
      } yield {
        Future {
          Dga.Annotations.uploadAnnotatedDataset(
            Some(auth), 
            s3Client, 
            badBedRowsStore, 
            append = true)(annotation)
        }
      }
      
      scala.concurrent.Await.result(Future.sequence(fs), scala.concurrent.duration.Duration.Inf)
    }
  }
  
  addToGraph(tool)
  
  private def doLocally[A](body: => A)(implicit scriptContext: LoamScriptContext): NativeTool = {
    local {
      NativeTool {
        body
      }
    }
  }
}
