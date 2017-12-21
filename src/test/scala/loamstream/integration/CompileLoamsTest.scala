package loamstream.integration

import org.scalatest.FunSuite
import java.nio.file.Paths
import java.nio.file.Path
import java.nio.file.Files
import loamstream.LoamTestHelpers
import loamstream.compiler.LoamProject
import loamstream.loam.LoamScript
import loamstream.util.Shot
import loamstream.TestHelpers
import loamstream.util.Loggable
import loamstream.util.ConfigUtils
import loamstream.conf.LoamConfig
import loamstream.compiler.LoamCompiler
import loamstream.loam.LoamGraph
import loamstream.compiler.LoamEngine
import loamstream.util.TimeUtils
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.model.execute.Executable
import scala.util.Try
import scala.concurrent.Await

/**
 * @author clint
 * Dec 21, 2016
 */
final class CompileLoamsTest extends FunSuite with LoamTestHelpers with Loggable {
  import TestHelpers.path
  
  private val loamDir: Path = path("pipeline/loam/")
  
  test(s"Compile all files in $loamDir") {
    assert(results.isSuccess)
  }
  
  test(s"Making jobs from all files in $loamDir shouldn't take too long") {
    assert(results.isSuccess)
    
    //Getting the first graph chunk is fine for our purposes
    val graph: LoamGraph = results.graphSource.iterator.next().apply()
    
    import ExecutionContext.Implicits.global
    
    val f: Future[(Try[Executable], Long)] = Future {
      TimeUtils.elapsed {
        LoamEngine.toExecutable(graph)
      }
    }
    
    import scala.concurrent.duration._
    
    //A hard-coded time like this is a bit fraught, but it will stop builds from hanging forever.
    //Hopefully 10 minutes is enough even for a possibly-highly-loaded CI VM.
    val maxWaitTime = 10.minutes
    
    val (executableAttempt, elapsed) = Await.result(f, maxWaitTime)
    
    info(s"Making jobs from all files in $loamDir took $elapsed milliseconds")
    
    assert(executableAttempt.isSuccess)
  }
  
  private lazy val results: LoamCompiler.Result = {
    withSysProps(
        "dataConfig" -> "pipeline/conf/METSIM/METSIM.conf",,
        "pipelineConfig" -> "pipeline/conf/qc.conf") {
      
      val loams: Set[Path] = {
        import scala.collection.JavaConverters._
        
        Files.list(loamDir).iterator.asScala.filter(_.toString.endsWith(".loam")).toSet
      }
      
      info(s"Compiling ${loams.size} files:")
      loams.toSeq.sortBy(_.toString).foreach(loam => info(s"  $loam"))
  
      val scripts: Set[LoamScript] = Shot.sequence(loams.map(LoamScript.read)).get    
  
      val config: LoamConfig = {
        val typesafeConfig = ConfigUtils.configFromFile(path("pipeline/conf/loamstream.conf"))
        
        LoamConfig.fromConfig(typesafeConfig).get
      }
      
      compile(LoamProject(config, scripts))
    }
  }
  
  private def withSysProps[A](props: (String, String)*)(f: => A): A = {
    val (oldValues, toRemove) = {
      val tuples = props.map { case (key, _) => key -> System.getProperty(key) }
      
      def hasNullValue(t: (String, String)): Boolean = t._2 == null //scalastyle:ignore null
      
      (tuples.filterNot(hasNullValue), tuples.collect { case t @ (k, _) if hasNullValue(t) => k })  
    }
    
    def applyValues(toRestore: Seq[(String, String)], toRemove: Seq[String] = Nil): Unit = {
      toRestore.foreach { case (key, newValue) => 
        System.setProperty(key, newValue)
      }
      
      toRemove.foreach(System.clearProperty)
    }
    
    try {
      applyValues(props)
      
      f
    } finally {
      applyValues(oldValues, toRemove)
    }
  }
}
