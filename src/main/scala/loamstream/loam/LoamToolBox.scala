package loamstream.loam

import java.nio.file.{Path, Paths}

import loamstream.loam.files.LoamFileManager
import loamstream.model.Tool
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.{LJob, LToolBox}
import loamstream.util.{Hit, Miss, Shot, Snag}
import loamstream.loam.LoamGraph.StoreEdge.PathEdge
import loamstream.loam.LoamGraph.StoreEdge.ToolEdge
import loamstream.model.jobs.Output

/**
 * LoamStream
 * Created by oliverr on 6/21/2016.
 */
final class LoamToolBox extends LToolBox {

  private val fileManager = new LoamFileManager

  @volatile private[this] var loamJobs: Map[LoamTool, LJob] = Map.empty

  private[this] val lock = new AnyRef

  private[loam] def newLoamJob(tool: LoamTool): Shot[LJob] = {
    val graph = tool.graphBox.value

    val commandLineString = graph.toolTokens(tool).map(_.toString(fileManager)).mkString

    def pathOutputsFor(tool: LoamTool): Set[Output] = {
      val loamStores: Set[LoamStore] = graph.toolOutputs(tool) 
      
      def pathFor(loamStore: LoamStore): Option[Output] = {
        graph.storeSources(loamStore) match {
          case PathEdge(path) => Some(Output.PathOutput(path))
          /*case ToolEdge(tool) ???*/
          case _ => None
        }
      }
      
      loamStores.flatMap(pathFor)
    }
    
    val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))

    val shotsForPrecedingTools: Shot[Set[LJob]] = Shot.sequence(graph.toolsPreceding(tool).map(getLoamJob))
    
    shotsForPrecedingTools.map { inputJobs =>
      CommandLineStringJob(commandLineString, workDir, inputJobs, pathOutputsFor(tool))
    }
  }

  private[loam] def getLoamJob(tool: LoamTool): Shot[LJob] = lock.synchronized {
    loamJobs.get(tool) match {
      case Some(job) => Hit(job)
      case _ => newLoamJob(tool) match {
        case jobHit @ Hit(job) =>
          loamJobs += tool -> job
          jobHit
        case miss: Miss => miss
      }
    }
  }

  override def toolToJobShot(tool: Tool): Shot[LJob] = tool match {
    case loamTool: LoamTool => getLoamJob(loamTool)
    case _                  => Miss(Snag(s"LoamToolBox only knows Loam tools; it doesn't know about $tool."))
  }

}
