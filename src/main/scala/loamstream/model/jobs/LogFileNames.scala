package loamstream.model.jobs

import java.nio.file.Path
import java.nio.file.Paths
import loamstream.util.PathEnrichments

/**
 * @author clint
 * Nov 13, 2017
 */
object LogFileNames {
  
  def outputStreams(job: LJob, outputDirName: Path): OutputStreams = {
    OutputStreams(stdout(job, outputDirName), stderr(job, outputDirName))
  }
  
  def stdout(job: LJob, outputDirName: Path): Path = makePath(job, "stdout", outputDirName)
  
  def stderr(job: LJob, outputDirName: Path): Path = makePath(job, "stderr", outputDirName)
  
  private def makePath(job: LJob, suffix: String, outputDir: Path): Path = {
    import PathEnrichments._
    
    (outputDir / s"${mungeSpecialChars(job.name)}.$suffix").toAbsolutePath
  }
  
  //NB: Basically anything path-separator-related
  private[this] val specialChars: Set[Char] = Set('/', ':', '\\')
  
  private def mungeSpecialChars(s: String): String = s.map {
    case ch if ch.isWhitespace || specialChars.contains(ch) => '_'
    case ch => ch      
  }
}
