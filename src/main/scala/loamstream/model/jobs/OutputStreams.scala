package loamstream.model.jobs

import java.nio.file.Path

/**
 * @author clint
 * Dec 6, 2017
 */
final case class OutputStreams private (stdout: Path, stderr: Path)

object OutputStreams {
  def apply(stdout: Path, stderr: Path): OutputStreams = {
    new OutputStreams(stdout.toAbsolutePath, stderr.toAbsolutePath)
  }
}
