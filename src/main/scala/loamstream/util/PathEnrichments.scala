package loamstream.util

import java.nio.file.Path

/**
  * @author clint
  *         date: Jun 1, 2016
  */
object PathEnrichments {

  final implicit class PathHelpers(val path: Path) extends AnyVal {
    def /(next: String): Path = path.resolve(next)
  }

}