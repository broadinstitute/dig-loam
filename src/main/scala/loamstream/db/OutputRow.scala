package loamstream.db

import java.nio.file.Path
import java.time.Instant
import loamstream.util.Hash

/**
 * @author clint
 * date: Aug 11, 2016
 */

//TODO: Merge with jobs.Output??
final case class OutputRow(path: Path, lastModified: Instant, hash: Hash)