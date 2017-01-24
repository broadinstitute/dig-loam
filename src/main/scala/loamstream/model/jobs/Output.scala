package loamstream.model.jobs

import java.net.URI
import java.nio.file.Files
import java.nio.file.Path

import loamstream.util.{Hash, HashType, Hashes, PathUtils}
import java.time.Instant
import java.nio.file.Paths

import loamstream.googlecloud.CloudStorageClient
import loamstream.util.HashType.{Md5, Sha1}

/**
 * @author clint
 * @author kaan
 * Aug 1, 2016
 *
 * A trait representing a handle to the output of a job; for now, we're primarily concerned with the case where
 * that output is a file, directory, or Google Cloud Storage objects but other output types are possible.
 */
trait Output {
  def isPresent: Boolean
  
  final def isMissing: Boolean = !isPresent
  
  def hash: Option[Hash]

  def hashType: Option[HashType] = hash.map(_.tpe)

  def lastModified: Option[Instant]

  def location: String

  def toOutputRecord: OutputRecord
}

object Output {
  /**
   * A handle to an output of a job stored at 'path'.  Hashes and modification times are re-computed on
   * each access.
   */
  final case class PathOutput(path: Path) extends Output {
    override def isPresent: Boolean = Files.exists(path)

    override def hash: Option[Hash] = if (Files.exists(path)) Option(Hashes.sha1(path)) else None

    override def lastModified: Option[Instant] = {
      if (isPresent) Option(PathUtils.lastModifiedTime(path)) else None
    }

    override def location: String = PathUtils.normalize(path)

    override def toOutputRecord: OutputRecord =
      OutputRecord( loc = location,
                    isPresent = isPresent,
                    hash = hash.map(_.valueAsBase64String),
                    hashType = hashType.map(_.algorithmName),
                    lastModified = lastModified)

    def normalized: PathOutput = copy(path = normalize(path))

    private def normalize(p: Path): Path = Paths.get(PathUtils.normalize(p))
  }

  final case class GcsUriOutput(uri: URI, client: Option[CloudStorageClient]) extends Output {
    override def isPresent = client.exists(_.isPresent(uri))

    override def hash: Option[Hash] = client.flatMap(_.hash(uri))

    override def lastModified: Option[Instant] = client.flatMap(_.lastModified(uri))

    override def location: String = uri.toString

    override def toOutputRecord: OutputRecord =
      OutputRecord( loc = location,
                    isPresent = isPresent,
                    hash = hash.map(_.valueAsBase64String),
                    hashType = hashType.map(_.algorithmName),
                    lastModified = lastModified)
  }
}
