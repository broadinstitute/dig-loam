package loamstream.model.jobs

import java.nio.file.Path
import java.time.Instant

import loamstream.util.{PathUtils, TimeUtils}
import java.net.URI

/**
 * @author kyuksel
 * Dec 12, 2016
 *
 * A container for job output attributes that are to be recorded and are not system-dependent
 * (e.g. in hash type or how resources are identified [URI/Path/etc])
 */
final case class OutputRecord(loc: String,
                              isPresent: Boolean,
                              hash: Option[String],
                              hashType: Option[String],
                              lastModified: Option[Instant]) {

  def isMissing: Boolean = !isPresent

  def isOlderThan(other: OutputRecord): Boolean = {
    import TimeUtils.Implicits._

    lastModified match {
      case Some(timestamp) => other.lastModified match {
        case Some(otherTimestamp) => timestamp < otherTimestamp
        case None => false
      }
      case None => false
    }
  }

  def isHashed: Boolean = hash.isDefined

  def hasDifferentHashThan(other: OutputRecord): Boolean = (
    for {
      hashValue <- hash
      hashKind <- hashType
      otherHashValue <- other.hash
      otherHashKind <- other.hashType
    } yield hashKind != otherHashKind || hashValue != otherHashValue
    ).getOrElse(false)

  def withLastModified(t: Instant) = copy(lastModified = Option(t))

  override def toString: String = s"$loc"
}

object OutputRecord {
  def apply(loc: String): OutputRecord = OutputRecord(loc,
                                                      isPresent = false,
                                                      hash = None,
                                                      hashType = None,
                                                      lastModified = None)
                                                      
  def apply(path: Path): OutputRecord = OutputRecord(PathUtils.normalize(path))
  
  def apply(uri: URI): OutputRecord = OutputRecord(uri.toString)

  def apply(loc: String,
            hash: Option[String],
            hashType: Option[String],
            lastModified: Option[Instant]): OutputRecord = OutputRecord(loc,
                                                                        isPresent = lastModified.isDefined,
                                                                        hash = hash,
                                                                        hashType = hashType,
                                                                        lastModified = lastModified)

  

  def apply(output: Output): OutputRecord = output.lastModified match {
    case lmOpt @ Some(_) => OutputRecord( loc = output.location,
                                          isPresent = true,
                                          hash = output.hash.map(_.valueAsBase64String),
                                          hashType = output.hashType.map(_.algorithmName),
                                          lastModified = lmOpt)
    case _ => OutputRecord( loc = output.location,
                            isPresent = false,
                            hash = None,
                            hashType = None,
                            lastModified = None)
  }
}
