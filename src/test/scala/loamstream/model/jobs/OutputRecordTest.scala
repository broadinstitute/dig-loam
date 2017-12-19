package loamstream.model.jobs

import java.nio.file.{Path, Paths}
import java.time.Instant

import loamstream.model.jobs.Output.PathOutput
import loamstream.util.HashType.Sha1
import loamstream.util.{Hashes, PathUtils}
import org.scalatest.FunSuite

/**
 * @author kyuksel
 * date: Jan 3, 2017
 */
class OutputRecordTest extends FunSuite {
  private val fooLoc = normalize("src/test/resources/for-hashing/foo.txt")
  private val fooPath = Paths.get(fooLoc)
  private val fooHash = Hashes.sha1(fooPath).valueAsBase64String
  private val fooHashType = Sha1.algorithmName
  private val fooRec = OutputRecord(fooLoc, Option(fooHash), Option(fooHashType), lastModifiedOptOf(fooPath))

  private val fooPathCopy = fooPath
  private val fooHashCopy = Hashes.sha1(fooPathCopy).valueAsBase64String
  private val fooRecCopy = OutputRecord(fooLoc)

  private val emptyLoc = normalize("src/test/resources/for-hashing/empty.txt")
  private val emptyPath = Paths.get(emptyLoc)
  private val emptyHash = Hashes.sha1(emptyPath).valueAsBase64String
  private val emptyHashType = Sha1.algorithmName
  private val emptyRec = OutputRecord(emptyLoc, Option(emptyHash), Option(emptyHashType), lastModifiedOptOf(emptyPath))

  private val nonExistingLoc = normalize("non/existent/path")
  private val nonExistingRec = OutputRecord(nonExistingLoc)

  private def lastModifiedOptOf(p: Path): Option[Instant] = Option(PathUtils.lastModifiedTime(p))

  private def normalize(loc: String): String = PathUtils.normalize(Paths.get(loc))

  test("apply/isPresent/isMissing") {
    assert(fooRec.isPresent)

    assert(nonExistingRec.isMissing)

    val recFromFooOutput = OutputRecord(PathOutput(fooPath).normalized)
    assert(fooRec == recFromFooOutput)

    val expectedNonExistingRec = OutputRecord(nonExistingLoc, false, None, None, None)
    assert(nonExistingRec == expectedNonExistingRec)
  }

  test("hasDifferentHashThan") {
    assert(!fooRec.hasDifferentHashThan(fooRecCopy))
    assert(!fooRecCopy.hasDifferentHashThan(fooRec))
    assert(!fooRec.hasDifferentHashThan(fooRec))

    assert(fooRec.hasDifferentHashThan(emptyRec))
    assert(emptyRec.hasDifferentHashThan(fooRec))
  }

  test("isOlderThan/withLastModified") {
    assert(!fooRec.isOlderThan(fooRecCopy))
    assert(!fooRecCopy.isOlderThan(fooRec))
    assert(!fooRec.isOlderThan(nonExistingRec))
    assert(!nonExistingRec.isOlderThan(fooRec))

    val newRec = fooRec.withLastModified(Instant.now())
    
    assert(fooRec.isOlderThan(newRec))
    assert(!newRec.isOlderThan(fooRec))
  }

}
