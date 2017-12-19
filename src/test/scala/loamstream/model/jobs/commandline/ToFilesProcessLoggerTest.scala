package loamstream.model.jobs.commandline

import scala.sys.process.ProcessLogger
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.util.PathEnrichments
import loamstream.util.Files
import loamstream.util.CanBeClosed
import java.nio.file.Path


/**
 * @author clint
 * Nov 15, 2017
 */
final class ToFilesProcessLoggerTest extends FunSuite {
  import PathEnrichments._
  
  test("Nothing written to stdout, that file shouldn't get made") {
    val (stdoutPath, stderrPath) = withLogger { logger =>
      logger.err("foo ")
      logger.err("bar ")
      logger.err("baz")
    }
    
    assert(stdoutPath.toFile.exists === false)
    assert(stderrPath.toFile.exists === true)
    
    val expected = "foo bar baz"
                      
    assert(Files.readFrom(stderrPath) === expected)
  }
  
  test("Nothing written to stderr, that file shouldn't get made") {
    val (stdoutPath, stderrPath) = withLogger { logger =>
      logger.out("foo ")
      logger.out("bar ")
      logger.out("baz")
    }
    
    assert(stdoutPath.toFile.exists === true)
    assert(stderrPath.toFile.exists === false)
    
    val expected = "foo bar baz"
                      
    assert(Files.readFrom(stdoutPath) === expected)
  }
  
  test("Nothing written to stderr OR stderr, no files should get made") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val stdoutPath = workDir / "stdout"
    val stderrPath = workDir / "stderr"
    
    val logger = ToFilesProcessLogger(stdoutPath, stderrPath)
    
    logger.stop()
    
    assert(stdoutPath.toFile.exists === false)
    assert(stderrPath.toFile.exists === false)
  }
  
  test("Lines written to stdout and stderr") {
    val (stdoutPath, stderrPath) = withLogger { logger =>
      logger.out("foo")
      logger.out(" bar ")
      logger.out("baz")
      
      logger.err("nuh ")
      logger.err("zuh")
      logger.err(" blerg")
    }
    
    assert(stdoutPath.toFile.exists === true)
    assert(stderrPath.toFile.exists === true)
    
    val expectedOut = "foo bar baz"
                         
    val expectedErr = "nuh zuh blerg"
    
    assert(Files.readFrom(stdoutPath) === expectedOut)
    assert(Files.readFrom(stderrPath) === expectedErr)
  }
  
  private def withLogger(f: ToFilesProcessLogger => Any): (Path, Path) = {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val stdoutPath = workDir / "stdout"
    val stderrPath = workDir / "stderr"
    
    import CanBeClosed.{enclosed => stopAfter }
    
    stopAfter(ToFilesProcessLogger(stdoutPath, stderrPath))(f)
    
    (stdoutPath, stderrPath)
  }
}
