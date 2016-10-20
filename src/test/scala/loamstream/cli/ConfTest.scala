package loamstream.cli

import java.io.File
import java.nio.file.Paths

import org.scalatest.FunSuite
import org.scalatest.Matchers

/**
 * Created by kyuksel on 10/12/16.
 */
final class ConfTest extends FunSuite with Matchers {
  val testConfigFile = "src/test/resources/loamstream-test.conf".replace("/", File.separator)

  test("Single loam file along with conf file is parsed correctly") {
    val conf = Conf(Seq("--loam", "src/test/resources/a.txt",
                        "--conf", "src/test/resources/loamstream-test.conf"))

    conf.loam() shouldEqual List(Paths.get("src/test/resources/a.txt"))
    conf.conf().toString shouldEqual testConfigFile
  }

  test("Multiple loam files along with conf file are parsed correctly") {
    val conf = Conf(Seq("--loam", "src/test/resources/a.txt", "src/test/resources/a.txt",
                        "--conf", "src/test/resources/loamstream-test.conf"))

    conf.loam() shouldEqual List(Paths.get("src/test/resources/a.txt"), Paths.get("src/test/resources/a.txt"))
    conf.conf().toString shouldEqual testConfigFile
  }
  
  test("Test that we try to exit if passed no args") {
    intercept[CliException] {
      Conf(Seq.empty, exitTheJvmOnValidationError = false)
    }
  }
   
  test("Test that we try to exit if passed nonexistent file names") {
    intercept[CliException] {
      val args = Seq("--loam", "asdfasdf.txt", "src/test/resources/a.txt",
                     "--conf", "src/test/resources/loamstream-test.conf")
      
      Conf(args, exitTheJvmOnValidationError = false)
    }
    
    intercept[CliException] {
      val args = Seq("--loam", "src/test/resources/a.txt",
                     "--conf", "asdfasdf.txt")
      
      Conf(args, exitTheJvmOnValidationError = false)
    }
  }
}
