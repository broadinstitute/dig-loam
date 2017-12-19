package loamstream.cli

import java.io.File
import java.nio.file.Paths

import org.scalatest.FunSuite
import org.scalatest.Matchers
import java.net.URI

/**
 * Created by kyuksel on 10/12/16.
 */
final class ConfTest extends FunSuite with Matchers {
  private val testConfigFile = "src/test/resources/loamstream-test.conf".replace("/", File.separator)

  private def makeConf(args: Seq[String], exitTheJvmOnValidationError: Boolean = false): Conf = {
    Conf(args, exitTheJvmOnValidationError)
  }
  
  test("Single loam file along with conf file is parsed correctly") {
    val conf = makeConf(Seq("--conf", "src/test/resources/loamstream-test.conf", 
                            "src/test/resources/a.txt"))

    conf.loams() shouldEqual List(Paths.get("src/test/resources/a.txt"))
    conf.conf().toString shouldEqual testConfigFile
  }

  test("Multiple loam files along with conf file are parsed correctly") {
    val conf = makeConf(Seq("--conf", "src/test/resources/loamstream-test.conf",
                            "src/test/resources/a.txt", "src/test/resources/a.txt"))

    conf.loams() shouldEqual List(Paths.get("src/test/resources/a.txt"), Paths.get("src/test/resources/a.txt"))
    conf.conf().toString shouldEqual testConfigFile
  }
  
  test("Test that we try to exit if passed no args") {
    intercept[CliException] {
      Conf(Seq.empty, exitTheJvmOnValidationError = false)
    }
  }
  
  test("Test that we try to exit if we're passed just --version") {
    try {
      Conf(Seq("--version"), exitTheJvmOnValidationError = false)
      fail()
    } catch {
      case CliException(message) => assert(message === "version") 
    }
  }
   
  test("Test that we try to exit if passed nonexistent file names") {
    intercept[CliException] {
      val args = Seq("--conf", "src/test/resources/loamstream-test.conf",
                     "--backend", "local",
                     "asdfasdf.txt", "src/test/resources/a.txt")
      
      Conf(args, exitTheJvmOnValidationError = false)
    }
    
    intercept[CliException] {
      val args = Seq("--conf", "asdfasdf.txt", "--backend", "local", "src/test/resources/a.txt")
      
      Conf(args, exitTheJvmOnValidationError = false)
    }
  }
  
  test("--dry-run") {
    def doTest(flag: String, loams: Seq[String]): Unit = {
      val args = flag +: loams
      
      val conf = makeConf(args)
      
      assert(conf.dryRun.isSupplied)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.loams() === loams.map(Paths.get(_)))
    }
    
    doTest("--dry-run", Seq("src/examples/loam/cp.loam", "src/examples/loam/cp.loam"))
    doTest("-d", Seq("src/examples/loam/cp.loam", "src/examples/loam/cp.loam"))
  }
  
  test("--disable-hashing") {
    {
      val args = Seq("--disable-hashing", "--dry-run", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied)
      assert(conf.dryRun.isSupplied)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.loams() === Seq(Paths.get("src/examples/loam/cp.loam")))
    }
    
    {
      val args = Seq("--dry-run", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.dryRun.isSupplied)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.loams() === Seq(Paths.get("src/examples/loam/cp.loam")))
    }
  }
  
  test("Loam files must be specified if running normally or with --dry-run") {
    //No loam files
    intercept[CliException] {
      makeConf(Seq("--conf", "src/main/loam/cp.loam src/main/loam/cp.loam"))
    }
    
    //No loam files
    intercept[CliException] {
      makeConf(Seq("--conf", "src/main/loam/cp.loam src/main/loam/cp.loam", "--dry-run"))
    }
    
    //No loam files
    intercept[CliException] {
      makeConf(Nil)
    }
    
    //No loam files
    intercept[CliException] {
      makeConf(Seq("--dry-run"))
    }
    
    val exampleFile = "src/examples/loam/cp.loam"
    
    val expected = Paths.get(exampleFile)
    
    //Just a loam file
    {
      val conf = makeConf(Seq(exampleFile))
      
      conf.loams() shouldEqual List(expected)
      conf.dryRun() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
      conf.runEverything.isSupplied shouldBe(false)
    }
    
    {
      val conf = makeConf(Seq(exampleFile, exampleFile))
      
      conf.loams() shouldEqual List(expected, expected)
      conf.dryRun() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
      conf.runEverything.isSupplied shouldBe(false)
    }
    
    {
      val conf = makeConf(Seq("--dry-run", exampleFile, exampleFile))
      
      conf.loams() shouldEqual List(expected, expected)
      conf.dryRun() shouldBe(true)
      conf.conf.isSupplied shouldBe(false)
      conf.runEverything.isSupplied shouldBe(false)
    }
  }
 
  test("--run-everything") {
    val exampleFile = "src/examples/loam/cp.loam"
    
    val expected = Paths.get(exampleFile)
    
    {
      val conf = makeConf(Seq("--run-everything", exampleFile))
        
      conf.runEverything() shouldBe(true)
      conf.loams() shouldEqual List(expected)
      conf.dryRun() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
    
    {
      val conf = makeConf(Seq("-r", exampleFile))
        
      conf.runEverything() shouldBe(true)
      conf.loams() shouldEqual List(expected)
      conf.dryRun() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
    
    {
      val conf = makeConf(Seq(exampleFile))
        
      conf.runEverything() shouldBe(false)
      conf.loams() shouldEqual List(expected)
      conf.dryRun() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
  }
  
  test("--lookup") {
    val someFile = "some/arbitrary/output"
    val someUri = "gs://foo/bar/baz"
    
    val expectedPath = Paths.get(someFile)
    val expectedUri = URI.create(someUri)
    
    //Path, full arg name
    {
      val conf = makeConf(Seq("--lookup", someFile))
        
      conf.lookup.isSupplied shouldBe(true)
      conf.lookup() shouldBe(Left(expectedPath))
      
      conf.runEverything() shouldBe(false)
      conf.loams.isSupplied shouldBe(false)
      conf.dryRun() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
    //Path, short arg name
    {
      val conf = makeConf(Seq("-l", someFile))
        
      conf.lookup.isSupplied shouldBe(true)
      conf.lookup() shouldBe(Left(expectedPath))
      
      conf.runEverything() shouldBe(false)
      conf.loams.isSupplied shouldBe(false)
      conf.dryRun() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
    
    //URI, full arg name
    {
      val conf = makeConf(Seq("--lookup", someUri))
        
      conf.lookup.isSupplied shouldBe(true)
      conf.lookup() shouldBe(Right(expectedUri))
      
      conf.runEverything() shouldBe(false)
      conf.loams.isSupplied shouldBe(false)
      conf.dryRun() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
    //URI, short arg name
    {
      val conf = makeConf(Seq("-l", someUri))
        
      conf.lookup.isSupplied shouldBe(true)
      conf.lookup() shouldBe(Right(expectedUri))
      
      conf.runEverything() shouldBe(false)
      conf.loams.isSupplied shouldBe(false)
      conf.dryRun() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
  }
}
