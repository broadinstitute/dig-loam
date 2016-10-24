package loamstream.loam

import java.nio.file.{Path, Paths, Files => JFiles}

import loamstream.compiler.{LoamEngine, LoamPredef}
import loamstream.util.code.SourceUtils.AnyToStringLiteral
import loamstream.util.{Files, PathUtils}
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 10/17/2016.
  */
class LoamWorkDirTest extends FunSuite {

  def assertPathsEquivalent(path1: Path, path2: Path): Unit = assert(path1.normalize() === path2.normalize())

  def assertPathsEquivalent(path1: Path, path2: String): Unit = assertPathsEquivalent(path1, Paths.get(path2))

  def assertWorkDirIsSet(workDirOpt1: Option[Path], workDirOpt2: Option[Path]): Unit = {
    import LoamCmdTool._
    import LoamPredef._
    implicit val scriptContext = new LoamScriptContext(LoamProjectContext.empty)
    workDirOpt1 match {
      case Some(workDir1) => changeDir(workDir1)
      case None => ()
    }
    workDirOpt2 match {
      case Some(workDir2) => changeDir(workDir2)
      case None => ()
    }
    val fileName1 = "file1.vcf"
    val fileName2 = "file2.vcf"
    val store1 = store[VCF].from(fileName1)
    val store2 = store[VCF].from(fileName2)
    val tool = cmd"yo $store1 $store2"
    val workDir = (workDirOpt1, workDirOpt2) match {
      case (Some(workDir1), Some(workDir2)) => workDir1.resolve(workDir2)
      case (Some(workDir1), None) => workDir1
      case (None, Some(workDir2)) => workDir2
      case (None, None) => Paths.get(".")
    }
    assertPathsEquivalent(store1.path, workDir.resolve(fileName1))
    assertPathsEquivalent(store2.path, workDir.resolve(fileName2))
    assertPathsEquivalent(tool.workDirOpt.get, workDir)
  }

  test("Check work dirs and file paths are correctly set in graph") {
    val workDirs = Seq(Paths.get("."), PathUtils.newRelative("a", "b", "c"), PathUtils.newAbsolute("a", "b", "c"))
    val workDirOpts = workDirs.map(Option(_)) :+ None
    for (workDirOpt1 <- workDirOpts) {
      for (workDirOpt2 <- workDirOpts) {
        assertWorkDirIsSet(workDirOpt1, workDirOpt2)
      }
    }
  }

  private case class FilePaths(workDir1: Path, fileName1: String, fileName2: String) {
    val filePath1 = workDir1.resolve(fileName1)
    val filePath2 = workDir1.resolve(fileName2)
  }

  private def createFilePaths: FilePaths =
    FilePaths(
      workDir1 = JFiles.createTempDirectory("LoamWorkDirTest"),
      fileName1 = "file1.txt",
      fileName2 = "file2.txt"
    )

  private def createInputFiles(paths: FilePaths): Unit = {
    Files.writeTo(paths.filePath1)("Yo!")
  }

  private def createScript(paths: FilePaths): LoamScript = {
    val code =
      s"""
         |changeDir(${paths.workDir1.asStringLiteral})
         |val file1 = store[TXT].from(${paths.fileName1.asStringLiteral})
         |val file2 = store[TXT].from(${paths.fileName2.asStringLiteral})
         |cmd"cp $$file1 $$file2"
      """.stripMargin
    val scriptName = "LoamWorkDirTestScript"
    println(code)
    LoamScript(scriptName, code)
  }

  private def assertOutputFileExists(path: Path): Unit =
    assert(JFiles.exists(path), s"Output file $path does not exist!")


  private def assertOutputFilesExist(paths: FilePaths): Unit = {
    assertOutputFileExists(paths.filePath2)
  }

  test("Run example with changing work directory") {
    val filePaths = createFilePaths
    createInputFiles(filePaths)
    val engine = LoamEngine.default()
    val script = createScript(filePaths)
    val results = engine.run(script)
    assert(results.jobResultsOpt.nonEmpty, results.compileResultOpt)
    assertOutputFilesExist(filePaths)
  }
}

