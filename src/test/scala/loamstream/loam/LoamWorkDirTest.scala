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

  private class FilePaths {
    val rootDirs: Seq[Path] = Seq.fill(2)(JFiles.createTempDirectory("LoamWorkDirTest"))
    val subDirName = "subDir"
    val subDirs: Seq[Path] = rootDirs.map(_.resolve(subDirName))
    val workDirs = rootDirs ++ subDirs
    val inFileName = "inFile.txt"
    val inFilePath = rootDirs.head.resolve(inFileName)
    val outFileNames: Seq[String] = (0 to 5).map(index => s"outFile$index.txt")
    val outFileDirs: Seq[Path] = Seq(rootDirs.head, subDirs.head, subDirs.head, rootDirs(1), subDirs(1), subDirs(1))
    val outFilePaths: Seq[Path] = outFileDirs.zip(outFileNames).map({ case (dir, name) => dir.resolve(name) })
  }

  private def createFilePaths: FilePaths = new FilePaths

  private def createInputFiles(paths: FilePaths): Unit = {
    Files.writeTo(paths.inFilePath)("Yo!")
    for (workDir <- paths.workDirs) {
      if (!JFiles.exists(workDir)) {
        JFiles.createDirectory(workDir)
      }
    }
  }

  // scalastyle:off magic.number
  private def createScriptUsingChangeDir(paths: FilePaths): LoamScript = {
    val code =
      s"""
         |changeDir(${paths.rootDirs.head.asStringLiteral})
         |val inFile = store[TXT].from(${paths.inFileName.asStringLiteral})
         |val outFile0 = store[TXT].to(${paths.outFileNames.head.asStringLiteral})
         |cmd"cp $$inFile $$outFile0"
         |changeDir(${paths.subDirName.asStringLiteral})
         |val outFile1 = store[TXT].to(${paths.outFileNames(1).asStringLiteral})
         |val outFile2 = store[TXT].to(${paths.outFileNames(2).asStringLiteral})
         |cmd"cp $$outFile0 $$outFile1"
         |cmd"cp $$outFile1 $$outFile2"
         |changeDir(${paths.rootDirs(1).asStringLiteral})
         |val outFile3 = store[TXT].to(${paths.outFileNames(3).asStringLiteral})
         |cmd"cp $$outFile2 $$outFile3"
         |changeDir(${paths.subDirName.asStringLiteral})
         |val outFile4 = store[TXT].to(${paths.outFileNames(4).asStringLiteral})
         |val outFile5 = store[TXT].to(${paths.outFileNames(5).asStringLiteral})
         |cmd"cp $$outFile3 $$outFile4"
         |cmd"cp $$outFile4 $$outFile5"
        """.stripMargin
    val scriptName = "LoamWorkDirTestScriptUsingChangeDir"
    LoamScript(scriptName, code)
  }

  // scalastyle:on magic.number

  // scalastyle:off magic.number
  private def createScriptUsingInDir(paths: FilePaths): LoamScript = {
    val code =
      s"""
         |val outFile2 = store[TXT]
         |inDir(${paths.rootDirs.head.asStringLiteral}) {
         |  val inFile = store[TXT].from(${paths.inFileName.asStringLiteral})
         |  val outFile0 = store[TXT].to(${paths.outFileNames.head.asStringLiteral})
         |  cmd"cp $$inFile $$outFile0"
         |  inDir(${paths.subDirName.asStringLiteral}) {
         |    val outFile1 = store[TXT].to(${paths.outFileNames(1).asStringLiteral})
         |    outFile2.to(${paths.outFileNames(2).asStringLiteral})
         |    cmd"cp $$outFile0 $$outFile1"
         |    cmd"cp $$outFile1 $$outFile2"
         |  }
         |}
         |inDir(${paths.rootDirs(1).asStringLiteral}) {
         |  val outFile3 = store[TXT].to(${paths.outFileNames(3).asStringLiteral})
         |  cmd"cp $$outFile2 $$outFile3"
         |  inDir(${paths.subDirName.asStringLiteral}) {
         |    val outFile4 = store[TXT].to(${paths.outFileNames(4).asStringLiteral})
         |    val outFile5 = store[TXT].to(${paths.outFileNames(5).asStringLiteral})
         |    cmd"cp $$outFile3 $$outFile4"
         |    cmd"cp $$outFile4 $$outFile5"
         |  }
         |}
        """.stripMargin
    val scriptName = "LoamWorkDirTestScriptUsingInDir"
    LoamScript(scriptName, code)
  }

  // scalastyle:on magic.number

  private def assertOutputFileExists(path: Path): Unit =
    assert(JFiles.exists(path), s"Output file $path does not exist!")


  private def assertOutputFilesExist(paths: FilePaths): Unit = {
    for (outFilePath <- paths.outFilePaths) {
      assertOutputFileExists(outFilePath)
    }
  }

  private def testScript(script: LoamScript, filePaths: FilePaths): Unit = {
    createInputFiles(filePaths)
    val engine = LoamEngine.default()
    val results = engine.run(script)
    assert(results.jobResultsOpt.nonEmpty, results.compileResultOpt)
    assertOutputFilesExist(filePaths)
  }

  test("Toy pipeline of cp using changeDir(Path)") {
    val filePaths = createFilePaths
    testScript(createScriptUsingChangeDir(filePaths), filePaths)
  }

  test("Toy pipeline of cp using inDir(Path) {...} ") {
    val filePaths = createFilePaths
    testScript(createScriptUsingInDir(filePaths), filePaths)
  }
}

