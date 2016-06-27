package loamstream.compiler

import loamstream.compiler.ClientMessageHandler.OutMessageSink
import loamstream.compiler.repo.LoamRepository
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * LoamStream
  * Created by oliverr on 6/27/2016.
  */
final class LoamRepositoryTest extends FunSuite {
  test("Default repo is complete") {
    for (entry <- LoamRepository.defaultEntries) {
      val codeShot = LoamRepository.defaultRepo.get(entry)
      assert(codeShot.nonEmpty, codeShot.message)
    }
  }
  test("Default repo entries compile and yield valid graphs") {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)(global)
    val repo = LoamRepository.defaultRepo
    for (entry <- repo.list) {
      val codeShot = repo.get(entry)
      assert(codeShot.nonEmpty, codeShot.message)
      val code = codeShot.get
      val compileResult = compiler.compile(code)
      assert(compileResult.isSuccess, compileResult.summary)
      assert(compileResult.isClean, compileResult.report)
    }
  }
  test("LoamComboRepository") {

  }

}

