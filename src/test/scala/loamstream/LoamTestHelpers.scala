package loamstream

import java.nio.file.Path
import java.nio.file.Paths

import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamProject
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScript
import loamstream.loam.LoamToolBox
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.loam.ast.LoamGraphAstMapping
import loamstream.model.execute.Executable
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
import loamstream.util.Loggable


/**
  * @author clint
  *         date: Jul 8, 2016
  */
trait LoamTestHelpers extends Loggable {

  def compileFile(file: String): LoamCompiler.Result = compile(Paths.get(file))

  def compile(path: Path): LoamCompiler.Result = compile(LoamScript.read(path).get)

  def compile(script: LoamScript): LoamCompiler.Result = compile(LoamProject(Set(script)))
    
  def compile(project: LoamProject): LoamCompiler.Result = {

    val compiler = new LoamCompiler(LoamCompiler.Settings.default, LoggableOutMessageSink(this))

    val compileResults = compiler.compile(project)

    if (!compileResults.isValid) {
      throw new IllegalArgumentException(s"Could not compile '$project': ${compileResults.errors}.")
    }

    compileResults
  }

  def toExecutable(compileResults: LoamCompiler.Result): (LoamGraphAstMapping, Executable) = {
    val context: LoamProjectContext = compileResults.contextOpt.get
    val graph = context.graph

    val mapping = LoamGraphAstMapper.newMapping(graph)

    val toolBox = new LoamToolBox(context)

    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)

    (mapping, executable)
  }

  def run(executable: Executable): Map[LJob, JobState] = RxExecuter.default.execute(executable)
}
