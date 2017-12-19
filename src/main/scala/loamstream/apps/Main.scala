package loamstream.apps

import scala.util.Failure
import scala.util.Success

import org.ggf.drmaa.DrmaaException

import loamstream.cli.Conf
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamProject
import loamstream.loam.LoamScript
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.util.OneTimeLatch
import loamstream.util.Versions
import loamstream.cli.ExecutionInfo


/**
 * @author clint
 * Nov 10, 2016
 */
object Main extends Loggable {
  def main(args: Array[String]): Unit = {
    
    addUncaughtExceptionHandler()
    
    val cli = Conf(args)

    describeLoamstream()
    
    if (cli.dryRun.isSupplied) {
      compile(cli)
    } else if(cli.lookup.isSupplied) {
      doLookup(cli)
    } else {
      run(cli)
    }
  }
  
  private def addUncaughtExceptionHandler(): Unit = {
    val handler: Thread.UncaughtExceptionHandler = new Thread.UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        error(s"[${t.getName}] Fatal uncaught exception; will trigger shutdown: ", e)

        e.printStackTrace(System.err)
      }
    }
    
    Thread.setDefaultUncaughtExceptionHandler(handler)
    
    Thread.currentThread.setUncaughtExceptionHandler(handler)
  }

  private def addShutdownHook(wiring: AppWiring): Unit = {
    def toThread(block: => Any): Thread = new Thread(new Runnable { override def run: Unit = block })
    
    Runtime.getRuntime.addShutdownHook(toThread {
      shutdown(wiring)
    })
  }

  private[apps] def run(cli: Conf): Unit = {
    val wiring = AppWiring(cli)
    
    addShutdownHook(wiring)

    val loamEngine = {
      val loamCompiler = LoamCompiler.default
      
      LoamEngine(wiring.config, loamCompiler, wiring.executer, wiring.cloudStorageClient)
    }

    try {
      val project = LoamProject(loamEngine.config, loamScripts)

      //TODO: Move to AppWiring?
      val runner = LoamRunner(loamEngine, prj => compile(loamEngine, prj), shutdownAfter(wiring))
      
      val jobsToExecutions = runner.run(project)
      
      listResults(jobsToExecutions)
      
      describeExecutions(jobsToExecutions.values)
    } catch {
      case e: DrmaaException => warn(s"Unexpected DRMAA exception: ${e.getClass.getName}", e)
    }

    def loamScripts: Iterable[LoamScript] = {
      val loamFiles = cli.loams()
      val loamScriptsShot = loamEngine.scriptsFrom(loamFiles)
      assert(loamScriptsShot.isHit, "Could not load loam scripts")

      loamScriptsShot.get
    }
  }

  private def shutdownAfter[A](execution: AppWiring)(f: => A): A = try { f } finally { shutdown(execution) }

  private def describeLoamstream(): Unit = Versions.load match {
    case Success(versions) => info(versions.toString)
    case Failure(e) => warn("Unable to determine version info: ", e)
  }

  private def listResults(jobsToExecutions: Map[LJob, Execution]): Unit = {
    //NB: Order (LJob, Execution) tuples based on the Executions' start times (if any).
    //If no start time is present (for jobs where Resources couldn't be - or weren't - 
    //determined, like Skipped jobs, those jobs/Executions come first. 
    def ordering(a: (LJob, Execution), b: (LJob, Execution)): Boolean = {
      val (_, executionA) = a
      val (_, executionB) = b
      
      (executionA.resources, executionB.resources) match {
        case (Some(resourcesA), Some(resourcesB)) => {
          resourcesA.startTime.toEpochMilli < resourcesB.startTime.toEpochMilli
        }
        case (_, None) => false
        case _ => true
      }
    }
    
    for {
      (job, execution) <- jobsToExecutions.toSeq.sortWith(ordering)
    } {
      info(s"${execution.status}\t(${execution.result}):\tRan $job got $execution")
    }
  }
  
  private def describeExecutions(executions: Iterable[Execution]): Unit = {
    def isSkipped(e: Execution) = e.status.isSkipped
    def isCouldNotStart(e: Execution) = e.status.isCouldNotStart
    def neitherSuccessNorFailure(e: Execution) = !e.isSuccess && !e.isFailure
      
    val numSucceeded = executions.count(_.isSuccess)
    val numFailed = executions.count(_.isFailure)
    val numSkipped = executions.count(isSkipped)
    val numCouldNotStart = executions.count(isCouldNotStart)
    val numOther = executions.count(neitherSuccessNorFailure)
    val numRan = executions.size
    
    val message = {
      s"$numRan jobs ran. $numSucceeded succeeded, $numFailed failed, $numSkipped skipped, " + 
      s"$numCouldNotStart could not start, $numOther other."
    }
    
    info(message)
  }
  
  private[this] val shutdownLatch: OneTimeLatch = new OneTimeLatch

  private[apps] def shutdown(wiring: AppWiring): Unit = {
    shutdownLatch.doOnce {
      info("LoamStream shutting down...")
      
      wiring.shutdown() match {
        case Nil => info("LoamStream shut down successfully")
        case exceptions => {
          error(s"LoamStream shut down with ${exceptions.size} errors: ")

          exceptions.foreach { e =>
            error(s"Error shutting down: ${e.getClass.getName}", e)
          }
        }
      }
    }
  }

  private def contextFrom(compilationResult: LoamCompiler.Result) = {
    assert(compilationResult.contextOpt.nonEmpty, "Loam compilation results do not have context.")

    compilationResult.contextOpt.get
  }

  private def compile(loamEngine: LoamEngine, project: LoamProject): LoamCompiler.Result = {
    
    info(s"Now compiling project with ${project.scripts.size} scripts.")

    val compilationResult = loamEngine.compile(project)
    
    assert(compilationResult.isValid, "Loam compilation result is not valid.")
    
    info(compilationResult.summary)

    compilationResult
  }

  private def compile(cli: Conf): Unit = {
    val wiring = AppWiring(cli)

    val loamEngine = LoamEngine.default(wiring.config)

    val compilationResultShot = loamEngine.compileFiles(cli.loams())

    assert(compilationResultShot.nonEmpty, compilationResultShot.message)

    val compilationResult = compilationResultShot.get

    info(compilationResult.report)
  }
  
  private def doLookup(cli: Conf): Unit = {
    val wiring = AppWiring(cli)
    
    val outputPathOrUri = cli.lookup()
    
    val descriptionOpt = ExecutionInfo.forOutput(wiring.dao)(outputPathOrUri)
    
    def outputAsString: String = outputPathOrUri.fold(_.toString, _.toString)
    
    info(descriptionOpt.getOrElse(s"No records found for $outputAsString"))
  }
}
