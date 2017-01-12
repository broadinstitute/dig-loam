package loamstream.apps

import com.typesafe.config.Config
import loamstream.db.slick.DbDescriptor
import loamstream.db.LoamDao
import loamstream.model.execute.ChunkRunner
import loamstream.model.execute.Executer
import loamstream.cli.Conf
import com.typesafe.config.ConfigFactory
import loamstream.db.slick.SlickLoamDao
import loamstream.db.slick.DbType
import loamstream.uger.UgerChunkRunner
import loamstream.conf.UgerConfig
import loamstream.util.Loggable
import loamstream.uger.Poller
import loamstream.util.RxSchedulers
import loamstream.uger.JobMonitor
import loamstream.model.execute.RxExecuter
import loamstream.model.execute.Executable
import scala.concurrent.duration.Duration
import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobState
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.JobFilter
import loamstream.util.Terminable
import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.model.execute.CompositeChunkRunner
import loamstream.util.ExecutionContexts
import loamstream.googlecloud.GoogleCloudChunkRunner
import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.CloudSdkDataProcClient
import loamstream.util.Throwables

/**
 * @author clint
 * Nov 10, 2016
 */
trait AppWiring extends Terminable {
  def dao: LoamDao

  def executer: Executer

  private[AppWiring] def makeJobFilter(conf: Conf): JobFilter = {
    if (conf.runEverything()) JobFilter.RunEverything else new DbBackedJobFilter(dao)
  }
}

object AppWiring extends TypesafeConfigHelpers with DrmaaClientHelpers with Loggable {

  def apply(cli: Conf): AppWiring = new AppWiring with DefaultDb {
    override def executer: Executer = terminableExecuter

    override def stop(): Unit = terminableExecuter.stop()

    private val terminableExecuter = {
      info("Creating executer...")

      val jobFilter = makeJobFilter(cli)

      val threadPoolSize = 50
      
      //TODO: Make the number of threads this uses configurable
      val numberOfCPUs = Runtime.getRuntime.availableProcessors
      
      val (localEC, localEcHandle) = ExecutionContexts.threadPool(numberOfCPUs)
      
      val localRunner = AsyncLocalChunkRunner()(localEC)

      val (ugerRunner, ugerRunnerHandles) = ugerChunkRunner(cli, threadPoolSize)

      val googleRunner = googleChunkRunner(cli, localRunner)
      
      val compositeRunner = CompositeChunkRunner(localRunner +: (ugerRunner.toSeq ++ googleRunner))

      import loamstream.model.execute.ExecuterHelpers._
      import ExecutionContexts.threadPool

      val (executionContextWithThreadPool, threadPoolHandle) = threadPool(threadPoolSize)

      import scala.concurrent.duration._
      
      val windowLength = 30.seconds
      
      val rxExecuter = RxExecuter(compositeRunner, windowLength, jobFilter)(executionContextWithThreadPool)

      val handles: Seq[Terminable] = threadPoolHandle +: localEcHandle +: (ugerRunnerHandles ++ googleRunner)

      new TerminableExecuter(rxExecuter, handles: _*)
    }
  }
  
  private def googleChunkRunner(cli: Conf, delegate: ChunkRunner): Option[GoogleCloudChunkRunner] = {
    val config = loadConfig(cli)

    val attempt = for {
      googleConfig <- GoogleCloudConfig.fromConfig(config)
      client <- CloudSdkDataProcClient.fromConfig(googleConfig)
    } yield {
      info("Creating Google Cloud ChunkRunner...")
      
      GoogleCloudChunkRunner(client, delegate)
    }
    
    val result = attempt.toOption
    
    //TODO: A better way to enable or disable Google support; for now, this is purely expedient
    if(result.isEmpty) {
      val msg = s"""Google Cloud support NOT enabled because ${attempt.failed.get.getMessage}
                   |in the config file (${cli.conf.toOption})""".stripMargin
        
      info(msg)
    }
    
    result
  }
  
  private def ugerChunkRunner(cli: Conf, threadPoolSize: Int): (Option[UgerChunkRunner], Seq[Terminable]) = {
    val result @ (ugerRunnerOption, _) = unpack(makeUgerChunkRunner(cli, threadPoolSize))

    //TODO: A better way to enable or disable Uger support; for now, this is purely expedient
    if(ugerRunnerOption.isEmpty) {
      val msg = s"""Uger support is NOT enabled. It can be enabled by defining loamstream.uger section
                   |in the config file (${cli.conf.toOption}).""".stripMargin
        
      info(msg)
    }
    
    result
  }
  
  private def unpack[A,B](o: Option[(A, Seq[B])]): (Option[A], Seq[B]) = o match {
    case Some((a, b)) => (Some(a), b)
    case None => (None, Nil)
  }

  private def makeUgerChunkRunner(cli: Conf, threadPoolSize: Int): Option[(UgerChunkRunner, Seq[Terminable])] = {
    debug("Parsing Uger config...")

    val config = loadConfig(cli)

    val ugerConfigAttempt = UgerConfig.fromConfig(config)

    for {
      ugerConfig <- ugerConfigAttempt.toOption
    } yield {
      info("Creating Uger ChunkRunner...")

      val drmaaClient = makeDrmaaClient

      import loamstream.model.execute.ExecuterHelpers._

      val threadPoolSize = 50

      val pollingFrequencyInHz = 0.1

      val poller = Poller.drmaa(drmaaClient)

      val (scheduler, schedulerHandle) = RxSchedulers.backedByThreadPool(threadPoolSize)

      val ugerRunner = {
        val jobMonitor = new JobMonitor(scheduler, poller, pollingFrequencyInHz)

        UgerChunkRunner(ugerConfig, drmaaClient, jobMonitor, pollingFrequencyInHz)
      }

      val handles = Seq(drmaaClient, schedulerHandle, ugerRunner)

      (ugerRunner, handles)
    }
  }

  private def loadConfig(cli: Conf): Config = {
    def defaults: Config = ConfigFactory.load()

    cli.conf.toOption match {
      case Some(confFile) => configFromFile(confFile).withFallback(defaults)
      case None           => defaults
    }
  }

  private trait DefaultDb { self: AppWiring =>
    override lazy val dao: LoamDao = {
      val dbDescriptor = DbDescriptor(DbType.H2, "jdbc:h2:./.loamstream/db")

      val dao = new SlickLoamDao(dbDescriptor)

      dao.createTables()

      dao
    }
  }
  private[apps] final class TerminableExecuter(
      val delegate: Executer,
      toStop: Terminable*) extends Executer with Terminable {

    override def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, JobState] = {
      delegate.execute(executable)(timeout)
    }

    final override def stop(): Unit = {
      import Throwables._
      
      for {
        terminable <- toStop
      } {
        quietly("Error shutting down: ") {
          terminable.stop()
        }
      }
    }
  }
}