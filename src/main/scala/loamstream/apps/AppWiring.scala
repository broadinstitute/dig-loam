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
import loamstream.conf.{LoamConfig, PythonConfig, RConfig, UgerConfig}
import loamstream.util.Loggable
import loamstream.uger.Poller
import loamstream.util.RxSchedulers
import loamstream.uger.JobMonitor
import loamstream.model.execute.RxExecuter
import loamstream.model.execute.Executable

import scala.concurrent.duration.Duration
import loamstream.model.jobs.{Execution, LJob}
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.JobFilter
import loamstream.util.Terminable
import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.model.execute.CompositeChunkRunner
import loamstream.util.ExecutionContexts
import loamstream.googlecloud._
import loamstream.util.Throwables

import scala.util.Try
import loamstream.uger.AccountingClient
import loamstream.uger.Drmaa1Client
import loamstream.uger.UgerClient
import loamstream.conf.ExecutionConfig
import loamstream.util.ConfigUtils
import loamstream.util.Tries
import loamstream.uger.JobSubmitter
import loamstream.model.execute.HashingStrategy

/**
 * @author clint
 *         kyuksel
 * Nov 10, 2016
 */
trait AppWiring {
  def config: LoamConfig
  
  def dao: LoamDao

  def executer: Executer

  def cloudStorageClient: Option[CloudStorageClient]
  
  private[AppWiring] def makeJobFilter(conf: Conf): JobFilter = {
    import AppWiring.determineHashingStrategy
    
    if (conf.runEverything()) JobFilter.RunEverything else new DbBackedJobFilter(dao, determineHashingStrategy(conf))
  }
  
  def shutdown(): Seq[Throwable]
}

object AppWiring extends DrmaaClientHelpers with Loggable {

  def apply(cli: Conf): AppWiring = new DefaultAppWiring(cli)
  
  private final class DefaultAppWiring(cli: Conf) extends AppWiring with DefaultDb {
    override lazy val config: LoamConfig = {
      val typesafeConfig: Config = loadConfig(cli)
      
      //NB: .get is safe here, since we know LoamConfig.fromConfig won't return Failure
      //TODO: Revisit this
      LoamConfig.fromConfig(typesafeConfig).get
    }
    
    override def executer: Executer = terminableExecuter

    override def shutdown(): Seq[Throwable] = terminableExecuter.shutdown()

    override lazy val cloudStorageClient: Option[CloudStorageClient] = makeCloudStorageClient(cli)

    private lazy val terminableExecuter: TerminableExecuter = {
      trace("Creating executer...")

      val jobFilter = makeJobFilter(cli)

      val threadPoolSize = 50
      
      //TODO: Make the number of threads this uses configurable
      val numberOfCPUs = Runtime.getRuntime.availableProcessors
      
      val (localEC, localEcHandle) = ExecutionContexts.threadPool(numberOfCPUs)
      
      val localRunner = AsyncLocalChunkRunner(config.executionConfig)(localEC)

      val (ugerRunner, ugerRunnerHandles) = ugerChunkRunner(cli, config, threadPoolSize)

      val googleRunner = googleChunkRunner(cli, config.googleConfig, localRunner)
      
      val compositeRunner = CompositeChunkRunner(localRunner +: (ugerRunner.toSeq ++ googleRunner))

      import loamstream.model.execute.ExecuterHelpers._
      import ExecutionContexts.threadPool

      val (executionContextWithThreadPool, threadPoolHandle) = threadPool(threadPoolSize)

      import scala.concurrent.duration._
      
      val windowLength = 30.seconds
      
      val maxNumRunsPerJob = config.executionConfig.maxRunsPerJob
      
      val rxExecuter = {
        RxExecuter(compositeRunner, windowLength, jobFilter, maxNumRunsPerJob)(executionContextWithThreadPool)
      }

      val handles: Seq[Terminable] = (ugerRunnerHandles ++ googleRunner) :+ threadPoolHandle :+ localEcHandle

      new TerminableExecuter(rxExecuter, handles: _*)
    }
  }
  
  private[apps] def determineHashingStrategy(conf: Conf): HashingStrategy = {
    import HashingStrategy.{DontHashOutputs, HashOutputs}
    
    conf.disableHashing.toOption.map { shouldDisableHashing =>
      if(shouldDisableHashing) DontHashOutputs else HashOutputs
    }.getOrElse {
      HashOutputs
    }
  }

  private def googleChunkRunner(
      cli: Conf,
      googleConfigOpt: Option[GoogleCloudConfig], 
      delegate: ChunkRunner): Option[GoogleCloudChunkRunner] = {
    
    //TODO: A better way to enable or disable Google support; for now, this is purely expedient
    
    def noGoogleConfig: Option[GoogleCloudChunkRunner] = {
      debug("Google Cloud support NOT enabled due to missing 'loamstream.googlecloud' section in the config file")
        
      None
    }
    
    def googleConfigPresent(googleConfig: GoogleCloudConfig): Option[GoogleCloudChunkRunner] = {
      val clientAttempt = CloudSdkDataProcClient.fromConfig(googleConfig)
    
      val runnerAttempt: Try[GoogleCloudChunkRunner] = {
        for {
          client <- CloudSdkDataProcClient.fromConfig(googleConfig)
        } yield {
          trace("Creating Google Cloud ChunkRunner...")
    
          GoogleCloudChunkRunner(client, googleConfig, delegate)
        }
      }

      //NB: Invoke .recover for the logging side effect only :\
      runnerAttempt.recover { case e =>
        val msg = s"""|Google Cloud support NOT enabled because ${e.getMessage}
                      |in the config file (${cli.conf.toOption})""".stripMargin
      
        debug(msg)
      }
    
      runnerAttempt.toOption
    }
    
    googleConfigOpt.fold(noGoogleConfig)(googleConfigPresent)
  }
  
  private def ugerChunkRunner(
      cli: Conf, 
      loamConfig: LoamConfig, 
      threadPoolSize: Int): (Option[UgerChunkRunner], Seq[Terminable]) = {
    
    val result @ (ugerRunnerOption, _) = unpack(makeUgerChunkRunner(loamConfig, threadPoolSize))

    //TODO: A better way to enable or disable Uger support; for now, this is purely expedient
    if(ugerRunnerOption.isEmpty) {
      val msg = s"""Uger support is NOT enabled. It can be enabled by defining loamstream.uger section
                   |in the config file (${cli.conf.toOption}).""".stripMargin
        
      debug(msg)
    }
    
    result
  }
  
  private def unpack[A,B](o: Option[(A, Seq[B])]): (Option[A], Seq[B]) = o match {
    case Some((a, b)) => (Some(a), b)
    case None => (None, Nil)
  }

  private def makeCloudStorageClient(cli: Conf): Option[CloudStorageClient] = {
    val config = loadConfig(cli)

    val attempt = for {
      googleConfig <- GoogleCloudConfig.fromConfig(config)
      gcsDriver <- GcsDriver.fromConfig(googleConfig)
    } yield {
      trace("Creating Google Cloud Storage Client...")
      GcsClient(gcsDriver)
    }

    val result = attempt.toOption
    
    if(result.isEmpty) {
      val msg = s"""Job recording is turned off for outputs identified by URIs because
                    |Google Cloud Storage Client could not be created due to ${attempt.failed.get.getMessage}
                    |in the config file (${cli.conf.toOption})""".stripMargin
      debug(msg)
    }

    result
  }

  private def makeUgerChunkRunner(
      loamConfig: LoamConfig, 
      threadPoolSize: Int): Option[(UgerChunkRunner, Seq[Terminable])] = {
    
    trace("Parsing Uger config...")

    for {
      ugerConfig <- loamConfig.ugerConfig
    } yield {
      debug("Creating Uger ChunkRunner...")

      val ugerClient = makeUgerClient

      import loamstream.model.execute.ExecuterHelpers._

      val threadPoolSize = 50

      val pollingFrequencyInHz = 0.1

      val poller = Poller.drmaa(ugerClient)

      val (scheduler, schedulerHandle) = RxSchedulers.backedByThreadPool(threadPoolSize)

      val ugerRunner = {
        val jobMonitor = new JobMonitor(scheduler, poller, pollingFrequencyInHz)

        val jobSubmitter = JobSubmitter.Drmaa(ugerClient, ugerConfig)
        
        UgerChunkRunner(loamConfig.executionConfig, ugerConfig, jobSubmitter, jobMonitor, pollingFrequencyInHz)
      }

      val handles = Seq(ugerClient, schedulerHandle, ugerRunner)

      (ugerRunner, handles)
    }
  }
  
  private def makeUgerClient: UgerClient = new UgerClient(new Drmaa1Client, AccountingClient.useActualBinary())

  private def loadConfig(cli: Conf): Config = {
    def defaults: Config = ConfigFactory.load()

    cli.conf.toOption match {
      case Some(confFile) => ConfigUtils.configFromFile(confFile).withFallback(defaults)
      case None           => ConfigUtils.allowSyspropOverrides(defaults)
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
      toStop: Terminable*) extends Executer {

    override def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
      delegate.execute(executable)(timeout)
    }

    def shutdown(): Seq[Throwable] = {
      import Throwables._
      
      for {
        terminable <- toStop
        e <- quietly("Error shutting down: ")(terminable.stop()) 
      } yield e
    }
  }
}
