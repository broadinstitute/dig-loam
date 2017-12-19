package loamstream.uger

import java.nio.file.Path

import scala.concurrent.duration.Duration
import scala.util.Try

import org.ggf.drmaa.DrmaaException
import org.ggf.drmaa.ExitTimeoutException
import org.ggf.drmaa.InvalidJobException
import org.ggf.drmaa.JobInfo
import org.ggf.drmaa.JobTemplate
import org.ggf.drmaa.Session
import org.ggf.drmaa.SessionFactory

import loamstream.conf.UgerConfig
import loamstream.model.execute.Resources.UgerResources
import loamstream.util.Classes.simpleNameOf
import loamstream.util.CompositeException
import loamstream.util.Loggable
import loamstream.util.ValueBox
import loamstream.model.execute.UgerSettings
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime

/**
 * Created on: 5/19/16
 *
 * @author Kaan Yuksel 
 * @author clint
 * 
 * A DRMAAv1 implementation of DrmaaClient; can submit work to UGER and monitor it.
 * 
 */
final class Drmaa1Client extends DrmaaClient with Loggable {

  /*
   * NOTE: BEWARE: DRMAAv1 is not thread-safe.  All operations on org.ggf.drmaa.Sessions that change the number
   * of remote jobs - either by submitting them, killing them, or otherwise altering them with Session.control() -
   * need to be synchronized; they can't happen concurrently.
   * 
   * Currently, this is handled by doing all operations that need a Session inside a call to withSession().
   */
  
  import DrmaaClient._

  /*
   * NB: Several DRMAA operations are only valid if they're performed via the same Session as previous operations;
   * We use one Session per client to ensure that all operations performed by this instance use the same Session.
   * We wrap the Session in a ValueBox to make it easier to synchronize access to it.   
   */
  private[this] lazy val sessionBox: ValueBox[Session] = ValueBox(getNewSession)
  
  private def getNewSession: Session = {
    debug("Getting new DRMAA session")

    try {
      val s = SessionFactory.getFactory.getSession

      debug(s"\tVersion: ${s.getVersion}")
      debug(s"\tDrmSystem: ${s.getDrmSystem}")
      debug(s"\tDrmaaImplementation: ${s.getDrmaaImplementation}")

      //NB: Passing an empty string (or null) means that "the default DRM system is used, provided there is only one
      //DRMAA implementation available" according to the DRMAA javadocs. (Whatever that means :\)
      s.init("")

      s
    } catch {
        case e: UnsatisfiedLinkError =>
          error(s"Please check if you are running on a system with UGER (e.g. Broad VM). " +
            s"Note that UGER is required if the configuration file specifies a 'uger { ... }' block.")
          throw e
    }
  }
  
  /**
   * Kill the job with the specified id, if the job is running.
   */
  override def killJob(jobId: String): Unit = {
    debug(s"Killing Job '$jobId'")
    
    withSession(_.control(jobId, Session.TERMINATE))
  }
  
  /**
   * Kill all jobs.
   */
  override def killAllJobs(): Unit = {
    debug(s"Killing all jobs...")
    
    killJob(Session.JOB_IDS_SESSION_ALL)
  }
  
  /**
   * Shut down this client and dispose of any DRMAA resources it has acquired (Sessions, etc)
   */
  override def stop(): Unit = withSession { session =>
    def failureAsOption(block: => Any): Option[Throwable] = Try(block).failed.toOption
    
    val killAllFailureOpt = failureAsOption(killAllJobs())
    val shutdownFailureOpt = failureAsOption(tryShuttingDown(session))
    
    val failures = killAllFailureOpt.toSeq ++ shutdownFailureOpt
    
    if(failures.nonEmpty) {
      throw new CompositeException(failures)
    }
  }
  
  /**
   * Synchronously obtain the status of one running UGER job, given its id.
   *
   * @param jobId the id of the job to get the status of
   * @return Success wrapping the JobStatus corresponding to the code obtained from UGER,
   * or Failure if the job id isn't known.  (Lamely, this can occur if the job is finished.)
   */
  override def statusOf(jobId: String): Try[UgerStatus] = {
    Try {
      withSession { session =>
        val status = session.getJobProgramStatus(jobId)
        val jobStatus = UgerStatus.fromUgerStatusCode(status)

        debug(s"Job '$jobId' has status $status, mapped to $jobStatus")

        if (jobStatus.isFinished) {
          doWait(session, jobId, Duration.Zero)
        } else {
          jobStatus
        }
      }
    }
  }

  /**
   * Synchronously submit a job, in the form of a UGER wrapper shell script.
   *
   * @param ugerConfig contains the parameters to configure a job
   * @param pathToScript the path to the script that UGER should run
   * @param jobName a descriptive prefix used to identify the job.  Has no impact on how the job runs.
   * @param numTasks length of task array to be submitted as a single UGER job
   */
  override def submitJob(
      ugerSettings: UgerSettings,
      ugerConfig: UgerConfig,
      taskArray: UgerTaskArray): DrmaaClient.SubmissionResult = {

    val ugerWorkDir = ugerConfig.workDir
    
    val fullNativeSpec = Drmaa1Client.nativeSpec(ugerSettings)
    
    runJob(taskArray, ugerWorkDir, fullNativeSpec)
  }
  
  /**
   * Synchronously wait for the timeout period for the job with the given id to complete.
   * If the timeout period elapses and the job doesn't complete, assume the job is still running, and inquire
   * about its status with statusOf.
   * 
   * @param jobId the job id to wait for
   * @param timeout how long to wait (note that this method can be called many times)
   * @return Success with a JobStatus reflecting the completion status of the job, or a Failure.
   * If the timeout elapses without the job finishing, return Success(Running).  If an InvalidJobException
   * is thrown while waiting, return Success(Done).
   */
  override def waitFor(jobId: String, timeout: Duration): Try[UgerStatus] = {
    val waitAttempt = Try {
      withSession { session =>
        doWait(session, jobId, timeout)
      }
    }
      
    //If we time out before the job finishes, and we don't get an InvalidJobException, the job must be running 
    waitAttempt.recover {
      case e: ExitTimeoutException =>
        debug(s"Timed out waiting for job '$jobId' to finish; assuming the job's state is ${UgerStatus.Running}")
        UgerStatus.Running
      case e: InvalidJobException =>
        debug(s"Received InvalidJobException while 'wait'ing for job '$jobId'. " +
          s"Assuming that the data records of the job was already reaped by a previous call, " +
          s"and therefore mapping its status to ${UgerStatus.Done}")
        UgerStatus.Done
    }
  }
  
  private def doWait(session: Session, jobId: String, timeout: Duration): UgerStatus = {
    val jobInfo = session.wait(jobId, timeout.toSeconds)
    
    val resources = Drmaa1Client.toResources(jobInfo)
      
    //Use recover for side-effect only
    resources.recover {
      case e: Exception => warn(s"Error parsing resource usage data for Job '$jobId'", e)
    }
    
    val resourcesOption = resources.toOption
    
    val result = if (jobInfo.hasExited) {
      val exitCode = jobInfo.getExitStatus
      
      debug(s"Job '$jobId' exited with status code '${exitCode}'")

      UgerStatus.CommandResult(exitCode, resourcesOption)
    } else if (jobInfo.wasAborted) {
      info(s"Job '$jobId' was aborted; job info: $jobInfo")

      //TODO: Add JobStatus.Aborted?
      UgerStatus.Failed(resourcesOption)
    } else if (jobInfo.hasSignaled) {
      info(s"Job '$jobId' signaled, terminatingSignal = '${jobInfo.getTerminatingSignal}'")

      UgerStatus.Failed(resourcesOption)
    } else if (jobInfo.hasCoreDump) {
      info(s"Job '$jobId' dumped core")
      
      UgerStatus.Failed(resourcesOption)
    } else {
      debug(s"Job '$jobId' finished with unknown status")

      UgerStatus.DoneUndetermined(resourcesOption)
    }
    
    debug(s"Job '$jobId' finished, returning status $result")
    
    result
  }

  private def tryShuttingDown(s: Session): Unit = {
    try { s.exit() }
    catch {
      //NB: session.exit() will fail if an exception was thrown by session.init(), or if it is invoked more than
      //once.  In those cases, there's not much we can do.
      case e: DrmaaException => warn(s"Could not properly exit DRMAA Session due to ${e.getClass.getName}", e)
    }
  }
  
  private def withSession[A](f: Session => A): A = {
    sessionBox.get { currentSession =>
      try { f(currentSession) } 
      catch {
        case e: DrmaaException => {
          debug(s"Got ${simpleNameOf(e)}; re-throwing", e)
          
          throw e
        }
      }
    }
  }
  
  private def runJob(taskArray: UgerTaskArray,
                     outputDir: Path,
                     nativeSpecification: String): SubmissionResult = {

    withJobTemplate { (session, jt) =>
      val taskStartIndex = 1
      val taskEndIndex = taskArray.size
      val taskIndexIncr = 1

      val jobName = taskArray.ugerJobName
      val pathToScript = taskArray.ugerScriptFile
      
      debug(s"Using native spec: '$nativeSpecification'")
      debug(s"Using job name: '$jobName'")
      debug(s"Using script: '$pathToScript'")
      
      import Drmaa1Client._
      
      jt.setNativeSpecification(nativeSpecification)
      jt.setRemoteCommand(pathToScript.toString)
      jt.setJobName(jobName)
      //NB: Where a job's stdout goes
      jt.setOutputPath(taskArray.stdOutPathTemplate)
      //NB: Where a job's stderr goes
      jt.setErrorPath(taskArray.stdErrPathTemplate)

      import scala.collection.JavaConverters._
      
      val jobIds = session.runBulkJobs(jt, taskStartIndex, taskEndIndex, taskIndexIncr).asScala.map(_.toString)
    
      debug(s"Jobs have been submitted with ids ${jobIds.mkString(",")}")

      val idsForJobs = jobIds.zip(taskArray.ugerJobs).toMap
      
      def ugerIdsToJobsString = {
        (for {
          (ugerId, job) <- idsForJobs.mapValues(_.commandLineJob)
        } yield {
          s"Uger Id: $ugerId => $job"
        }).mkString("\n")
      }
      
      info(s"Uger ids assigned to jobs:\n$ugerIdsToJobsString")
      
      SubmissionSuccess(idsForJobs)
    }
  }

  private def withJobTemplate[A <: SubmissionResult](f: (Session, JobTemplate) => A): SubmissionResult = {
    withSession { session =>
      val jt = session.createJobTemplate
      
      try { f(session, jt) }
      catch {
        case e: DrmaaException => {
          error(s"Error: ${e.getMessage}", e)
          
          SubmissionFailure(e)
        }
      }
      finally { session.deleteJobTemplate(jt) }
    }
  }
}

object Drmaa1Client {
  private[uger] def toResources(jobInfo: JobInfo): Try[UgerResources] = {
    import scala.collection.JavaConverters._
    
    UgerResources.fromMap(jobInfo.getResourceUsage.asScala.toMap)
  }

  private[uger] def nativeSpec(ugerSettings: UgerSettings): String = {
    //Will this ever change?
    val staticPart = "-cwd -shell y -b n"
    
    val dynamicPart = {
      import ugerSettings._
    
      val numCores = cores.value
      val runTimeInHours: Int = maxRunTime.hours.toInt
      val mem: Int = memoryPerCore.gb.toInt
      
      s"-binding linear:${numCores} -pe smp ${numCores} -q ${queue} -l h_rt=${runTimeInHours}:0:0,h_vmem=${mem}g"
    }
    
    s"$staticPart $dynamicPart"
  }
}
