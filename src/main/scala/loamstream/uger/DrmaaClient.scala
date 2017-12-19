package loamstream.uger

import java.nio.file.Path

import loamstream.conf.UgerConfig

import scala.util.Try
import scala.concurrent.duration.Duration
import loamstream.util.Terminable
import loamstream.model.execute.UgerSettings

/**
 * @author clint
 * date: Jun 29, 2016
 * 
 * A trait to represent operations that can be performed on a Uger cluster.
 * For now, these are just submitting a job and checking a job's status.
 */
trait DrmaaClient extends Terminable {
  /**
   * Synchronously submit a job to UGER.
   *
   * @param ugerConfig contains the parameters to configure a job
   * @param pathToScript the path to the script that UGER should run
   * @param jobName a descriptive prefix used to identify the job.  Has no impact on how the job runs.
   * @param numTasks length of task array to be submitted as a single UGER job
   */
  def submitJob(
      ugerSettings: UgerSettings,
      ugerConfig: UgerConfig,
      taskArrayScript: UgerTaskArray): DrmaaClient.SubmissionResult
    
  /**
   * Synchronously inspect the status of a job with the given ID
 *
   * @param jobId the job ID, assigned by UGER, to inquire about
   * @return a Try, since inquiring might fail
   */
  def statusOf(jobId: String): Try[UgerStatus]

  /**
   * Wait (synchronously) for a job to complete.
 *
   * @param jobId the job ID, assigned by UGER, of the job to wait for
   * @param timeout how long to wait.  If timeout elapses and the job doesn't finish, try to determine the job's
   * status using statusOf()
   */
  def waitFor(jobId: String, timeout: Duration): Try[UgerStatus]
  
  /**
   * Shut down this client and dispose of any DRMAA resources it has acquired (Sessions, etc)
   */
  override def stop(): Unit
  
  /**
   * Kill the job with the specified id, if the job is running.
   */
  def killJob(jobId: String): Unit
  
  /**
   * Kill all jobs.
   */
  def killAllJobs(): Unit
}

object DrmaaClient {

  sealed trait SubmissionResult {
    def isFailure: Boolean
  }

  final case class SubmissionFailure(cause: Exception) extends SubmissionResult {
    override val isFailure: Boolean = true
  }

  final case class SubmissionSuccess(idsForJobs: Map[String, UgerJobWrapper]) extends SubmissionResult {
    override val isFailure: Boolean = false
  }
}
