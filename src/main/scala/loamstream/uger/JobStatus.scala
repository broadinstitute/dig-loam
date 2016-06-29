package loamstream.uger

import scala.util.Try
import org.ggf.drmaa.Session
import scala.util.Success

/**
 * @author clint
 * date: Jun 16, 2016
 * 
 * An ADT/"Enum" to represent job statuses as reported by UGER.  Values correspond to
 * org.ggf.drmaa.Session.{
 *   UNDETERMINED,
 *   QUEUED_ACTIVE,
 *   SYSTEM_ON_HOLD,
 *   USER_ON_HOLD,
 *   USER_SYSTEM_ON_HOLD,
 *   RUNNING,
 *   SYSTEM_SUSPENDED,
 *   USER_SUSPENDED,
 *   USER_SYSTEM_SUSPENDED,
 *   DONE,
 *   FAILED
 * }
 */
sealed trait JobStatus {
  import JobStatus._
  
  def isDone: Boolean = this == Done
  def isFailed: Boolean = this == Failed
  def isQueued: Boolean = this == Queued
  def isQueuedHeld: Boolean = this == QueuedHeld
  def isRunning: Boolean = this == Running
  def isSuspended: Boolean = this == Suspended
  def isUndetermined: Boolean = this == Undetermined
}

object JobStatus {
  case object Done extends JobStatus
  case object Failed extends JobStatus
  case object Queued extends JobStatus
  case object QueuedHeld extends JobStatus
  case object Requeued extends JobStatus
  case object RequeuedHeld extends JobStatus
  case object Running extends JobStatus
  case object Suspended extends JobStatus
  case object Undetermined extends JobStatus
  
  import Session._
  
  def fromUgerStatusCode(status: Int): JobStatus = status match {
    case QUEUED_ACTIVE                                              => Queued
    case SYSTEM_ON_HOLD | USER_ON_HOLD | USER_SYSTEM_ON_HOLD        => QueuedHeld
    case RUNNING                                                    => Running
    case SYSTEM_SUSPENDED | USER_SUSPENDED | USER_SYSTEM_SUSPENDED  => Suspended
    case DONE                                                       => Done
    case FAILED                                                     => Failed
    case UNDETERMINED | _                                           => Undetermined
  }
}