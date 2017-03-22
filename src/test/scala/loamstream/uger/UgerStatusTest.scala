package loamstream.uger

import org.scalatest.FunSuite
import org.ggf.drmaa.Session
import loamstream.model.jobs.JobResult
import loamstream.model.execute.Memory
import java.time.Instant
import loamstream.model.execute.CpuTime
import loamstream.model.execute.Resources.UgerResources

/**
 * @author clint
 * Jan 5, 2017
 */
final class UgerStatusTest extends FunSuite {
  import UgerStatus._

  //scalastyle:off magic.number
  
  import scala.concurrent.duration._
    
  private val resources = UgerResources(
      Memory.inGb(2), 
      CpuTime(1.second), 
      Some("example.com"), 
      Some(Queue.Long),
      Instant.now,
      Instant.now)
  
  test("transformResources") {
    def doTestWithExistingResources(makeInitialStatus: Option[UgerResources] => UgerStatus): Unit = {
      val initialStatus = makeInitialStatus(Some(resources))
      
      assert(initialStatus.resourcesOpt !== None)
      assert(initialStatus.resourcesOpt.get.queue === Some(Queue.Long))
      
      val transformed = initialStatus.transformResources(_.withQueue(Queue.Short))
      
      assert(transformed.getClass === initialStatus.getClass)
      
      assert(initialStatus.resourcesOpt !== None)
      assert(initialStatus.resourcesOpt.get.queue === Some(Queue.Long))
      
      assert(transformed.resourcesOpt.get.queue === Some(Queue.Short))
    }
    
    doTestWithExistingResources(CommandResult(0, _))
    doTestWithExistingResources(CommandResult(1, _))
    doTestWithExistingResources(CommandResult(42, _))
    doTestWithExistingResources(Failed( _))
    doTestWithExistingResources(DoneUndetermined(_))
    doTestWithExistingResources(Suspended(_))
    doTestWithExistingResources(Undetermined(_))
    
    def doTestWithoutResources(initialStatus: UgerStatus): Unit = {
      assert(initialStatus.resourcesOpt === None)
      
      val transformed = initialStatus.transformResources(_.withQueue(Queue.Short))
      
      assert(initialStatus.resourcesOpt === None)
      
      assert(transformed.resourcesOpt === None)
      
      assert(transformed.getClass === initialStatus.getClass)
      assert(transformed eq initialStatus)
    }
   
    doTestWithoutResources(Done)
    doTestWithoutResources(Queued)
    doTestWithoutResources(QueuedHeld)
    doTestWithoutResources(Requeued)
    doTestWithoutResources(RequeuedHeld)
    doTestWithoutResources(Running)
  }
      
  test("fromUgerStatusCode") {
    import Session._
    
    assert(fromUgerStatusCode(QUEUED_ACTIVE) === Queued)
    assert(fromUgerStatusCode(SYSTEM_ON_HOLD) === QueuedHeld)
    assert(fromUgerStatusCode(USER_ON_HOLD) === QueuedHeld)
    assert(fromUgerStatusCode(USER_SYSTEM_ON_HOLD) === QueuedHeld)
    assert(fromUgerStatusCode(RUNNING) === Running)
    assert(fromUgerStatusCode(SYSTEM_SUSPENDED) === Suspended())
    assert(fromUgerStatusCode(USER_SUSPENDED) === Suspended())
    assert(fromUgerStatusCode(USER_SYSTEM_SUSPENDED) === Suspended())
    assert(fromUgerStatusCode(DONE) === Done)
    assert(fromUgerStatusCode(FAILED) === Failed())
    assert(fromUgerStatusCode(UNDETERMINED) === Undetermined())
    assert(fromUgerStatusCode(-123456) === Undetermined())
    assert(fromUgerStatusCode(123456) === Undetermined())
    assert(fromUgerStatusCode(Int.MinValue) === Undetermined())
    assert(fromUgerStatusCode(Int.MaxValue) === Undetermined())
  }

  test("toJobState") {
    assert(toJobResult(Done) === JobResult.Succeeded)
    
    assert(toJobResult(CommandResult(-1, Some(resources))) === JobResult.CommandResult(-1, Some(resources)))
    assert(toJobResult(CommandResult(0, Some(resources))) === JobResult.CommandResult(0, Some(resources)))
    assert(toJobResult(CommandResult(42, Some(resources))) === JobResult.CommandResult(42, Some(resources)))
    
    assert(toJobResult(DoneUndetermined(Some(resources))) === JobResult.Failed(Some(resources)))
    assert(toJobResult(Failed()) === JobResult.Failed())
    assert(toJobResult(Failed(Some(resources))) === JobResult.Failed(Some(resources)))
    
    assert(toJobResult(Queued) === JobResult.Running)
    assert(toJobResult(QueuedHeld) === JobResult.Running)
    assert(toJobResult(Requeued) === JobResult.Running)
    assert(toJobResult(RequeuedHeld) === JobResult.Running)
    assert(toJobResult(Running) === JobResult.Running)
    
    assert(toJobResult(Suspended()) === JobResult.Failed())
    assert(toJobResult(Undetermined()) === JobResult.Failed())
    assert(toJobResult(Suspended(Some(resources))) === JobResult.Failed(Some(resources)))
    assert(toJobResult(Undetermined(Some(resources))) === JobResult.Failed(Some(resources)))
  }
  
  
  test("isDone") {
    doFlagTest(
      _.isDone, 
      expectedTrueFor = Done, 
      expectedFalseFor = DoneUndetermined(), Failed(Some(resources)), Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended(Some(resources)), Undetermined())
  }
  
  test("isFailed") {
    doFlagTest(
      _.isFailed, 
      expectedTrueFor = Failed(), 
      expectedFalseFor = Done, DoneUndetermined(), Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended(Some(resources)), Undetermined(Some(resources)))
  }
  
  test("isQueued") {
    doFlagTest(
      _.isQueued, 
      expectedTrueFor = Queued, 
      expectedFalseFor = Done, DoneUndetermined(Some(resources)), Failed(Some(resources)), QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended(), Undetermined())
  }
  
  test("isQueuedHeld") {
    doFlagTest(
      _.isQueuedHeld, 
      expectedTrueFor = QueuedHeld, 
      expectedFalseFor = Done, DoneUndetermined(Some(resources)), Failed(Some(resources)), Queued, Requeued, 
                         RequeuedHeld, Running, Suspended(), Undetermined())
  }
  
  test("isRunning") {
    doFlagTest(
      _.isRunning, 
      expectedTrueFor = Running, 
      expectedFalseFor = Done, DoneUndetermined(), Failed(), Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Suspended(Some(resources)), Undetermined(Some(resources)))
  }
  
  test("isSuspended") {
    doFlagTest(
      _.isSuspended, 
      expectedTrueFor = Suspended(), 
      expectedFalseFor = Done, DoneUndetermined(Some(resources)), Failed(Some(resources)), Queued, QueuedHeld, 
                         Requeued, RequeuedHeld, Running, Undetermined())
  }
  
  test("isUndetermined") {
    doFlagTest(
      _.isUndetermined, 
      expectedTrueFor = Undetermined(Some(resources)), 
      expectedFalseFor = Done, DoneUndetermined(), Failed(), Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended(Some(resources)))
  }
  
  test("isDoneUndetermined") {
    doFlagTest(
      _.isDoneUndetermined, 
      expectedTrueFor = DoneUndetermined(Some(resources)), 
      expectedFalseFor = Done, Failed(Some(resources)), Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended(), Undetermined())
  }

  test("notFinished") {
    assert(Queued.notFinished === true)
    assert(QueuedHeld.notFinished === true)
    assert(Running.notFinished === true)
    assert(Suspended().notFinished === true)
    assert(Undetermined().notFinished === true)
    assert(Suspended(Some(resources)).notFinished === true)
    assert(Undetermined(Some(resources)).notFinished === true)
    
    assert(CommandResult(42, None).notFinished === false)
    assert(CommandResult(42, Some(resources)).notFinished === false)
    assert(CommandResult(0, None).notFinished === false)
    assert(CommandResult(0, Some(resources)).notFinished === false)
    assert(Done.notFinished === false)
    assert(DoneUndetermined().notFinished === false)
    assert(Failed().notFinished === false)
    assert(DoneUndetermined(Some(resources)).notFinished === false)
    assert(Failed(Some(resources)).notFinished === false)
    
    //TODO: ???
    assert(Requeued.notFinished === false)
    //TODO: ???
    assert(RequeuedHeld.notFinished === false)
  }
  
  test("isFinished") {
    assert(Queued.isFinished === false)
    assert(QueuedHeld.isFinished === false)
    assert(Running.isFinished === false)
    assert(Suspended().isFinished === false)
    assert(Undetermined().isFinished === false)
    assert(Suspended(Some(resources)).isFinished === false)
    assert(Undetermined(Some(resources)).isFinished === false)
    
    assert(CommandResult(42, None).isFinished === true)
    assert(CommandResult(42, Some(resources)).isFinished === true)
    assert(CommandResult(0, None).isFinished === true)
    assert(CommandResult(0, Some(resources)).isFinished === true)
    assert(Done.isFinished === true)
    assert(DoneUndetermined().isFinished === true)
    assert(Failed().isFinished === true)
    assert(DoneUndetermined(Some(resources)).isFinished === true)
    assert(Failed(Some(resources)).isFinished === true)
    
    //TODO: ???
    assert(Requeued.isFinished === true)
    //TODO: ???
    assert(RequeuedHeld.isFinished === true)
  }
  
  private def doFlagTest(
      flag: UgerStatus => Boolean, 
      expectedTrueFor: UgerStatus, 
      expectedFalseFor: UgerStatus*): Unit = {
    
    assert(flag(expectedTrueFor) === true)
    
    for(ugerStatus <- expectedFalseFor) {
      assert(flag(ugerStatus) === false)
    }
  }
  
  //scalastyle:on magic.number
}
