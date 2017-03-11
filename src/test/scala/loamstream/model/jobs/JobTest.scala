package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.util.Futures
import loamstream.util.ObservableEnrichments
import scala.concurrent.ExecutionContext
import loamstream.oracle.Resources.LocalResources

/**
 * @author clint
 * date: May 27, 2016
 */
final class JobTest extends FunSuite with TestJobs {
  
  //scalastyle:off magic.number
  
  import JobState._
  import Futures.waitFor
  import ObservableEnrichments._
  
  test("execute") {
    val job = MockJob(CommandResult(42, LocalResources))
    
    val states = job.states.until(_.isFinished).to[Seq].firstAsFuture
    
    job.execute(ExecutionContext.global)
    
    assert(waitFor(states) === Seq(NotStarted, Running, CommandResult(42, LocalResources)))
  }
  
  test("lastState - simple") {
    val job = MockJob(CommandResult(42, LocalResources))
    
    val lastStateFuture = job.lastState.firstAsFuture
    
    job.execute(ExecutionContext.global)
    
    assert(waitFor(lastStateFuture) === CommandResult(42, LocalResources))
  }

  test("lastState - subsequent 'terminal' states don't count") {
    val job = MockJob(CommandResult(42, LocalResources))
    
    val lastStatesFuture = job.lastState.to[Seq].firstAsFuture
    
    job.updateAndEmitJobState(NotStarted)
    job.updateAndEmitJobState(NotStarted)
    job.updateAndEmitJobState(Running)
    job.updateAndEmitJobState(Running)
    job.updateAndEmitJobState(Failed())
    job.updateAndEmitJobState(CommandResult(42, LocalResources))
    
    assert(waitFor(lastStatesFuture) === Seq(Failed()))
  }
  
  test("finalInputStates - no deps") {
    val noDeps = MockJob(CommandResult(42, LocalResources))
    
    val finalInputStatesFuture = noDeps.finalInputStates.firstAsFuture
    
    assert(waitFor(finalInputStatesFuture) === Nil)
  }
  
  test("finalInputStates - some deps") {
    val deps: Set[LJob] = Set(MockJob(Failed()), MockJob(CommandResult(0, LocalResources)), MockJob(Succeeded))
    
    val noDeps = MockJob(
        toReturn = CommandResult(42, LocalResources), 
        inputs = deps)
    
    val finalInputStatesFuture = noDeps.finalInputStates.firstAsFuture
    
    deps.foreach(_.execute(ExecutionContext.global))
    
    //NB: Use Sets to ignore order
    assert(waitFor(finalInputStatesFuture).toSet === Set(Failed(), CommandResult(0, LocalResources), Succeeded))
  }
  
  test("state/states/updateAndEmitJobState") {
    val job = MockJob(CommandResult(42, LocalResources))
    
    val first5States = job.states.take(5).to[Seq].firstAsFuture
    
    assert(job.state === NotStarted)
    
    job.updateAndEmitJobState(Unknown)
    
    assert(job.state === Unknown)
    
    job.updateAndEmitJobState(Failed(Some(LocalResources)))
    
    assert(job.state === Failed(Some(LocalResources)))
    
    job.updateAndEmitJobState(Running)
    
    assert(job.state === Running)
    
    job.updateAndEmitJobState(CommandResult(42, LocalResources))
    
    assert(job.state === CommandResult(42, LocalResources))
    
    job.updateAndEmitJobState(Succeeded)
    
    assert(job.state === Succeeded)
    
    val expected = Seq(Unknown, Failed(Some(LocalResources)), Running, CommandResult(42, LocalResources), Succeeded)
    
    assert(waitFor(first5States) === expected)
  }
  
  test("selfRunnable - no deps") {
    def doTest(resultState: JobState): Unit = {
      val noDeps = MockJob(resultState)
      
      assert(waitFor(noDeps.selfRunnable.firstAsFuture) eq noDeps)
    }
    
    doTest(Succeeded)
    doTest(Failed(Some(LocalResources)))
    doTest(NotStarted)
    doTest(CommandResult(42, LocalResources))
    doTest(Unknown)
  }
  
  test("selfRunnable - some deps") {
    def doTest(resultState: JobState, anyFailures: Boolean): Unit = {
      def mockJob(toReturn: JobState, startingState: Option[JobState] = None) = {
        val j = MockJob(toReturn)
        
        j.updateAndEmitJobState(startingState.getOrElse(toReturn))
        
        j
      }
      
      val notFinished = mockJob(CommandResult(0, LocalResources), startingState = Some(Running))
      
      val i0 = mockJob(Succeeded)
      
      val i1 = mockJob(if(anyFailures) Failed() else Succeeded)
      
      val inputs: Set[LJob] = Set(i0, notFinished, i1)
      
      val job = MockJob(toReturn = resultState, inputs = inputs)

      notFinished.updateAndEmitJobState(Succeeded)
      
      if(anyFailures) {
        assert(waitFor(job.selfRunnable.isEmpty.firstAsFuture))
      } else {
        val selfRunnableFuture = job.selfRunnable.firstAsFuture
        
        assert(waitFor(selfRunnableFuture) eq job)
      }
    }
    
    doTest(Succeeded, anyFailures = true)
    doTest(Succeeded, anyFailures = false)
    doTest(Failed(), anyFailures = true)
    doTest(Failed(), anyFailures = false)
    doTest(NotStarted, anyFailures = true)
    doTest(NotStarted, anyFailures = false)
    doTest(CommandResult(42, LocalResources), anyFailures = true)
    doTest(CommandResult(42, LocalResources), anyFailures = false)
    doTest(Unknown, anyFailures = true)
    doTest(Unknown, anyFailures = false)
  }
  
  test("runnables - no deps") {
    def doTest(resultState: JobState): Unit = {
      val job = MockJob(resultState)
      
      val runnables = job.runnables.to[Seq].firstAsFuture
      
      assert(waitFor(runnables) === Seq(job))
      
      assert(waitFor(runnables).head eq job)
    }
    
    doTest(Succeeded)
    doTest(Failed(Some(LocalResources)))
    doTest(NotStarted)
    doTest(CommandResult(42, LocalResources))
    doTest(Unknown)
  }
  
  test("runnables - some deps, no failures") {
    
    /*
     * gc0
     *    \    
     *     +---c0 
     *    /      \       
     * gc1        \
     *             +---root
     * gc2        /
     *    \      /
     *     +---c1
     *    /
     * gc3
     */
    
    def execute(jobs: Iterable[LJob]): Unit = jobs.foreach(_.execute(ExecutionContext.global))
    
    val gc0 = MockJob(Succeeded)
    val gc1 = MockJob(CommandResult(0, LocalResources))
    val gc2 = MockJob(Succeeded)
    val gc3 = MockJob(CommandResult(0, LocalResources))
    
    val c0 = MockJob(toReturn = Succeeded, inputs = Set(gc0, gc1))
    val c1 = MockJob(toReturn = CommandResult(0, LocalResources), inputs = Set(gc2, gc3))
    
    val rootJob = MockJob(Succeeded, inputs = Set(c0,c1))
    
    val grandChildren = waitFor(rootJob.runnables.take(4).to[Set].firstAsFuture)
    
    assert(grandChildren === Set(gc0, gc1, gc2, gc3))
    
    val futureChildren = rootJob.runnables.drop(4).take(2).to[Set].firstAsFuture
    
    execute(grandChildren)
    
    assert(waitFor(futureChildren) === Set(c0, c1))
    
    execute(Seq(c0, c1))
    
    val futureRoot = rootJob.runnables.drop(6).to[Set].firstAsFuture
    
    val roots = waitFor(futureRoot)
    
    assert(roots === Set(rootJob))
    assert(roots.head eq rootJob)
  }
  
  test("runnables - some deps, some failures") {
    
    /*
     * gc0 (success)
     *    \    
     *     +-------c0 (failure)
     *    /         \       
     * gc1 (success) \
     *                +---root
     * gc2 (success) /
     *    \         /
     *     +-------c1 (success)
     *    /
     * gc3 (success)
     */
    
    def execute(jobs: Iterable[LJob]): Unit = jobs.foreach(_.execute(ExecutionContext.global))
    
    val gc0 = MockJob(Succeeded)
    val gc1 = MockJob(CommandResult(0, LocalResources))
    val gc2 = MockJob(Succeeded)
    val gc3 = MockJob(CommandResult(0, LocalResources))
    
    val c0 = MockJob(toReturn = Failed(), inputs = Set(gc0, gc1))
    val c1 = MockJob(toReturn = CommandResult(0, LocalResources), inputs = Set(gc2, gc3))
    
    val rootJob = MockJob(Succeeded, inputs = Set(c0,c1))
    
    val grandChildren = waitFor(rootJob.runnables.take(4).to[Set].firstAsFuture)
    
    //We should get all the grandchildren, since they start out runnable
    assert(grandChildren === Set(gc0, gc1, gc2, gc3))
    
    val futureChildren = rootJob.runnables.drop(4).take(2).to[Set].firstAsFuture
    
    execute(grandChildren)
    
    //We should get all the children, since their children all succeed
    assert(waitFor(futureChildren) === Set(c0, c1))
    
    execute(Seq(c0, c1))
    
    //We shouldn't get the root, since one of its children failed
    val futureRootMissing = rootJob.runnables.drop(6).isEmpty.firstAsFuture
    
    assert(waitFor(futureRootMissing))
  }
  
  //scalastyle:on magic.number
}
