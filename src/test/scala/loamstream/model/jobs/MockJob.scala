package loamstream.model.jobs

import loamstream.TestHelpers

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.util.Sequence
import loamstream.util.ValueBox
import loamstream.model.execute.{ExecutionEnvironment, LocalSettings}

/**
 * @author clint
 * date: Jun 2, 2016
 */
class MockJob(
               val toReturn: Execution,
               override val name: String,
               override val inputs: Set[LJob],
               val outputs: Set[Output],
               val delay: Int) extends LJob {
  
  override def executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.Local
  
  val id: Int = MockJob.nextId()
  
  override def toString: String = s"'$name'(#$id, returning $toReturn, ${inputs.size} dependencies)"
 
  //NB: Previous versions defined equals() and hashCode() only in terms of 'toReturn', which caused problems;
  //switched back to reference equality.

  override protected def executeSelf(implicit context: ExecutionContext): Future[JobResult] = {
    count.mutate(_ + 1)

    if (delay > 0) {
      Thread.sleep(delay)
    }

    updateAndEmitJobStatus(toReturn.status)
    
    Future.successful(toReturn)
  }
  
  private[this] val count = ValueBox(0)

  def executionCount = count.value

  def copy(
            toReturn: Execution = this.toReturn,
            name: String = this.name,
            inputs: Set[LJob] = this.inputs,
            outputs: Set[Output] = this.outputs,
            delay: Int = this.delay): MockJob = new MockJob(toReturn, name, inputs, outputs, delay)

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}

object MockJob {
  def apply(
             toReturn: Execution,
             name: String = nextId().toString,
             inputs: Set[LJob] = Set.empty,
             outputs: Set[Output] = Set.empty,
             delay: Int = 0): MockJob = new MockJob(toReturn, name, inputs, outputs, delay)

  def apply(
             toReturn: JobResult,
             name: String = nextId().toString,
             inputs: Set[LJob] = Set.empty,
             outputs: Set[Output] = Set.empty,
             delay: Int = 0): MockJob = new MockJob(executionFrom(jobResult = toReturn), name, inputs, outputs, delay)

  def apply(
             toReturn: JobStatus,
             name: String = nextId().toString,
             inputs: Set[LJob] = Set.empty,
             outputs: Set[Output] = Set.empty,
             delay: Int = 0): MockJob = new MockJob(executionFrom(jobStatus = toReturn), name, inputs, outputs, delay)

  def unapply(job: LJob): Option[(Execution, String, Set[LJob], Set[Output], Int)] = job match {
    case mj: MockJob => Some((mj.toReturn, mj.name, mj.inputs, mj.outputs, mj.delay))
    case _ => None
  }

  private[this] def executionFrom(jobStatus: JobStatus = JobStatus.Succeeded,
                                  jobResult: JobResult = JobResult.CommandResult(0)) = {
    Execution(TestHelpers.env,
              cmd = None,
              settings = LocalSettings(),
              jobStatus,
              Option(jobResult),
              resources = None,
              Set.empty[OutputRecord])
  }

  private[this] val ids: Sequence[Int] = Sequence()
  
  def nextId(): Int = ids.next()
}