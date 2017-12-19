package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.execute.Environment
import loamstream.model.jobs.JobStatus.Succeeded

/**
 * @author Kaan Yuksel
 * date: Jun 2, 2016
 */
//TODO: Get rid of this
final case class NoOpJob(override val inputs: Set[JobNode]) extends LocalJob {
  override def execute(implicit context: ExecutionContext): Future[Execution] = Future.successful {
    Execution(
        env = Environment.Local,
        cmd = None,
        status = Succeeded,
        result = None,
        resources = None,
        outputs = Set.empty[OutputRecord],
        outputStreams = None)
  }
  
  override def toString: String = name
  
  override def name: String = s"${getClass.getSimpleName}#${id}(${inputs.size} dependencies)"
  
  override def executionEnvironment: Environment = Environment.Local

  override val outputs: Set[Output] = Set.empty
}
