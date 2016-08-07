package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.util.{Maps, Shot}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

/**
  * @author clint
  *         date: Jun 1, 2016
  */
final class RxExecuter(runner: ChunkRunner)(implicit executionContext: ExecutionContext) extends LExecuter {

  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    val futureResults = Future.sequence(executable.jobs.map(executeJob)).map(Maps.mergeMaps)

    val future = futureResults.map(ExecuterHelpers.toShotMap)

    Await.result(future, timeout)
  }

  private def executeJob(job: LJob)(implicit executionContext: ExecutionContext): Future[Map[LJob, Result]] = {
    def loop(remainingOption: Option[LJob], acc: Map[LJob, Result]): Future[Map[LJob, Result]] = {
      remainingOption match {
        case None => Future.successful(acc)
        case Some(j) =>
          val leaves = j.leaves

          def leafChunks: Seq[Set[LJob]] = leaves.grouped(runner.maxNumJobs).toSeq

          val futures = for {
            leafChunk <- leafChunks
          } yield {
            for {
              leafResults <- runner.run(leafChunk)
              shouldStop = j.isLeaf
              next = if (shouldStop) None else Some(j.removeAll(leaves))
              resultsSoFar <- loop(next, acc ++ leafResults)
            } yield resultsSoFar
          }

          Future.sequence(futures).map(Maps.mergeMaps)
      }
    }

    loop(Option(job), Map.empty)
  }

  private def anyFailures(m: Map[LJob, LJob.Result]): Boolean = m.values.exists(_.isFailure)
}

object RxExecuter {

  def default: RxExecuter = new RxExecuter(AsyncLocalChunkRunner)(ExecutionContext.global)

  def apply(chunkRunner: ChunkRunner)(implicit context: ExecutionContext): RxExecuter = {
    new RxExecuter(chunkRunner)
  }
  
  object AsyncLocalChunkRunner extends ChunkRunner {

    import ExecuterHelpers._

    override def maxNumJobs = 100 // scalastyle:ignore magic.number

    override def run(leaves: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, Result]] = {
      //NB: Use an iterator to evaluate input jobs lazily, so we can stop evaluating
      //on the first failure, like the old code did.
      val leafResultFutures = leaves.iterator.map(executeSingle)

      //NB: Convert the iterator to an IndexedSeq to force evaluation, and make sure 
      //input jobs are evaluated before jobs that depend on them.
      val futureLeafResults = Future.sequence(leafResultFutures).map(consumeUntilFirstFailure)

      futureLeafResults.map(Maps.mergeMaps)
    }
  }
}