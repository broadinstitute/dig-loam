package loamstream.util

import loamstream.model.jobs.LJob

/**
  * LoamStream
  * Created by oliverr on 3/4/2016.
  */
object TestUtils {

  def assertSomeAndGet[A](option: Option[A]): A = {
    import org.scalatest.Assertions._

    assert(option.nonEmpty)
    
    option.get
  }

  def isHitOfSetOfOne(shot: Shot[Set[LJob]]): Boolean = shot match {
    case Hit(jobs) => jobs.size == 1
    case _ => false
  }
}