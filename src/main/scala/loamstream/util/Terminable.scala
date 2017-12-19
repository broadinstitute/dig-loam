package loamstream.util

/**
 * @author clint
 * Nov 23, 2016
 */
trait Terminable {
  def stop(): Unit
}

object Terminable {
  def apply(doStop: => Unit): Terminable = new Terminable {
    override def stop(): Unit = doStop
  }
}
