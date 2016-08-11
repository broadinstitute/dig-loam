package loamstream.util

/**
 * @author clint
 * date: Aug 11, 2016
 */
object Traversables {
  object Implicits {
    final implicit class TraversableOps[A](val as: Traversable[A]) extends AnyVal {
      def mapTo[B](field: A => B): Map[A,B] = as.map(a => a -> field(a)).toMap
    }
  }
}