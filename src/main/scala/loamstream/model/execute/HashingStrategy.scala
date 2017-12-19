package loamstream.model.execute

/**
 * @author clint
 * Dec 12, 2017
 */
sealed trait HashingStrategy {
  final def shouldHash: Boolean = this == HashingStrategy.HashOutputs
  final def shouldNotHash: Boolean = !shouldHash
}

object HashingStrategy {
  case object HashOutputs extends HashingStrategy
  case object DontHashOutputs extends HashingStrategy
}
