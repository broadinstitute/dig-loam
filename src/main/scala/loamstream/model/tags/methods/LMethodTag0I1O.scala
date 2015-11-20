package loamstream.model.tags.methods

import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/27/2015.
  */
case class LMethodTag0I1O[O0 <: LPileTag](output0: O0)
  extends LMethodTag with LMethodTag.Has1O[O0] {
  override def inputs: Seq[LPileTag] = Seq.empty

  override def outputs: Seq[LPileTag] = Seq(output0)

  def plusKey[KN: TypeTag] = LMethodTag0I1O[O0#UpTag[KN]](output0.plusKey[KN])
}
