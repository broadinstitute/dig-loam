package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has2I, Has2O}
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag2I2O
import loamstream.model.tags.piles.LPileTag

/**
  * LoamStream
  * Created by oliverr on 10/29/2015.
  */
object LMethod2I2O {

  case class LSocketI0[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag,
  O1 <: LPileTag](method: LMethod2I2O[I0, I1, O0, O1])
    extends LSocket[I0, LMethod2I2O[I0, I1, O0, O1]] {
    override def pileTag = method.tag.input0
  }

  case class LSocketI1[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag,
  O1 <: LPileTag](method: LMethod2I2O[I0, I1, O0, O1])
    extends LSocket[I1, LMethod2I2O[I0, I1, O0, O1]] {
    override def pileTag = method.tag.input1
  }

  case class LSocketO0[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag,
  O1 <: LPileTag](method: LMethod2I2O[I0, I1, O0, O1])
    extends LSocket[O0, LMethod2I2O[I0, I1, O0, O1]] {
    override def pileTag = method.tag.output0
  }

  case class LSocketO1[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag,
  O1 <: LPileTag](method: LMethod2I2O[I0, I1, O0, O1])
    extends LSocket[O1, LMethod2I2O[I0, I1, O0, O1]] {
    override def pileTag = method.tag.output1
  }

}

trait LMethod2I2O[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag]
  extends Has2I[I0, I1, LMethod2I2O[I0, I1, O0, O1]] with Has2O[O0, O1, LMethod2I2O[I0, I1, O0, O1]] {
  type MTag = LMethodTag2I2O[I0, I1, O0, O1]
}

