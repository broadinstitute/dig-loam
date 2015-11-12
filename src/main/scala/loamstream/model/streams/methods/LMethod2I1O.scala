package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has1O, Has2I}
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag2I1O
import loamstream.model.tags.piles.LPileTag

/**
  * LoamStream
  * Created by oliverr on 10/29/2015.
  */
object LMethod2I1O {

  case class LSocketI0[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag](method: LMethod2I1O[I0, I1, O0])
    extends LSocket[I0, LMethod2I1O[I0, I1, O0]] {
    override def pileTag = method.tag.input0
  }

  case class LSocketI1[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag](method: LMethod2I1O[I0, I1, O0])
    extends LSocket[I1, LMethod2I1O[I0, I1, O0]] {
    override def pileTag = method.tag.input1
  }

  case class LSocketO0[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag](method: LMethod2I1O[I0, I1, O0])
    extends LSocket[O0, LMethod2I1O[I0, I1, O0]] {
    override def pileTag = method.tag.output0
  }

}

trait LMethod2I1O[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag]
  extends Has2I[I0, I1, LMethod2I1O[I0, I1, O0]] with Has1O[O0, LMethod2I1O[I0, I1, O0]] {
  type MTag = LMethodTag2I1O[I0, I1, O0]
}

