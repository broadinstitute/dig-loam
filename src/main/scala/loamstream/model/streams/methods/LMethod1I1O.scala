package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has1I, Has1O}
import loamstream.model.tags.methods.LMethodTag1I1O
import loamstream.model.tags.piles.LPileTag

/**
  * LoamStream
  * Created by oliverr on 10/29/2015.
  */
trait LMethod1I1O[I0 <: LPileTag, O0 <: LPileTag, M <: LMethod] extends Has1I[I0, M] with Has1O[O0, M] {
  type T = LMethodTag1I1O[I0, O0]
}

