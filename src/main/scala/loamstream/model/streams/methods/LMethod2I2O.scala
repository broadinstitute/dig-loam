package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has2I, Has2O}
import loamstream.model.tags.methods.LMethodTag2I2O
import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/29/2015.
 */
trait LMethod2I2O[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag,
MT <: LMethodTag2I2O[I0, I1, O0, O1], M <: LMethod[MT]]
  extends Has2I[I0, I1, MT, M] with Has2O[O0, O1, MT, M]

