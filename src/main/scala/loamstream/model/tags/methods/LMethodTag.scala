package loamstream.model.tags.methods

import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/22/2015.
  */
object LMethodTag {

  trait Has1I[I0 <: LPileTag] extends LMethodTag {
    def input0: I0

    def plusKeyI0[KN: TypeTag]: Has1I[I0#UpTag[KN]]
  }

  trait Has2I[I0 <: LPileTag, I1 <: LPileTag] extends Has1I[I0] {
    def input1: I1

    def plusKeyI1[KN: TypeTag]: Has2I[I0, I1#UpTag[KN]]
  }

  trait Has3I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag] extends Has2I[I0, I1] {
    def input2: I2

    def plusKeyI2[KN: TypeTag]: Has3I[I0, I1, I2#UpTag[KN]]
  }

  trait Has4I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag, I3 <: LPileTag] extends Has3I[I0, I1, I2] {
    def input3: I3

    def plusKeyI3[KN: TypeTag]: Has4I[I0, I1, I2, I3#UpTag[KN]]
  }

  trait Has5I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag, I3 <: LPileTag, I4 <: LPileTag]
    extends Has4I[I0, I1, I2, I3] {
    def input4: I4

    def plusKeyI4[KN: TypeTag]: Has5I[I0, I1, I2, I3, I4#UpTag[KN]]
  }

  trait Has6I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag, I3 <: LPileTag, I4 <: LPileTag, I5 <: LPileTag]
    extends Has5I[I0, I1, I2, I3, I4] {
    def input5: I5

    def plusKeyI5[KN: TypeTag]: Has6I[I0, I1, I2, I3, I4, I5#UpTag[KN]]
  }

  trait Has7I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag, I3 <: LPileTag, I4 <: LPileTag, I5 <: LPileTag,
  I6 <: LPileTag]
    extends Has6I[I0, I1, I2, I3, I4, I5] {
    def input6: I6

    def plusKeyI6[KN: TypeTag]: Has7I[I0, I1, I2, I3, I4, I5, I6#UpTag[KN]]
  }

  trait Has1O[O0 <: LPileTag] extends LMethodTag {
    def output0: O0
  }

  trait Has2O[O0 <: LPileTag, O1 <: LPileTag] extends Has1O[O0] {
    def output1: O1
  }

  trait Has3O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag] extends Has2O[O0, O1] {
    def output2: O2
  }

  trait Has4O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag, O3 <: LPileTag] extends Has3O[O0, O1, O2] {
    def output3: O3
  }

  trait Has5O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag, O3 <: LPileTag, O4 <: LPileTag]
    extends Has4O[O0, O1, O2, O3] {
    def output4: O4
  }

  trait Has6O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag, O3 <: LPileTag, O4 <: LPileTag, O5 <: LPileTag]
    extends Has5O[O0, O1, O2, O3, O4] {
    def output5: O5
  }

  trait Has7O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag, O3 <: LPileTag, O4 <: LPileTag, O5 <: LPileTag,
  O6 <: LPileTag]
    extends Has6O[O0, O1, O2, O3, O4, O5] {
    def output6: O6
  }

}

trait LMethodTag {
  def inputs: Seq[LPileTag]

  def outputs: Seq[LPileTag]

  def plusKey[KN: TypeTag]: LMethodTag
}
