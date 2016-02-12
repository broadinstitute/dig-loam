package loamstream.apps

import loamstream.model.calls.LMapCall
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.recipes.LCheckoutPreexisting

/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App {

  class VariantId

  class GenotypeCall

  val genotypeCalls =
    LMapCall.apply[(String, VariantId), GenotypeCall](LCheckoutPreexisting("myGenotypecalls"),
      PileKinds.genotypeCallsBySampleAndVariant).get
  val sampleIds = genotypeCalls.extractKey(0, PileKinds.sampleIds)

  println(genotypeCalls)
  println(sampleIds)
  println("Yo!")

}
