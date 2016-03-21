package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
object PileKinds {

  val variantKeyIndexInGenotypes = 0
  val sampleKeyIndexInGenotypes = 1

  val genotypeCallsByVariantAndSample = LSpecificKind("Genotype calls by sample and variant")
  val sampleIds = LSpecificKind("sample ids")

}
