package loamstream.apps.minimal

import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe
import loamstream.model.values.LType.LTuple.LTuple2
import loamstream.model.values.LType.{LGenotype, LSampleId, LVariantId}
import loamstream.model.{LPipeline, LPipelineOps}

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
case class MiniPipeline(genotypesId: String) extends LPipeline {
  val genotypeCallsPile =
    LPile(genotypesId, LSig.Map(LTuple2(LVariantId, LSampleId), LGenotype),
      PileKinds.genotypeCallsByVariantAndSample)
  val genotypeCallsRecipe = LRecipe.preExistingCheckout(genotypesId, genotypeCallsPile)
  val sampleIdsPile =
    LPipelineOps.extractKeyPile(genotypeCallsPile, PileKinds.sampleKeyIndexInGenotypes, PileKinds.sampleIds)
  val sampleIdsRecipe = LPipelineOps.extractKeyRecipe(genotypeCallsPile, PileKinds.sampleKeyIndexInGenotypes,
    sampleIdsPile)

  val piles = Set(genotypeCallsPile, sampleIdsPile)
  val recipes = Set(genotypeCallsRecipe, sampleIdsRecipe)

}
