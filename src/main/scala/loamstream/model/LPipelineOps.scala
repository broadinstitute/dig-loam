package loamstream.model

import loamstream.model.kinds.LKind
import loamstream.model.values.LType.LTuple.LTuple1
import loamstream.tools.core.CoreStore

/**
  * LoamStream
  * Created by oliverr on 2/25/2016.
  */
object LPipelineOps {

  def extractKeyPile(inputPile: Store, index: Int, kind: LKind): Store = {
    //TODO: NB: Fragile, what if no type at index?
    val keyType = inputPile.spec.sig.keyTypes.asSeq(index)
    
    CoreStore("Extract Keys", LSig.Set.of(keyType), kind)
  }

  def extractKeyRecipe(inputPile: Store, index: Int, outputPile: Store): ToolBase = {
    ToolBase.keyExtraction(inputPile, outputPile, index)
  }

  def extractKey(inputPile: Store, index: Int, outputPile: Store, kind: LKind): (Store, ToolBase) = {
    val outputPile = extractKeyPile(inputPile, index, kind)
    
    val recipe = extractKeyRecipe(inputPile, index, outputPile)
    
    (outputPile, recipe)
  }

  def importVcfRecipe(inputPile: Store, index: Int, outputPile: Store): ToolBase = {
    ToolBase.vcfImport(inputPile, outputPile, index)
  }

  def calculateSingletonsRecipe(inputPile: Store, index: Int, outputPile: Store): ToolBase = {
    ToolBase.singletonCalculation(inputPile, outputPile, index)
  }
}
