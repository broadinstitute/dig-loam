package loamstream.model.calls

import loamstream.model.kinds.LKind
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LPileCall {
  def getPreexisting(sig: LSig, kind: LKind, id: String): LPileCall = getPreexisting(LPile(sig, kind), id)

  def getPreexisting(pile: LPile, id: String): LPileCall = LPileCall(pile, LRecipe.preExistingCheckout(id, pile))

  def apply(sig: LSig, kind: LKind, recipe: LRecipe): LPileCall = apply(LPile(sig, kind), recipe)
}

case class LPileCall(pile: LPile, recipe: LRecipe) {
  def extractKey(index: Int, output: LPile, kind: LKind) =
    LPileCall(LPile(LSig.Set(Seq(pile.sig.keyTypes(index))), kind), LRecipe.keyExtraction(pile, output, index))
}
