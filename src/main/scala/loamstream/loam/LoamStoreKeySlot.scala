package loamstream.loam

import loamstream.util.ValueBox

/** A key slot of a Loam store, defined by store and slot name */
final case class LoamStoreKeySlot(store: LoamStore, name: String)(implicit graphBox: ValueBox[LoamGraph]) {
  /** Specifies that this key slot has the same set of keys as that key slot (order may be different) */
  def setSameSetAs(oSlot: LoamStoreKeySlot): LoamStoreKeySlot = {
    graphBox.mutate(_.withKeysSameSet(this, oSlot))
    this
  }

  /** Specifies that this key slot has the same list of keys as that key slot (same order) */
  def setSameListAs(oSlot: LoamStoreKeySlot): LoamStoreKeySlot = {
    graphBox.mutate(_.withKeysSameList(this, oSlot))
    this
  }

  /** True if this and that slot have same same key set */
  def isSameSetAs(oSlot: LoamStoreKeySlot): Boolean = graphBox.get(_.areSameKeySets(this, oSlot))

  /** True if this and that slot have same same key list */
  def isSameListAs(oSlot: LoamStoreKeySlot): Boolean = graphBox.get(_.areSameKeyLists(this, oSlot))
}
