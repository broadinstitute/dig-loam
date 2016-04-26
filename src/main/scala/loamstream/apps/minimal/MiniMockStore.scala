package loamstream.apps.minimal

import loamstream.model.id.LId
import loamstream.model.kinds.instances.StoreKinds
import loamstream.model.piles.{LPileSpec, LSig}
import loamstream.model.values.LType.LTuple.{LTuple1, LTuple2}
import loamstream.model.values.LType.{LGenotype, LSampleId, LString, LVariantId}
import loamstream.model.StoreBase
import loamstream.tools.core.CoreStore
import loamstream.Sigs

/**
  * LoamStream
  * Created by oliverr on 3/29/2016.
  */
object MiniMockStore {

  val genotypesCassandraTable: StoreBase = CoreStore(
      "Cassandra genotype calls table", 
      LPileSpec(Sigs.variantAndSampleToGenotype, StoreKinds.genotypesCassandraTable))
      
  val sampleIdsCassandraTable: StoreBase = CoreStore(
      "Cassandra sample ids table.", 
      LPileSpec(Sigs.setOf(LSampleId), StoreKinds.sampleIdsCassandraTable))

  val stores = Set[StoreBase](genotypesCassandraTable, sampleIdsCassandraTable)
}
