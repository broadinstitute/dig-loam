package loamstream.model.collections.sets

import loamstream.model.collections.LoamCollection09

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet09[K00, K01, K02, K03, K04, K05, K06, K07, K08]
  extends LoamSet with LoamCollection09[K00, K01, K02, K03, K04, K05, K06, K07, K08] {

  override def up[K09]: LoamSet10[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09]

}
