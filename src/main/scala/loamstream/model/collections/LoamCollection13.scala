package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection13[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12] extends LoamCollection {

  type K = (K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12)

  def up[K13]: LoamCollection14[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12, K13]

}
