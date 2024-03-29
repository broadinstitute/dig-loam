object Pca extends loamstream.LoamFile {

  /**
   * PCA
   *  Description:
   *    Calculate PCs for all non-outlier samples combined (to be used for adjustment during sample outlier removal)
   *  Requires: FlashPCA2
   */
  import ProjectConfig._
  import ArrayStores._
  
  def Pca(array: ConfigArray): Unit = {
  
    drmWith(imageName = s"${utils.image.imgFlashPca}", cores = projectConfig.resources.flashPca.cpus, mem = projectConfig.resources.flashPca.mem, maxRunTime = projectConfig.resources.flashPca.maxRunTime) {
    
      cmd"""${utils.binary.binFlashPca} --verbose
        --seed 1
        --numthreads ${projectConfig.resources.flashPca.cpus}
        --ndim 20
        --bfile ${arrayStores(array).prunedData.plink.base}
        --outpc ${arrayStores(array).pcaData.scores}
        --outvec ${arrayStores(array).pcaData.eigenVecs}
        --outload ${arrayStores(array).pcaData.loadings}
        --outval ${arrayStores(array).pcaData.eigenVals}
        --outpve ${arrayStores(array).pcaData.pve}
        --outmeansd ${arrayStores(array).pcaData.meansd}
        > ${arrayStores(array).pcaData.log}"""
        .in(arrayStores(array).prunedData.plink.data)
        .out(arrayStores(array).pcaData.log, arrayStores(array).pcaData.scores, arrayStores(array).pcaData.eigenVecs, arrayStores(array).pcaData.loadings, arrayStores(array).pcaData.eigenVals, arrayStores(array).pcaData.pve, arrayStores(array).pcaData.meansd)
        .tag(s"${arrayStores(array).pcaData.scores}".split("/").last)
    
    }
  
  }

}
