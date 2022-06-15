object Kinship extends loamstream.LoamFile {

  /**
    * Kinship Step
    *  Description:
    *    Calculate kinship to identify duplicates and any samples exhibiting abnormal (excessive) sharing
    *    Calculate fam sizes to identify extreme relatedness
    *  Requires: King, R
    *  Notes:
    *     King is preferred to Plink or Hail based IBD calcs due to robust algorithm handling of population stratification. This step should be followed by a visual inspection for duplicates or excessive sharing
    * King only writes the '.kin0' file if families are found, so a bash script is used to write an empty file in that case
    */

  import ProjectConfig._
  import ArrayStores._
  
  def Kinship(array: ConfigArray): Unit = {
  
    drmWith(imageName = s"${utils.image.imgKing}", cores = projectConfig.resources.king.cpus, mem = projectConfig.resources.king.mem, maxRunTime = projectConfig.resources.king.maxRunTime) {
  
      cmd"""${utils.bash.shKing} ${utils.binary.binKing} ${arrayStores(array).filteredData.plink.base.local.get}.bed ${arrayStores(array).kinshipData.base} ${projectConfig.resources.king.cpus} ${arrayStores(array).kinshipData.log} ${arrayStores(array).kinshipData.kin0}"""
      .in(arrayStores(array).filteredData.plink.data.local.get)
      .out(arrayStores(array).kinshipData.log, arrayStores(array).kinshipData.kin0)
      .tag(s"${arrayStores(array).filteredData.plink.base.local.get}.binKing".split("/").last)
  
    }
  
    drmWith(imageName = s"${utils.image.imgR}") {
  
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rCalcKinshipFamSizes}
        --kin0 ${arrayStores(array).kinshipData.kin0}
        --out ${arrayStores(array).kinshipData.famSizes}"""
        .in(arrayStores(array).kinshipData.kin0)
        .out(arrayStores(array).kinshipData.famSizes)
        .tag(s"${arrayStores(array).kinshipData.famSizes}".split("/").last)
  
    }
  
  }

}
