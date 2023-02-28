object Kinship extends loamstream.LoamFile {

  /**
    * Kinship Step
    *  Description:
    *    Calculate kinship to identify duplicates and any samples exhibiting abnormal (excessive) sharing
    *    Calculate fam sizes to identify extreme relatedness
    *  Requires: Plink2 (King), R
    *  Notes:
    *     King is preferred to Plink or Hail based IBD calcs due to robust algorithm handling of population stratification. Plink2 now contains a King-robust option. This step should be followed by a visual inspection for duplicates or excessive sharing
    * King only writes the '.kin0' file if families are found, so a bash script is used to write an empty file in that case
    */

  import ProjectConfig._
  import ArrayStores._
  
  def Kinship(array: ConfigArray): Unit = {
  
    drmWith(imageName = s"${utils.image.imgPlink2}", cores = projectConfig.resources.highMemPlink.cpus, mem = projectConfig.resources.highMemPlink.mem, maxRunTime = projectConfig.resources.highMemPlink.maxRunTime) {
  
      cmd"""${utils.binary.binPlink2}
        --bfile ${arrayStores(array).filteredData.plink.base.local.get}
        --make-king-table
        --king-table-filter 0.0884
        --out ${arrayStores(array).kinshipData.base}
        --memory ${projectConfig.resources.highMemPlink.mem * 0.9 * 1000}
        && sed -i 's/#//g' ${arrayStores(array).kinshipData.kin0}"""
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
