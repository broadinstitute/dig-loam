object ExportCleanBgen extends loamstream.LoamFile {

  /**
    * Export Clean Bgen File
    *  Description:
    *    Generate clean bgen file
    *  Requires: Python2, Bgen
    */
  import ProjectConfig._
  import ArrayStores._
  
  final case class CfgException(s: String) extends Exception(s)
  
  def ExportCleanBgen(array: ConfigArray): Unit = {

    val outChr = projectConfig.referenceGenome match {
      case "GRCh37" => "MT"
      case "GRCh38" => "M"
    }

    drmWith(imageName = s"${utils.image.imgPlink2}", cores = projectConfig.resources.standardPlinkMultiCpu.cpus, mem = projectConfig.resources.standardPlinkMultiCpu.mem, maxRunTime = projectConfig.resources.standardPlinkMultiCpu.maxRunTime) {

      cmd"""${utils.binary.binPlink2}
        --vcf ${arrayStores(array).cleanVcf.vcf.data.local.get}
        --double-id
        --export bgen-1.2 'bits=8' ref-first id-paste=iid
        --set-all-var-ids @:#:\$$r:\$$a
        --new-id-max-allele-len ${array.varUidMaxAlleleLen}
        --output-chr ${outChr}
        --out ${arrayStores(array).cleanBgen.get.base.local.get}
        --memory ${projectConfig.resources.standardPlinkMultiCpu.mem * projectConfig.resources.standardPlinkMultiCpu.cpus * 0.9 * 1000}
        --keep-allele-order"""
        .in(arrayStores(array).cleanVcf.vcf.data.local.get, arrayStores(array).cleanVcf.vcf.tbi.local.get)
        .out(arrayStores(array).cleanBgen.get.data.local.get, arrayStores(array).cleanBgen.get.sample.local.get)
        .tag(s"${arrayStores(array).cleanBgen.get.data.local.get}".split("/").last)
    
    }
    
    drmWith(imageName = s"${utils.image.imgBgen}", cores = projectConfig.resources.bgenix.cpus, mem = projectConfig.resources.tabix.mem, maxRunTime = projectConfig.resources.bgenix.maxRunTime) {
  
      cmd"""${utils.binary.binBgenix} -g ${arrayStores(array).cleanBgen.get.data.local.get} -index"""
        .in(arrayStores(array).cleanBgen.get.data.local.get)
        .out(arrayStores(array).cleanBgen.get.bgi.local.get)
        .tag(s"${arrayStores(array).cleanBgen.get.bgi.local.get}".split("/").last)
  
    }
  
  }

}
