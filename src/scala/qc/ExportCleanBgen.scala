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
        --export bgen-1.2 'bits=8' ref-first
        --set-all-var-ids @:#:\$$r:\$$a
        --new-id-max-allele-len 1000
        --output-chr ${outChr}
        --out ${arrayStores(array).cleanBgen.get.base.local.get}
        --memory ${projectConfig.resources.standardPlinkMultiCpu.mem * projectConfig.resources.standardPlinkMultiCpu.cpus * 0.9 * 1000}
        --keep-allele-order
        && mv ${arrayStores(array).cleanBgen.get.sample.local.get} ${arrayStores(array).cleanBgen.get.sample.local.get}.tmp
        && (head -2 ${arrayStores(array).cleanBgen.get.sample.local.get}.tmp; sed '1,2d' ${arrayStores(array).cleanBgen.get.sample.local.get}.tmp | awk '{print $$2" "$$2" "$$3" "$$4}') > ${arrayStores(array).cleanBgen.get.sample.local.get}
        && rm ${arrayStores(array).cleanBgen.get.sample.local.get}.tmp"""
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
