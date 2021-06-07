object ExportCleanVcf extends loamstream.LoamFile {

  /**
    * Export Clean VCF File
    *  Description:
    *    Generate clean vcf file
    *  Requires: Hail, Python
    */
  import ProjectConfig._
  import ArrayStores._
  
  final case class CfgException(s: String) extends Exception(s)
  
  def ExportCleanVcf(array: ConfigArray): Unit = {
  
    projectConfig.hailCloud match {
    
      case true =>
    
        google {
        
          hail"""${utils.python.pyHailExportCleanVcf} --
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.google.get}
            --vcf-out ${arrayStores(array).cleanVcf.vcf.data.google.get}
            --samples-remove ${arrayStores(array).filterQc.samplesExclude.google.get},${arrayStores(array).filterPostQc.samplesExclude.google.get}
            --variants-remove ${arrayStores(array).filterPostQc.variantsExclude.google.get}
            --cloud
            --log ${arrayStores(array).cleanVcf.hailLog.google.get}"""
            .in(arrayStores(array).refData.mt.google.get, arrayStores(array).filterQc.samplesExclude.google.get, arrayStores(array).filterPostQc.samplesExclude.google.get, arrayStores(array).filterPostQc.variantsExclude.google.get)
            .out(arrayStores(array).cleanVcf.vcf.data.google.get, arrayStores(array).cleanVcf.hailLog.google.get)
            .tag(s"${arrayStores(array).cleanVcf.vcf.base.local.get}.pyHailExportCleanVcf".split("/").last)
        
        }
    
        local {
    
          googleCopy(arrayStores(array).cleanVcf.vcf.data.google.get, arrayStores(array).cleanVcf.vcf.data.local.get)
          googleCopy(arrayStores(array).cleanVcf.hailLog.google.get, arrayStores(array).cleanVcf.hailLog.local.get)
    
        }
    
      case false =>
    
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyHailExportCleanVcf}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.local.get}
            --vcf-out ${arrayStores(array).cleanVcf.vcf.data.local.get}
            --samples-remove ${arrayStores(array).filterQc.samplesExclude.local.get},${arrayStores(array).filterPostQc.samplesExclude.local.get}
            --variants-remove ${arrayStores(array).filterPostQc.variantsExclude.local.get}
            --log ${arrayStores(array).cleanVcf.hailLog.local.get}"""
            .in(arrayStores(array).refData.mt.local.get, arrayStores(array).filterQc.samplesExclude.local.get, arrayStores(array).filterPostQc.samplesExclude.local.get, arrayStores(array).filterPostQc.variantsExclude.local.get)
            .out(arrayStores(array).cleanVcf.vcf.data.local.get, arrayStores(array).cleanVcf.hailLog.local.get)
            .tag(s"${arrayStores(array).cleanVcf.vcf.base.local.get}.pyHailExportCleanVcf".split("/").last)
        
        }
    
    }
  
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.tabix.cpus, mem = projectConfig.resources.tabix.mem, maxRunTime = projectConfig.resources.tabix.maxRunTime) {
  
      cmd"""${utils.binary.binTabix} -p vcf ${arrayStores(array).cleanVcf.vcf.data.local.get}"""
        .in(arrayStores(array).cleanVcf.vcf.data.local.get)
        .out(arrayStores(array).cleanVcf.vcf.tbi.local.get)
        .tag(s"${arrayStores(array).cleanVcf.vcf.tbi.local.get}".split("/").last)
  
    }
  
  }

}
