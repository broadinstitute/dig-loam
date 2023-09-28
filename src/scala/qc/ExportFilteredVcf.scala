object ExportFilteredVcf extends loamstream.LoamFile {

  /**
    * Export Clean VCF File
    *  Description:
    *    Generate clean vcf file
    *  Requires: Hail, Python
    */
  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  
  final case class CfgException(s: String) extends Exception(s)
  
  def ExportFilteredVcf(array: ConfigArray): Unit = {
  
    projectConfig.hailCloud match {
    
      case true =>
    
        google {
        
          hail"""${utils.python.pyHailExportVcf} --
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.google.get}
            --vcf-out ${arrayStores(array).filteredVcf.vcf.data.google.get}
            --samples-remove ${arrayStores(array).filterQc.samplesExclude.google.get},${arrayStores(array).filterPostQc.samplesExclude.google.get}
            --variants-remove ${arrayStores(array).filterPostQc.variantsExclude.google.get}
            --cloud
            --log ${arrayStores(array).filteredVcf.hailLog.google.get}"""
            .in(arrayStores(array).refData.mt.google.get, arrayStores(array).filterQc.samplesExclude.google.get, arrayStores(array).filterPostQc.samplesExclude.google.get, arrayStores(array).filterPostQc.variantsExclude.google.get)
            .out(arrayStores(array).filteredVcf.vcf.data.google.get, arrayStores(array).filteredVcf.hailLog.google.get)
            .tag(s"${arrayStores(array).filteredVcf.vcf.base.local.get}.pyHailExportVcf".split("/").last)
        
        }
    
        local {
    
          googleCopy(arrayStores(array).filteredVcf.vcf.data.google.get, arrayStores(array).filteredVcf.vcf.data.local.get)
          googleCopy(arrayStores(array).filteredVcf.hailLog.google.get, arrayStores(array).filteredVcf.hailLog.local.get)
    
        }
    
      case false =>
    
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyHailExportVcf}
            --driver-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
            --executor-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
            --tmp-dir ${projectStores.tmpDir}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.local.get}
            --vcf-out ${arrayStores(array).filteredVcf.vcf.data.local.get}
            --samples-remove ${arrayStores(array).filterQc.samplesExclude.local.get},${arrayStores(array).filterPostQc.samplesExclude.local.get}
            --variants-remove ${arrayStores(array).filterPostQc.variantsExclude.local.get}
            --log ${arrayStores(array).filteredVcf.hailLog.local.get}"""
            .in(arrayStores(array).refData.mt.local.get, arrayStores(array).filterQc.samplesExclude.local.get, arrayStores(array).filterPostQc.samplesExclude.local.get, arrayStores(array).filterPostQc.variantsExclude.local.get, projectStores.tmpDir)
            .out(arrayStores(array).filteredVcf.vcf.data.local.get, arrayStores(array).filteredVcf.hailLog.local.get)
            .tag(s"${arrayStores(array).filteredVcf.vcf.base.local.get}.pyHailExportVcf".split("/").last)
        
        }
    
    }
  
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.tabix.cpus, mem = projectConfig.resources.tabix.mem, maxRunTime = projectConfig.resources.tabix.maxRunTime) {
  
      cmd"""${utils.binary.binTabix} -p vcf ${arrayStores(array).filteredVcf.vcf.data.local.get}"""
        .in(arrayStores(array).filteredVcf.vcf.data.local.get)
        .out(arrayStores(array).filteredVcf.vcf.tbi.local.get)
        .tag(s"${arrayStores(array).filteredVcf.vcf.tbi.local.get}".split("/").last)
  
    }
  
  }

}
