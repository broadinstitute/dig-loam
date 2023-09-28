object ExportUnfilteredVcf extends loamstream.LoamFile {

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
  
  def ExportUnfilteredVcf(array: ConfigArray): Unit = {
  
    projectConfig.hailCloud match {
    
      case true =>
    
        google {
        
          hail"""${utils.python.pyHailExportVcf} --
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.google.get}
            --vcf-out ${arrayStores(array).unfilteredVcf.vcf.data.google.get}
            --cloud
            --log ${arrayStores(array).unfilteredVcf.hailLog.google.get}"""
            .in(arrayStores(array).refData.mt.google.get)
            .out(arrayStores(array).unfilteredVcf.vcf.data.google.get, arrayStores(array).unfilteredVcf.hailLog.google.get)
            .tag(s"${arrayStores(array).unfilteredVcf.vcf.base.local.get}.pyHailExportVcf".split("/").last)
        
        }
    
        local {
    
          googleCopy(arrayStores(array).unfilteredVcf.vcf.data.google.get, arrayStores(array).unfilteredVcf.vcf.data.local.get)
          googleCopy(arrayStores(array).unfilteredVcf.hailLog.google.get, arrayStores(array).unfilteredVcf.hailLog.local.get)
    
        }
    
      case false =>
    
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyHailExportVcf}
            --driver-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
            --executor-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
            --tmp-dir ${projectStores.tmpDir}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.local.get}
            --vcf-out ${arrayStores(array).unfilteredVcf.vcf.data.local.get}
            --log ${arrayStores(array).unfilteredVcf.hailLog.local.get}"""
            .in(arrayStores(array).refData.mt.local.get, projectStores.tmpDir)
            .out(arrayStores(array).unfilteredVcf.vcf.data.local.get, arrayStores(array).unfilteredVcf.hailLog.local.get)
            .tag(s"${arrayStores(array).unfilteredVcf.vcf.base.local.get}.pyHailExportVcf".split("/").last)
        
        }
    
    }
  
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.tabix.cpus, mem = projectConfig.resources.tabix.mem, maxRunTime = projectConfig.resources.tabix.maxRunTime) {
  
      cmd"""${utils.binary.binTabix} -p vcf ${arrayStores(array).unfilteredVcf.vcf.data.local.get}"""
        .in(arrayStores(array).unfilteredVcf.vcf.data.local.get)
        .out(arrayStores(array).unfilteredVcf.vcf.tbi.local.get)
        .tag(s"${arrayStores(array).unfilteredVcf.vcf.tbi.local.get}".split("/").last)
  
    }
  
  }

}
