object ExportGenotypes extends loamstream.LoamFile {

  /**
    * Export VCF and BGEN Files
    *  Description:
    *    Generate vcf and bgen files
    *  Requires: Hail, Python
    */
  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  
  final case class CfgException(s: String) extends Exception(s)
  
  def ExportGenotypes(array: ConfigArray, filter: Boolean): Unit = {

    val outChr = projectConfig.referenceGenome match {
      case "GRCh37" => "MT"
      case "GRCh38" => "M"
    }

    filter match {

      case false =>
  
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
	    
        drmWith(imageName = s"${utils.image.imgPlink2}", cores = projectConfig.resources.standardPlinkMultiCpu.cpus, mem = projectConfig.resources.standardPlinkMultiCpu.mem, maxRunTime = projectConfig.resources.standardPlinkMultiCpu.maxRunTime) {
	    
          cmd"""${utils.binary.binPlink2}
            --vcf ${arrayStores(array).unfilteredVcf.vcf.data.local.get}
            --double-id
            --export bgen-1.2 'bits=8' ref-first id-paste=iid
            --set-all-var-ids @:#:\$$r:\$$a
            --new-id-max-allele-len ${array.varUidMaxAlleleLen}
            --output-chr ${outChr}
            --out ${arrayStores(array).unfilteredBgen.base.local.get}
            --memory ${projectConfig.resources.standardPlinkMultiCpu.mem * projectConfig.resources.standardPlinkMultiCpu.cpus * 0.9 * 1000}"""
            .in(arrayStores(array).unfilteredVcf.vcf.data.local.get, arrayStores(array).unfilteredVcf.vcf.tbi.local.get)
            .out(arrayStores(array).unfilteredBgen.data.local.get, arrayStores(array).unfilteredBgen.sample.local.get)
            .tag(s"${arrayStores(array).unfilteredBgen.data.local.get}".split("/").last)
        
        }
        
        drmWith(imageName = s"${utils.image.imgBgen}", cores = projectConfig.resources.bgenix.cpus, mem = projectConfig.resources.tabix.mem, maxRunTime = projectConfig.resources.bgenix.maxRunTime) {
	    
          cmd"""${utils.binary.binBgenix} -g ${arrayStores(array).unfilteredBgen.data.local.get} -index"""
            .in(arrayStores(array).unfilteredBgen.data.local.get)
            .out(arrayStores(array).unfilteredBgen.bgi.local.get)
            .tag(s"${arrayStores(array).unfilteredBgen.bgi.local.get}".split("/").last)
	    
        }

      case true =>

        projectConfig.hailCloud match {
    
          case true =>
        
            google {
            
              hail"""${utils.python.pyHailExportVcf} --
                --reference-genome ${projectConfig.referenceGenome}
                --mt-in ${arrayStores(array).refData.mt.google.get}
                --vcf-out ${arrayStores(array).filteredVcf.get.vcf.data.google.get}
                --samples-remove ${arrayStores(array).filterQc.samplesExclude.google.get},${arrayStores(array).filterPostQc.samplesExclude.google.get}
                --variants-remove ${arrayStores(array).filterPostQc.variantsExclude.google.get}
                --cloud
                --log ${arrayStores(array).filteredVcf.get.hailLog.google.get}"""
                .in(arrayStores(array).refData.mt.google.get, arrayStores(array).filterQc.samplesExclude.google.get, arrayStores(array).filterPostQc.samplesExclude.google.get, arrayStores(array).filterPostQc.variantsExclude.google.get)
                .out(arrayStores(array).filteredVcf.get.vcf.data.google.get, arrayStores(array).filteredVcf.get.hailLog.google.get)
                .tag(s"${arrayStores(array).filteredVcf.get.vcf.base.local.get}.pyHailExportVcf".split("/").last)
            
            }
        
            local {
        
              googleCopy(arrayStores(array).filteredVcf.get.vcf.data.google.get, arrayStores(array).filteredVcf.get.vcf.data.local.get)
              googleCopy(arrayStores(array).filteredVcf.get.hailLog.google.get, arrayStores(array).filteredVcf.get.hailLog.local.get)
        
            }
        
          case false =>
        
            drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
            
              cmd"""${utils.binary.binPython} ${utils.python.pyHailExportVcf}
                --driver-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
                --executor-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
                --tmp-dir ${projectStores.tmpDir}
                --reference-genome ${projectConfig.referenceGenome}
                --mt-in ${arrayStores(array).refData.mt.local.get}
                --vcf-out ${arrayStores(array).filteredVcf.get.vcf.data.local.get}
                --samples-remove ${arrayStores(array).filterQc.samplesExclude.local.get},${arrayStores(array).filterPostQc.samplesExclude.local.get}
                --variants-remove ${arrayStores(array).filterPostQc.variantsExclude.local.get}
                --log ${arrayStores(array).filteredVcf.get.hailLog.local.get}"""
                .in(arrayStores(array).refData.mt.local.get, arrayStores(array).filterQc.samplesExclude.local.get, arrayStores(array).filterPostQc.samplesExclude.local.get, arrayStores(array).filterPostQc.variantsExclude.local.get, projectStores.tmpDir)
                .out(arrayStores(array).filteredVcf.get.vcf.data.local.get, arrayStores(array).filteredVcf.get.hailLog.local.get)
                .tag(s"${arrayStores(array).filteredVcf.get.vcf.base.local.get}.pyHailExportVcf".split("/").last)
            
            }
        
        }
	    
        drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.tabix.cpus, mem = projectConfig.resources.tabix.mem, maxRunTime = projectConfig.resources.tabix.maxRunTime) {
	    
          cmd"""${utils.binary.binTabix} -p vcf ${arrayStores(array).filteredVcf.get.vcf.data.local.get}"""
            .in(arrayStores(array).filteredVcf.get.vcf.data.local.get)
            .out(arrayStores(array).filteredVcf.get.vcf.tbi.local.get)
            .tag(s"${arrayStores(array).filteredVcf.get.vcf.tbi.local.get}".split("/").last)
	    
        }

        drmWith(imageName = s"${utils.image.imgPlink2}", cores = projectConfig.resources.standardPlinkMultiCpu.cpus, mem = projectConfig.resources.standardPlinkMultiCpu.mem, maxRunTime = projectConfig.resources.standardPlinkMultiCpu.maxRunTime) {
	    
          cmd"""${utils.binary.binPlink2}
            --vcf ${arrayStores(array).filteredVcf.get.vcf.data.local.get}
            --double-id
            --export bgen-1.2 'bits=8' ref-first id-paste=iid
            --set-all-var-ids @:#:\$$r:\$$a
            --new-id-max-allele-len ${array.varUidMaxAlleleLen}
            --output-chr ${outChr}
            --out ${arrayStores(array).filteredBgen.get.base.local.get}
            --memory ${projectConfig.resources.standardPlinkMultiCpu.mem * projectConfig.resources.standardPlinkMultiCpu.cpus * 0.9 * 1000}"""
            .in(arrayStores(array).filteredVcf.get.vcf.data.local.get, arrayStores(array).filteredVcf.get.vcf.tbi.local.get)
            .out(arrayStores(array).filteredBgen.get.data.local.get, arrayStores(array).filteredBgen.get.sample.local.get)
            .tag(s"${arrayStores(array).filteredBgen.get.data.local.get}".split("/").last)
        
        }
        
        drmWith(imageName = s"${utils.image.imgBgen}", cores = projectConfig.resources.bgenix.cpus, mem = projectConfig.resources.tabix.mem, maxRunTime = projectConfig.resources.bgenix.maxRunTime) {
	    
          cmd"""${utils.binary.binBgenix} -g ${arrayStores(array).filteredBgen.get.data.local.get} -index"""
            .in(arrayStores(array).filteredBgen.get.data.local.get)
            .out(arrayStores(array).filteredBgen.get.bgi.local.get)
            .tag(s"${arrayStores(array).filteredBgen.get.bgi.local.get}".split("/").last)
	    
        }
  
    }

  }

}
