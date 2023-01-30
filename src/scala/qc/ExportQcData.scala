object ExportQcData extends loamstream.LoamFile {

  /**
    * Export Qc Data
    *  Description:
    *    Write config set variant filters to file
    *    Filter variants in matrix table, sampling randomly down to a set variant count if necessary
    *    Generate filtered Plink files
    *    Prune variants for QC
    *  Requires: Hail, Plink
    */

  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  import Fxns._
  
  final case class CfgException(s: String) extends Exception(s)
  
  def ExportQcData(array: ConfigArray): Unit = {
  
    var vFilters = Seq[String]()
    array.qcVariantFilters match {
      case Some(l) =>
        vFilters = vFilters ++ variantFiltersToPrintableList(cfg = projectConfig, filters = l)
      case None => ()
    }
  
    vFilters.size match {
  
      case n if n > 0 =>
  
        drmWith(imageName = s"${utils.image.imgTools}") {
        
          cmd"""echo "${vFilters.mkString("\n")}" > ${arrayStores(array).filteredData.variantFilters.local.get}"""
            .out(arrayStores(array).filteredData.variantFilters.local.get)
            .tag(s"${arrayStores(array).filteredData.variantFilters.local.get}".split("/").last)
        
        }
  
      case _ =>
  
        drmWith(imageName = s"${utils.image.imgTools}") {
        
          cmd"""touch ${arrayStores(array).filteredData.variantFilters.local.get}"""
            .out(arrayStores(array).filteredData.variantFilters.local.get)
            .tag(s"${arrayStores(array).filteredData.variantFilters.local.get}".split("/").last)
        
        }
  
    }
  
    val sampleN = array.qcVariantSampleN.getOrElse("") match {
      case "" => ""
      case _ =>
        s"--sample-n ${array.qcVariantSampleN.get}"
    }
    
    val sampleSeed = array.qcVariantSampleSeed.getOrElse("") match {
      case "" => ""
      case _ =>
        s"--sample-seed ${array.qcVariantSampleSeed.get}"
    }
    
    projectConfig.hailCloud match {
    
      case true =>
  
        local {
    
          googleCopy(arrayStores(array).filteredData.variantFilters.local.get, arrayStores(array).filteredData.variantFilters.google.get)
    
        }

        // has failed in past with pre-emptible workers
        google {
        
          hail"""${utils.python.pyHailExportQcData} --
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.google.get}
            --regions-exclude ${projectStores.regionsExclude.google.get}
            --variant-filters ${arrayStores(array).filteredData.variantFilters.google.get}
            ${sampleN}
            ${sampleSeed}
            --variants-out ${arrayStores(array).filteredData.variantMetrics.google.get}
            --plink-out ${arrayStores(array).filteredData.plink.base.google.get}
            --cloud
            --log ${arrayStores(array).filteredData.hailLog.google.get}"""
            .in(arrayStores(array).refData.mt.google.get, projectStores.regionsExclude.google.get, arrayStores(array).filteredData.variantFilters.google.get)
            .out(arrayStores(array).filteredData.plink.data.google.get :+ arrayStores(array).filteredData.variantMetrics.google.get :+ arrayStores(array).filteredData.hailLog.google.get)
            .tag(s"${arrayStores(array).filteredData.plink.base.local.get}.pyHailExportQcData".split("/").last)
        
        }
    
        local {
    
          googleCopy(arrayStores(array).filteredData.plink.data.google.get, arrayStores(array).filteredData.plink.data.local.get)
          googleCopy(arrayStores(array).filteredData.variantMetrics.google.get, arrayStores(array).filteredData.variantMetrics.local.get)
          googleCopy(arrayStores(array).filteredData.hailLog.google.get, arrayStores(array).filteredData.hailLog.local.get)
    
        }
    
      case false =>
    
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyHailExportQcData}
            --driver-memory ${projectConfig.resources.matrixTableHail.mem}
            --executor-memory ${projectConfig.resources.matrixTableHail.mem}
            --tmp-dir ${projectStores.tmpDir}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.local.get}
            --regions-exclude ${projectStores.regionsExclude.local.get}
            --variant-filters ${arrayStores(array).filteredData.variantFilters.local.get}
            ${sampleN}
            ${sampleSeed}
            --variants-out ${arrayStores(array).filteredData.variantMetrics.local.get}
            --plink-out ${arrayStores(array).filteredData.plink.base.local.get}
            --log ${arrayStores(array).filteredData.hailLog.local.get}"""
            .in(arrayStores(array).refData.mt.local.get, projectStores.regionsExclude.local.get, arrayStores(array).filteredData.variantFilters.local.get, projectStores.tmpDir)
            .out(arrayStores(array).filteredData.plink.data.local.get :+ arrayStores(array).filteredData.variantMetrics.local.get :+ arrayStores(array).filteredData.hailLog.local.get)
            .tag(s"${arrayStores(array).filteredData.plink.base.local.get}.pyHailExportQcData".split("/").last)
        
        }
    
    }

    val outChr = projectConfig.referenceGenome match {
      case "GRCh37" => "MT"
      case "GRCh38" => "chrM"
    }
    
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.highMemPlink.cpus, mem = projectConfig.resources.highMemPlink.mem, maxRunTime = projectConfig.resources.highMemPlink.maxRunTime) {
    
      cmd"""${utils.binary.binPlink}
        --bfile ${arrayStores(array).filteredData.plink.base.local.get}
        --allow-no-sex
        --indep-pairwise 1000kb 1 0.2
        --out ${arrayStores(array).filteredData.plink.base.local.get}
        --memory ${projectConfig.resources.highMemPlink.mem * 0.9 * 1000}
        --seed 1"""
        .in(arrayStores(array).filteredData.plink.data.local.get)
        .out(arrayStores(array).filteredData.pruneIn)
        .tag(s"${arrayStores(array).filteredData.pruneIn}".split("/").last)
    
      cmd"""${utils.binary.binPlink}
        --bfile ${arrayStores(array).filteredData.plink.base.local.get}
        --allow-no-sex 
        --extract ${arrayStores(array).filteredData.pruneIn}
        --output-chr ${outChr}
        --keep-allele-order
        --make-bed
        --out ${arrayStores(array).prunedData.plink.base}
        --memory ${projectConfig.resources.highMemPlink.mem * 0.9 * 1000}
        --seed 1"""
        .in(arrayStores(array).filteredData.plink.data.local.get :+ arrayStores(array).filteredData.pruneIn)
        .out(arrayStores(array).prunedData.plink.data)
        .tag(s"${arrayStores(array).prunedData.plink.base}".split("/").last)
    
    }
  
  }

}
