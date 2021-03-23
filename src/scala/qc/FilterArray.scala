object FilterArray extends loamstream.LoamFile {

  /**
    * Filter Array
    * Description:
    *   Filter samples and variants to generate array-level exclusions for analysis
    *  Requires: Hail
    */

  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  import Fxns._
  
  final case class CfgException(s: String) extends Exception(s)
  
  def FilterArray(array: ConfigArray): Unit = {
  
    var vFilters = Seq[String]()
    array.postQcVariantFilters match {
      case Some(l) =>
        vFilters = vFilters ++ variantFiltersToPrintableList(cfg = projectConfig, filters = l)
      case None => ()
    }
  
    var sFilters = Seq[String]()
    array.postQcSampleFilters match {
      case Some(l) =>
        sFilters = sFilters ++ sampleFiltersToPrintableList(cfg = projectConfig, filters = l)
      case None => ()
    }
  
    vFilters.size match {
  
      case n if n > 0 =>
  
        drmWith(imageName = s"${utils.image.imgTools}") {
        
          cmd"""echo "${vFilters.mkString("\n")}" > ${arrayStores(array).filterPostQc.vFilters.local.get}"""
            .out(arrayStores(array).filterPostQc.vFilters.local.get)
            .tag(s"${arrayStores(array).filterPostQc.vFilters.local.get}".split("/").last)
  
        }
  
      case _ =>
  
        drmWith(imageName = s"${utils.image.imgTools}") {
        
          cmd"""touch ${arrayStores(array).filterPostQc.vFilters.local.get}"""
            .out(arrayStores(array).filterPostQc.vFilters.local.get)
            .tag(s"${arrayStores(array).filterPostQc.vFilters.local.get}".split("/").last)
  
        }
  
    }
  
    sFilters.size match {
  
      case n if n > 0 =>
  
        drmWith(imageName = s"${utils.image.imgTools}") {
        
          cmd"""echo "${sFilters.mkString("\n")}" > ${arrayStores(array).filterPostQc.sFilters.local.get}"""
            .out(arrayStores(array).filterPostQc.sFilters.local.get)
            .tag(s"${arrayStores(array).filterPostQc.sFilters.local.get}".split("/").last)
        
        }
  
      case _ =>
  
        drmWith(imageName = s"${utils.image.imgTools}") {
        
          cmd"""touch ${arrayStores(array).filterPostQc.sFilters.local.get}"""
            .out(arrayStores(array).filterPostQc.sFilters.local.get)
            .tag(s"${arrayStores(array).filterPostQc.sFilters.local.get}".split("/").last)
        
        }
  
    }
  
    projectConfig.hailCloud match {
  
      case true =>
   
        local {
        
          googleCopy(arrayStores(array).filterQc.samplesExclude.local.get, arrayStores(array).filterQc.samplesExclude.google.get)
          googleCopy(arrayStores(array).filterPostQc.vFilters.local.get, arrayStores(array).filterPostQc.vFilters.google.get)
          googleCopy(arrayStores(array).filterPostQc.sFilters.local.get, arrayStores(array).filterPostQc.sFilters.google.get)
        
        }
  
        googleWith(projectConfig.cloudResources.mtCluster) {
  
          hail"""${utils.python.pyHailFilter} --
            --cloud
            --reference-genome ${projectConfig.referenceGenome}
            --hail-utils ${projectStores.hailUtils.google.get}
            --sample-filters ${arrayStores(array).filterPostQc.sFilters.google.get}
            --variant-filters ${arrayStores(array).filterPostQc.vFilters.google.get}
            --log ${arrayStores(array).filterPostQc.hailLog.google.get}
            --mt-in ${arrayStores(array).refData.mt.google.get}
            --samples-remove ${arrayStores(array).filterQc.samplesExclude.google.get}
            --samples-stats-out ${arrayStores(array).filterPostQc.samplesStats.google.get}
            --samples-exclude-out ${arrayStores(array).filterPostQc.samplesExclude.google.get}
            --variants-stats-out ${arrayStores(array).filterPostQc.variantsStats.google.get}
            --variants-exclude-out ${arrayStores(array).filterPostQc.variantsExclude.google.get}"""
            .in(projectStores.hailUtils.google.get, arrayStores(array).refData.mt.google.get, arrayStores(array).filterPostQc.sFilters.google.get, arrayStores(array).filterPostQc.vFilters.google.get, arrayStores(array).sexcheckData.sexcheck.google.get, arrayStores(array).filterQc.samplesExclude.google.get)
            .out(arrayStores(array).filterPostQc.samplesStats.google.get, arrayStores(array).filterPostQc.samplesExclude.google.get, arrayStores(array).filterPostQc.variantsStats.google.get, arrayStores(array).filterPostQc.variantsExclude.google.get, arrayStores(array).filterPostQc.hailLog.google.get)
            .tag(s"${arrayStores(array).filterPostQc.hailLog.local.get}.pyHailFilter.google".split("/").last)
        
        }
        
        local {
        
          googleCopy(arrayStores(array).filterPostQc.samplesStats.google.get, arrayStores(array).filterPostQc.samplesStats.local.get)
          googleCopy(arrayStores(array).filterPostQc.samplesExclude.google.get, arrayStores(array).filterPostQc.samplesExclude.local.get)
          googleCopy(arrayStores(array).filterPostQc.variantsStats.google.get, arrayStores(array).filterPostQc.variantsStats.local.get)
          googleCopy(arrayStores(array).filterPostQc.variantsExclude.google.get, arrayStores(array).filterPostQc.variantsExclude.local.get)
          googleCopy(arrayStores(array).filterPostQc.hailLog.google.get, arrayStores(array).filterPostQc.hailLog.local.get)
        
        }
  
      case false =>
  
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
  
          cmd"""${utils.binary.binPython} ${utils.python.pyHailFilter}
            --reference-genome ${projectConfig.referenceGenome}
            --sample-filters ${arrayStores(array).filterPostQc.sFilters.local.get}
            --variant-filters ${arrayStores(array).filterPostQc.vFilters.local.get}
            --log ${arrayStores(array).filterPostQc.hailLog.local.get}
            --mt-in ${arrayStores(array).refData.mt.local.get}
            --samples-remove ${arrayStores(array).filterQc.samplesExclude.local.get}
            --samples-stats-out ${arrayStores(array).filterPostQc.samplesStats.local.get}
            --samples-exclude-out ${arrayStores(array).filterPostQc.samplesExclude.local.get}
            --variants-stats-out ${arrayStores(array).filterPostQc.variantsStats.local.get}
            --variants-exclude-out ${arrayStores(array).filterPostQc.variantsExclude.local.get}"""
            .in(arrayStores(array).refData.mt.local.get, arrayStores(array).sexcheckData.sexcheck.local.get, arrayStores(array).filterPostQc.sFilters.local.get, arrayStores(array).filterPostQc.vFilters.local.get, arrayStores(array).filterQc.samplesExclude.local.get)
            .out(arrayStores(array).filterPostQc.samplesStats.local.get, arrayStores(array).filterPostQc.samplesExclude.local.get, arrayStores(array).filterPostQc.variantsStats.local.get, arrayStores(array).filterPostQc.variantsExclude.local.get, arrayStores(array).filterPostQc.hailLog.local.get)
            .tag(s"${arrayStores(array).filterPostQc.hailLog.local.get}.pyHailFilter".split("/").last)
  
        }
  
    }
  
  }

}
