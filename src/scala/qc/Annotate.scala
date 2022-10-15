object Annotate extends loamstream.LoamFile {

  /**
    * Annotation
    *  Description:
    *    Annotate site vcf with VEP and load into Hail table
    *  Requires: VEP, Hail
    *  Notes:
    */
  import ProjectConfig._
  import ProjectStores._
  import ArrayStores._
  
  def Annotate(array: ConfigArray): Unit = {
  
    drmWith(imageName = s"${utils.image.imgEnsemblVep}", cores = projectConfig.resources.vep.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.vep.maxRunTime) {
  
      cmd"""${utils.bash.shAnnotate} ${arrayStores(array).refData.sitesVcf.local.get} ${projectConfig.resources.vep.cpus} ${projectStores.fasta} ${projectStores.vepCacheDir} ${projectStores.dbNSFP} ${arrayStores(array).refData.annotations.local.get} ${arrayStores(array).refData.annotationWarnings} ${arrayStores(array).refData.annotationHeader} ${projectConfig.referenceGenome} ${projectStores.gnomad}"""
      .in(arrayStores(array).refData.sitesVcf.local.get, projectStores.fasta, projectStores.vepCacheDir, projectStores.dbNSFP, projectStores.gnomad)
      .out(arrayStores(array).refData.annotations.local.get, arrayStores(array).refData.annotationWarnings, arrayStores(array).refData.annotationHeader)
      .tag(s"${arrayStores(array).refData.annotations.local.get}".split("/").last)
  
    }
  
    val minPartitions =  array.minPartitions.getOrElse("") match { case "" => ""; case _ => s"--min-partitions ${array.minPartitions.get}" }
  
    projectConfig.hailCloud match {
  
      case true =>
  
        local {
          googleCopy(arrayStores(array).refData.annotations.local.get, arrayStores(array).refData.annotations.google.get)
        }
  
        googleWith(projectConfig.cloudResources.variantHtCluster) {
        
          hail"""${utils.python.pyHailLoadAnnotations} --
            --annotations ${arrayStores(array).refData.annotations.google.get}
            --out ${arrayStores(array).refData.annotationsHt.google.get}
            ${minPartitions}
            --reference-genome ${projectConfig.referenceGenome}
            --cloud
            --log ${arrayStores(array).refData.annotationsHailLog.google.get}"""
              .in(arrayStores(array).refData.annotations.google.get)
              .out(arrayStores(array).refData.annotationsHt.google.get, arrayStores(array).refData.annotationsHailLog.google.get)
              .tag(s"${arrayStores(array).refData.annotationsHt.local.get}.google".split("/").last)
        
        }
  
      case false =>
  
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.tableHail.cpus, mem = projectConfig.resources.tableHail.mem, maxRunTime = projectConfig.resources.tableHail.maxRunTime) {
  
          cmd"""${utils.binary.binPython} ${utils.python.pyHailLoadAnnotations}
            --tmp-dir ${projectStores.tmpDir}
            --annotations ${arrayStores(array).refData.annotations.local.get}
            --out ${arrayStores(array).refData.annotationsHt.local.get}
            ${minPartitions}
            --reference-genome ${projectConfig.referenceGenome}
            --log ${arrayStores(array).refData.annotationsHailLog.local.get}"""
              .in(arrayStores(array).refData.annotations.local.get, projectStores.tmpDir)
              .out(arrayStores(array).refData.annotationsHt.local.get, arrayStores(array).refData.annotationsHailLog.local.get)
              .tag(s"${arrayStores(array).refData.annotationsHt.local.get}".split("/").last)
  
        }
  
    }
  
  }

}
