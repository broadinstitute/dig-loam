object Load extends loamstream.LoamFile {

  /**
    * Load Step
    *  Description: Generate the Hail matrix table from VCF file
    *  Requires: Hail v0.2
    */
  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  import Fxns._
  
  def Load(array: ConfigArray): Unit = {
  
    val minPartitions =  array.minPartitions.getOrElse("") match { case "" => ""; case _ => s"--min-partitions ${array.minPartitions.get}" }
    val gqThreshold =  array.gqThreshold.getOrElse("") match { case "" => ""; case _ => s"--gq-threshold ${array.gqThreshold.get}" }
  
    val inputType = array.technology + "_" + array.format
  
    projectConfig.hailCloud match {
  
      case true =>
  
        val inputOptionString = (array.technology, array.format) match {
          case (m,n) if inputTypesSeqVcf.contains((m,n)) => s"--vcf-in ${arrayStores(array).refData.vcf.get.data.google.get.toString.split("@")(1)}"
          case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) => s"--plink-in ${arrayStores(array).refData.plink.get.base.google.get}"
          case _ => throw new CfgException("Load: invalid technology and format combination: " + array.technology + ", " + array.format + " for array " + array.id)
        }
  
        val inputFiles = (array.technology, array.format) match {
          case (m,n) if inputTypesSeqVcf.contains((m,n)) => Seq(projectStores.hailUtils.google.get, arrayStores(array).refData.vcf.get.data.google.get, arrayStores(array).refData.vcf.get.tbi.google.get, projectStores.sampleFile.google.get)
          case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) => arrayStores(array).refData.plink.get.data.google.get :+ projectStores.hailUtils.google.get :+ projectStores.sampleFile.google.get
          case _ => throw new CfgException("Load: invalid input type " + inputType + " for array " + array.id)
        }
  
        val tagString = (array.technology, array.format) match {
          case (m,n) if inputTypesSeqVcf.contains((m,n)) => s"${arrayStores(array).refData.vcf.get.base.local.get}.pyHailLoad".split("/").last
          case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) => s"${arrayStores(array).refData.plink.get.base.local.get}.pyHailLoad".split("/").last
          case _ => throw new CfgException("Load: invalid technology and format combination: " + array.technology + ", " + array.format + " for array " + array.id)
        }
  
        (array.technology, array.format) match {
          case (m,n) if inputTypesSeqVcf.contains((m,n)) => 
            local {
              googleCopy(arrayStores(array).refData.vcf.get.data.local.get, arrayStores(array).refData.vcf.get.data.google.get)
              googleCopy(arrayStores(array).refData.vcf.get.tbi.local.get, arrayStores(array).refData.vcf.get.tbi.google.get)
            }
          case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) =>
            local {
              googleCopy(arrayStores(array).refData.plink.get.data.local.get, arrayStores(array).refData.plink.get.data.google.get)
            }
          case _ => throw new CfgException("Load: invalid technology and format combination: " + array.technology + ", " + array.format + " for array " + array.id)
        }
  
        googleWith(projectConfig.cloudResources.mtCluster) {
        
          hail"""${utils.python.pyHailLoad} --
            --reference-genome ${projectConfig.referenceGenome}
            ${minPartitions}
            ${gqThreshold}
            --cloud
            --hail-utils ${projectStores.hailUtils.google.get}
            --log ${arrayStores(array).refData.hailLog.google.get}
            ${inputOptionString}
            --sample-in ${projectStores.sampleFile.google.get}
            --id-col ${projectConfig.sampleFileId}
            --variant-metrics-out ${arrayStores(array).refData.variantMetrics.google.get}
            --sex-col ${projectConfig.sampleFileSrSex}
            --male-code ${projectConfig.sampleFileMaleCode}
            --female-code ${projectConfig.sampleFileFemaleCode}
            --sexcheck-out ${arrayStores(array).sexcheckData.sexcheck.google.get}
            --sexcheck-problems-out ${arrayStores(array).sexcheckData.problems.google.get}
            --sites-vcf-out ${arrayStores(array).refData.sitesVcf.google.get}
            --mt-checkpoint ${arrayStores(array).refData.mtCheckpoint.google.get}
            --mt-out ${arrayStores(array).refData.mt.google.get}"""
            .in(inputFiles)
            .out(arrayStores(array).refData.mt.google.get, arrayStores(array).refData.hailLog.google.get, arrayStores(array).refData.variantMetrics.google.get, arrayStores(array).sexcheckData.sexcheck.google.get, arrayStores(array).sexcheckData.problems.google.get, arrayStores(array).refData.sitesVcf.google.get)
            .tag(tagString)
        
        }
  
        local {
  
          googleCopy(arrayStores(array).refData.hailLog.google.get, arrayStores(array).refData.hailLog.local.get)
          googleCopy(arrayStores(array).refData.variantMetrics.google.get, arrayStores(array).refData.variantMetrics.local.get)
          googleCopy(arrayStores(array).refData.sitesVcf.google.get, arrayStores(array).refData.sitesVcf.local.get)
          googleCopy(arrayStores(array).sexcheckData.sexcheck.google.get, arrayStores(array).sexcheckData.sexcheck.local.get)
          googleCopy(arrayStores(array).sexcheckData.problems.google.get, arrayStores(array).sexcheckData.problems.local.get)

          cmd"""${gsutilBinaryOpt.get} -m rm -r ${arrayStores(array).refData.mtCheckpoint.google.get}"""
            .in(arrayStores(array).refData.sitesVcf.google.get)
            .tag(s"${arrayStores(array).refData.sitesVcf.local.get}.mtCheckpoint.remove".split("/").last)
  
        }
  
      case false =>
  
        val inputOptionString = (array.technology, array.format) match {
          case (m,n) if inputTypesSeqVcf.contains((m,n)) => s"--vcf-in ${arrayStores(array).refData.vcf.get.data.local.get.toString.split("@")(1)}"
          case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) => s"--plink-in ${arrayStores(array).refData.plink.get.base.local.get}"
          case _ => throw new CfgException("Load: invalid technology and format combination: " + array.technology + ", " + array.format + " for array " + array.id)
        }
  
        val inputFiles = (array.technology, array.format) match {
          case (m,n) if inputTypesSeqVcf.contains((m,n)) => Seq(arrayStores(array).refData.vcf.get.data.local.get, arrayStores(array).refData.vcf.get.tbi.local.get, projectStores.sampleFile.local.get)
          case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) => arrayStores(array).refData.plink.get.data.local.get :+ projectStores.sampleFile.local.get
          case _ => throw new CfgException("Load: invalid technology and format combination: " + array.technology + ", " + array.format + " for array " + array.id)
        }
  
        val tagString = (array.technology, array.format) match {
          case (m,n) if inputTypesSeqVcf.contains((m,n)) => s"${arrayStores(array).refData.vcf.get.base.local.get}.pyHailLoad".split("/").last
          case (o,p) if (inputTypesGwasVcf ++ inputTypesPlink).contains((o,p)) => s"${arrayStores(array).refData.plink.get.base.local.get}.pyHailLoad".split("/").last
          case _ => throw new CfgException("Load: invalid technology and format combination: " + array.technology + ", " + array.format + " for array " + array.id)
        }
  
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
  
          cmd"""${utils.binary.binPython} ${utils.python.pyHailLoad}
            --reference-genome ${projectConfig.referenceGenome}
            ${minPartitions}
            ${gqThreshold}
            --log ${arrayStores(array).refData.hailLog.local.get}
            ${inputOptionString}
            --sample-in ${projectStores.sampleFile.local.get}
            --id-col ${projectConfig.sampleFileId}
            --variant-metrics-out ${arrayStores(array).refData.variantMetrics.local.get}
            --sex-col ${projectConfig.sampleFileSrSex}
            --male-code ${projectConfig.sampleFileMaleCode}
            --female-code ${projectConfig.sampleFileFemaleCode}
            --sexcheck-out ${arrayStores(array).sexcheckData.sexcheck.local.get}
            --sexcheck-problems-out ${arrayStores(array).sexcheckData.problems.local.get}
            --sites-vcf-out ${arrayStores(array).refData.sitesVcf.local.get}
            --mt-checkpoint ${arrayStores(array).refData.mtCheckpoint.local.get}
            --mt-out ${arrayStores(array).refData.mt.local.get}"""
            .in(inputFiles)
            .out(arrayStores(array).refData.mt.local.get, arrayStores(array).refData.mtCheckpoint.local.get, arrayStores(array).refData.hailLog.local.get, arrayStores(array).refData.variantMetrics.local.get, arrayStores(array).sexcheckData.sexcheck.local.get, arrayStores(array).sexcheckData.problems.local.get, arrayStores(array).refData.sitesVcf.local.get)
            .tag(tagString)
  
        }
  
    }
  
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.tabix.cpus, mem = projectConfig.resources.tabix.mem, maxRunTime = projectConfig.resources.tabix.maxRunTime) {
    
      cmd"""${utils.binary.binTabix} -p vcf ${arrayStores(array).refData.sitesVcf.local.get}"""
        .in(arrayStores(array).refData.sitesVcf.local.get)
        .out(arrayStores(array).refData.sitesVcfTbi)
        .tag(s"${arrayStores(array).refData.sitesVcfTbi}".split("/").last)
    
    }
  
  }

}
