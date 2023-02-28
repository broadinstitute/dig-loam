object Load extends loamstream.LoamFile {

  /**
    * Load into hail matrix table
    *  Description:
    *    Generate the Hail matrix table from harmonized VCF files
    *      Add missing genotype fields
    *      Split multi-allelic variants
    *      Run sexcheck
    *      Convert to unphased if needed
    *      Convert males to diploid on non-PAR X/Y and set females to missing on Y
    *      Calculate raw variant QC metrics
    *    Generate site VCF
    *  Requires: Hail, Tabix
    */

  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  import Fxns._
  
  def Load(array: ConfigArray): Unit = {
  
    val minPartitions =  array.minPartitions.getOrElse("") match { case "" => ""; case _ => s"--min-partitions ${array.minPartitions.get}" }
    val gqThreshold =  array.gqThreshold.getOrElse("") match { case "" => ""; case _ => s"--gq-threshold ${array.gqThreshold.get}" }
  
    val inputType = array.technology + "_" + array.format

    val srSexString = projectConfig.sampleFileSrSex match {
      case Some(_) => s"""--sex-col "${projectConfig.sampleFileSrSex.get}""""
      case None => ""
    }

    val maleCodeString = projectConfig.sampleFileMaleCode match {
      case Some(_) => s"""--male-code "${projectConfig.sampleFileMaleCode.get}""""
      case None => ""
    }

    val femaleCodeString = projectConfig.sampleFileFemaleCode match {
      case Some(_) => s"""--female-code "${projectConfig.sampleFileFemaleCode.get}""""
      case None => ""
    }
  
    projectConfig.hailCloud match {
  
      case true =>

        arrayStores(array).refData.vcf match {
          case Some(s) =>
		    for {
		      vcf <- s
		    } yield {
		      local {
		      	googleCopy(vcf.data.local.get, vcf.data.google.get)
		      	googleCopy(vcf.tbi.local.get, vcf.tbi.google.get)
		      }
		    }
          case None => ()
        }

        arrayStores(array).refData.rawMt match {
          case Some(s) =>
            local {
              cmd"""${gsutilBinaryOpt.get} -m cp -r ${s.data.local.get} ${s.data.google.get}"""
              .in(s.data.local.get)
              .out(s.data.google.get)
              .tag(s"${s.data.local.get}.googleCopy".split("/").last)
            }
          case None => ()
        }

        val dataInString = (arrayStores(array).refData.vcf, arrayStores(array).refData.rawMt) match {
          case (Some(s), None) =>
            (arrayStores(array).refData.vcf.get.map(e => e.data.google.get).toSeq ++ arrayStores(array).refData.vcf.get.map(e => e.tbi.google.get).toSeq) :+ projectStores.hailUtils.google.get :+ projectStores.dbSNPht.google.get :+ projectStores.sampleFile.google.get
          case (None, Some(t)) =>
            Seq(arrayStores(array).refData.rawMt.get.data.google.get, projectStores.hailUtils.google.get, projectStores.dbSNPht.google.get, projectStores.sampleFile.google.get)
          case _ => throw new CfgException("invalid input for pyHailLoad: either vcf or rawMt must be defined")
        }

        val hailLoadIn = (arrayStores(array).refData.vcf, arrayStores(array).refData.rawMt) match {
          case (Some(s), None) =>
            s"""--vcf-in "${arrayStores(array).refData.vcfGlob.get.google.get}""""
          case (None, Some(t)) =>
            s"""--mt-in ${arrayStores(array).refData.rawMt.get.data.google.get.toString.split("@")(1)}"""
          case _ => throw new CfgException("invalid input for pyHailLoad: either vcf or rawMt must be defined")
        }

        googleWith(projectConfig.cloudResources.mtCluster) {
		
          hail"""${utils.python.pyHailLoad} --
            --reference-genome ${projectConfig.referenceGenome}
            ${minPartitions}
            ${gqThreshold}
            --cloud
            --hail-utils ${projectStores.hailUtils.google.get}
            --log ${arrayStores(array).refData.hailLog.google.get}
            ${hailLoadIn}
            --dbsnp-ht ${projectStores.dbSNPht.google.get}
            --sample-in ${projectStores.sampleFile.google.get}
            --id-col ${projectConfig.sampleFileId}
            --variant-metrics-out ${arrayStores(array).refData.variantMetrics.google.get}
            ${srSexString}
            ${maleCodeString}
            ${femaleCodeString}
            --sexcheck-out ${arrayStores(array).sexcheckData.sexcheck.google.get}
            --sexcheck-problems-out ${arrayStores(array).sexcheckData.problems.google.get}
            --sites-vcf-out ${arrayStores(array).refData.sitesVcf.google.get}
            --variant-list-out ${arrayStores(array).refData.varList.google.get}
            --sample-list-out ${arrayStores(array).refData.sampleList.google.get}
            --mt-out ${arrayStores(array).refData.mt.google.get}"""
            .in(dataInString)
            .out(arrayStores(array).refData.mt.google.get, arrayStores(array).refData.hailLog.google.get, arrayStores(array).refData.variantMetrics.google.get, arrayStores(array).sexcheckData.sexcheck.google.get, arrayStores(array).sexcheckData.problems.google.get, arrayStores(array).refData.sitesVcf.google.get, arrayStores(array).refData.varList.google.get, arrayStores(array).refData.sampleList.google.get)
            .tag(s"${arrayStores(array).refData.mt.google.get}.pyHailLoad".split("/").last)
        
        }
		
        local {
		
          googleCopy(arrayStores(array).refData.hailLog.google.get, arrayStores(array).refData.hailLog.local.get)
          googleCopy(arrayStores(array).refData.varList.google.get, arrayStores(array).refData.varList.local.get)
          googleCopy(arrayStores(array).refData.sampleList.google.get, arrayStores(array).refData.sampleList.local.get)
          googleCopy(arrayStores(array).refData.variantMetrics.google.get, arrayStores(array).refData.variantMetrics.local.get)
          googleCopy(arrayStores(array).refData.sitesVcf.google.get, arrayStores(array).refData.sitesVcf.local.get)
          googleCopy(arrayStores(array).sexcheckData.sexcheck.google.get, arrayStores(array).sexcheckData.sexcheck.local.get)
          googleCopy(arrayStores(array).sexcheckData.problems.google.get, arrayStores(array).sexcheckData.problems.local.get)
		
        }

      case false =>

        val dataInString = (arrayStores(array).refData.vcf, arrayStores(array).refData.rawMt) match {
          case (Some(s), None) =>
            (arrayStores(array).refData.vcf.get.map(e => e.data.local.get).toSeq ++ arrayStores(array).refData.vcf.get.map(e => e.tbi.local.get).toSeq) :+ projectStores.dbSNPht.local.get :+ projectStores.sampleFile.local.get :+ projectStores.tmpDir
          case (None, Some(t)) =>
            Seq(arrayStores(array).refData.rawMt.get.data.local.get, projectStores.dbSNPht.local.get, projectStores.sampleFile.local.get, projectStores.tmpDir)
          case _ => throw new CfgException("invalid input for pyHailLoad: either vcf or rawMt must be defined")
        }

        val hailLoadIn = (arrayStores(array).refData.vcf, arrayStores(array).refData.rawMt) match {
          case (Some(s), None) =>
            s"""--vcf-in "${arrayStores(array).refData.vcfGlob.get.local.get}""""
          case (None, Some(t)) =>
            s"""--mt-in ${arrayStores(array).refData.rawMt.get.data.local.get.toString.split("@")(1)}"""
          case _ => throw new CfgException("invalid input for pyHailLoad: either vcf or rawMt must be defined")
        }
  
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {

          cmd"""${utils.binary.binPython} ${utils.python.pyHailLoad}
            --driver-memory ${projectConfig.resources.matrixTableHail.mem}
            --executor-memory ${projectConfig.resources.matrixTableHail.mem}
            --tmp-dir ${projectStores.tmpDir}
            --reference-genome ${projectConfig.referenceGenome}
            ${minPartitions}
            ${gqThreshold}
            --log ${arrayStores(array).refData.hailLog.local.get}
            ${hailLoadIn}
            --dbsnp-ht ${projectStores.dbSNPht.local.get}
            --sample-in ${projectStores.sampleFile.local.get}
            --id-col ${projectConfig.sampleFileId}
            --variant-metrics-out ${arrayStores(array).refData.variantMetrics.local.get}
            ${srSexString}
            ${maleCodeString}
            ${femaleCodeString}
            --sexcheck-out ${arrayStores(array).sexcheckData.sexcheck.local.get}
            --sexcheck-problems-out ${arrayStores(array).sexcheckData.problems.local.get}
            --sites-vcf-out ${arrayStores(array).refData.sitesVcf.local.get}
            --variant-list-out ${arrayStores(array).refData.varList.local.get}
            --sample-list-out ${arrayStores(array).refData.sampleList.local.get}
            --mt-out ${arrayStores(array).refData.mt.local.get}"""
            .in(dataInString)
            .out(arrayStores(array).refData.mt.local.get, arrayStores(array).refData.hailLog.local.get, arrayStores(array).refData.variantMetrics.local.get, arrayStores(array).sexcheckData.sexcheck.local.get, arrayStores(array).sexcheckData.problems.local.get, arrayStores(array).refData.sitesVcf.local.get, arrayStores(array).refData.varList.local.get)
            .tag(s"${arrayStores(array).refData.mt.local.get}.pyHailLoad".split("/").last)
  
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
