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
  import Fxns._
  
  def Annotate(array: ConfigArray): Unit = {

    val lofteeOptions = projectConfig.referenceGenome match {
      case "GRCh37" => s"--conservation ${projectStores.vepConservation.toString.split("@")(1)}"
      case "GRCh38" => s"--conservation ${projectStores.vepConservation.toString.split("@")(1)} --gerpbw ${projectStores.vepGerpBW.get.toString.split("@")(1)}"
      case s => throw new CfgException("Annotate.lofteeOptions: reference genome " + s + " not supported")
    }

    val lofteeOptionsIn = projectConfig.referenceGenome match {
      case "GRCh37" => Seq(projectStores.vepConservation)
      case "GRCh38" => Seq(projectStores.vepConservation, projectStores.vepGerpBW.get)
      case s => throw new CfgException("Annotate.lofteeOptions: reference genome " + s + " not supported")
    }

    for {
  
      (chr, chrData) <- arrayStores(array).refData.annotations
  
    } yield {

      drmWith(imageName = s"${utils.image.imgTools}") {
        
        cmd"""${utils.bash.shTabixExtract}
          --tabix ${utils.binary.binTabix}
          --bgzip ${utils.binary.binBgzip}
          --file ${arrayStores(array).refData.sitesVcf.local.get}
          --region ${chrNumberToCode(chr, projectConfig.referenceGenome)}
          --out ${arrayStores(array).refData.sitesVcfChr(chr)}
          --reference-genome ${projectConfig.referenceGenome}"""
        .in(arrayStores(array).refData.sitesVcf.local.get, arrayStores(array).refData.sitesVcfTbi)
        .out(arrayStores(array).refData.sitesVcfChr(chr), arrayStores(array).refData.sitesVcfTbiChr(chr))
        .tag(s"${arrayStores(array).refData.sitesVcfChr(chr)}".split("/").last)
      
      }

      var annotationsIn = Seq(arrayStores(array).refData.sitesVcfChr(chr), arrayStores(array).refData.sitesVcfTbiChr(chr), projectStores.fasta, projectStores.vepCacheDir, projectStores.vepPluginsDir, projectStores.dbNSFP, projectStores.gnomad) ++ lofteeOptionsIn

      drmWith(imageName = s"${utils.image.imgEnsemblVep}", cores = projectConfig.resources.vep.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.vep.maxRunTime) {
	  
        cmd"""${utils.bash.shAnnotate}
          --sites-vcf ${arrayStores(array).refData.sitesVcfChr(chr)}
          --cpus ${projectConfig.resources.vep.cpus}
          --fasta ${projectStores.fasta}
          --dir-cache ${projectStores.vepCacheDir}
          --dir-plugins ${projectStores.vepPluginsDir}
          --dbnsfp ${projectStores.dbNSFP}
          --results ${arrayStores(array).refData.annotations(chr).local.get}
          --warnings ${arrayStores(array).refData.annotationWarnings(chr)}
          --header ${arrayStores(array).refData.annotationHeader(chr)}
          --reference-genome ${projectConfig.referenceGenome}
          --gnomad ${projectStores.gnomad}
          ${lofteeOptions}"""
        .in(annotationsIn)
        .out(arrayStores(array).refData.annotations(chr).local.get, arrayStores(array).refData.annotationWarnings(chr), arrayStores(array).refData.annotationHeader(chr))
        .tag(s"${arrayStores(array).refData.annotations(chr).local.get}".split("/").last)
	  
      }

      projectConfig.hailCloud match {
  
        case true =>
	    
          local {
            googleCopy(arrayStores(array).refData.annotations(chr).local.get, arrayStores(array).refData.annotations(chr).google.get)
          }

        case false => ()

      }

    }
  
    val minPartitions =  array.minPartitions.getOrElse("") match { case "" => ""; case _ => s"--min-partitions ${array.minPartitions.get}" }
    val (k1, v1) = arrayStores(array).refData.annotations.head
  
    projectConfig.hailCloud match {
  
      case true =>

        val annotionsGlob = s"""${arrayStores(array).refData.annotations(k1).google.get.toString.split("@")(1).replace(k1,"*")}"""

        val annotationsIn = (for {
          (k, v) <- arrayStores(array).refData.annotations
        } yield {
          v.google.get
        }).toSeq
  
        googleWith(projectConfig.cloudResources.variantHtCluster) {
        
          hail"""${utils.python.pyHailLoadAnnotations} --
            --annotations "${annotionsGlob}"
            --out ${arrayStores(array).refData.annotationsHt.google.get}
            ${minPartitions}
            --reference-genome ${projectConfig.referenceGenome}
            --cloud
            --log ${arrayStores(array).refData.annotationsHailLog.google.get}"""
              .in(annotationsIn)
              .out(arrayStores(array).refData.annotationsHt.google.get, arrayStores(array).refData.annotationsHailLog.google.get)
              .tag(s"${arrayStores(array).refData.annotationsHt.local.get}.google".split("/").last)
        
        }
  
      case false =>

        val annotionsGlob = s"""${arrayStores(array).refData.annotations(k1).local.get.toString.split("@")(1).replace(k1,"*")}"""

        val annotationsIn = (for {
          (k, v) <- arrayStores(array).refData.annotations
        } yield {
          v.local.get
        }).toSeq
  
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.tableHail.cpus, mem = projectConfig.resources.tableHail.mem, maxRunTime = projectConfig.resources.tableHail.maxRunTime) {
  
          cmd"""${utils.binary.binPython} ${utils.python.pyHailLoadAnnotations}
            --driver-memory ${(projectConfig.resources.tableHail.mem*0.9*1000).toInt}m
            --executor-memory ${(projectConfig.resources.tableHail.mem*0.9*1000).toInt}m
            --tmp-dir ${projectStores.tmpDir}
            --annotations "${annotionsGlob}"
            --out ${arrayStores(array).refData.annotationsHt.local.get}
            ${minPartitions}
            --reference-genome ${projectConfig.referenceGenome}
            --log ${arrayStores(array).refData.annotationsHailLog.local.get}"""
              .in(annotationsIn :+ projectStores.tmpDir)
              .out(arrayStores(array).refData.annotationsHt.local.get, arrayStores(array).refData.annotationsHailLog.local.get)
              .tag(s"${arrayStores(array).refData.annotationsHt.local.get}".split("/").last)
  
        }
  
    }
  
  }

}
