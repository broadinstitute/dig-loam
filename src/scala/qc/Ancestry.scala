object Ancestry extends loamstream.LoamFile {

  /**
    * Ancestry PCA
    *  Description:
    *    Merge with reference data on 5k Purcell AIMs
    *    Calculate PCs on merged file set
    *    Plot PCs and label by array data and reference ancestry groups
    *  Requires: Hail, FlashPCA2, R
    *  Notes:
    *     To perform ancestry inference and clustering with 1KG data, we must combine on common variants with reference data (clustering does not work when only using PCA loadings and projecting)
    */

  /**
    * Ancestry Clustering
    *  Description:
    *    Cluster with 1KG samples using Gaussian Mixture Modeling and infer ancestry
    *    Plot clusters and inferred ancestry
    *  Requires: Hail, Klustakwik, R
    *  Notes:
    *     *.ancestry.inferred.tsv contains the final inferred ancestry for each sample, including OUTLIERS
    *     This file is array specific
    */

  /**
    * Merge Inferred Ancestry
    *  Description:
    *    Merge inferred ancestry from all arrays
    *  Requires: R
    */

  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  
  def AncestryPca(array: ConfigArray): Unit = {
  
    projectConfig.hailCloud match {
  
      case true =>
  
        google {
        
          hail"""${utils.python.pyHailAncestryPcaMerge1kg} --
            --hail-utils ${projectStores.hailUtils.google.get}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.google.get}
            --kg-vcf-in ${projectStores.kgPurcellVcf.google.get}
            --kg-sample ${projectStores.kgSample.google.get}
            --plink-out ${arrayStores(array).ref1kgData.plink.base.google.get}
            --kg-samples-out ${arrayStores(array).ref1kgData.kgSamples.google.get}
            --cloud
            --log ${arrayStores(array).ref1kgData.hailLog.google.get}"""
            .in(projectStores.hailUtils.google.get, arrayStores(array).refData.mt.google.get, projectStores.kgPurcellVcf.google.get, projectStores.kgSample.google.get)
            .out(arrayStores(array).ref1kgData.plink.data.google.get :+ arrayStores(array).ref1kgData.kgSamples.google.get :+ arrayStores(array).ref1kgData.hailLog.google.get)
            .tag(s"${arrayStores(array).ref1kgData.plink.base.local.get}.pyHailAncestryPcaMerge1kg".split("/").last)
        
        }
        
        local {
        
          googleCopy(arrayStores(array).ref1kgData.plink.data.google.get, arrayStores(array).ref1kgData.plink.data.local.get)
          googleCopy(arrayStores(array).ref1kgData.kgSamples.google.get, arrayStores(array).ref1kgData.kgSamples.local.get)
          googleCopy(arrayStores(array).ref1kgData.hailLog.google.get, arrayStores(array).ref1kgData.hailLog.local.get)
        
        }
  
      case false =>
  
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
  
          cmd"""${utils.binary.binPython} ${utils.python.pyHailAncestryPcaMerge1kg}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.local.get}
            --kg-vcf-in ${projectStores.kgPurcellVcf.local.get}
            --kg-sample ${projectStores.kgSample.local.get}
            --plink-out ${arrayStores(array).ref1kgData.plink.base.local.get}
            --kg-samples-out ${arrayStores(array).ref1kgData.kgSamples.local.get}
            --log ${arrayStores(array).ref1kgData.hailLog.local.get}"""
            .in(arrayStores(array).refData.mt.local.get, projectStores.kgPurcellVcf.local.get, projectStores.kgSample.local.get)
            .out(arrayStores(array).ref1kgData.plink.data.local.get :+ arrayStores(array).ref1kgData.kgSamples.local.get :+ arrayStores(array).ref1kgData.hailLog.local.get)
            .tag(s"${arrayStores(array).ref1kgData.plink.base.local.get}".split("/").last)
  
        }
  
    }
  
    drmWith(imageName = s"${utils.image.imgFlashPca}", cores = projectConfig.resources.flashPca.cpus, mem = projectConfig.resources.flashPca.mem, maxRunTime = projectConfig.resources.flashPca.maxRunTime) {
    
      cmd"""${utils.binary.binFlashPca} --verbose
        --seed 1
        --numthreads ${projectConfig.resources.flashPca.cpus}
        --ndim 20
        --bfile ${arrayStores(array).ref1kgData.plink.base.local.get}
        --outpc ${arrayStores(array).ancestryPcaData.scores}
        --outvec ${arrayStores(array).ancestryPcaData.eigenVecs}
        --outload ${arrayStores(array).ancestryPcaData.loadings}
        --outval ${arrayStores(array).ancestryPcaData.eigenVals}
        --outpve ${arrayStores(array).ancestryPcaData.pve}
        --outmeansd ${arrayStores(array).ancestryPcaData.meansd}
        > ${arrayStores(array).ancestryPcaData.log}"""
        .in(arrayStores(array).ref1kgData.plink.data.local.get)
        .out(arrayStores(array).ancestryPcaData.log, arrayStores(array).ancestryPcaData.scores, arrayStores(array).ancestryPcaData.eigenVecs, arrayStores(array).ancestryPcaData.loadings, arrayStores(array).ancestryPcaData.eigenVals, arrayStores(array).ancestryPcaData.pve, arrayStores(array).ancestryPcaData.meansd)
        .tag(s"${arrayStores(array).ancestryPcaData.scores}".split("/").last)
    
    }
  
    drmWith(imageName = s"${utils.image.imgR}") {
  
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rPlotAncestryPca}
        --id ${projectConfig.projectId}
        --update-pop ID POP ${arrayStores(array).ref1kgData.kgSamples.local.get}
        --update-group ID GROUP ${arrayStores(array).ref1kgData.kgSamples.local.get}
        --pca-scores ${arrayStores(array).ancestryPcaData.scores}
        --out ${arrayStores(array).ancestryPcaData.plots}"""
        .in(arrayStores(array).ref1kgData.kgSamples.local.get, arrayStores(array).ancestryPcaData.scores)
        .out(arrayStores(array).ancestryPcaData.plots)
        .tag(s"${arrayStores(array).ancestryPcaData.plots}".split("/").last)
  
    }
  
  }
  
  def AncestryCluster(array: ConfigArray): Unit = {
  
    drmWith(imageName = s"${utils.image.imgTools}") {
  
      cmd"""bash -c "(echo 20; sed '1d' ${arrayStores(array).ancestryPcaData.scores} | cut -f3- | sed 's/\t/ /g') > ${arrayStores(array).ancestryClusterData.fet}""""
        .in(arrayStores(array).ancestryPcaData.scores)
        .out(arrayStores(array).ancestryClusterData.fet)
        .tag(s"${arrayStores(array).ancestryClusterData.fet}".split("/").last)
  
    }
  
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.klustakwik.cpus, mem = projectConfig.resources.klustakwik.mem, maxRunTime = projectConfig.resources.klustakwik.maxRunTime) {
    
      cmd"""${utils.binary.binKlustakwik} ${arrayStores(array).ancestryClusterData.base} 1 -UseFeatures ${projectConfig.ancestryInferenceFeatures} -UseDistributional 0 > ${arrayStores(array).ancestryClusterData.log}"""
        .in(arrayStores(array).ancestryClusterData.fet)
        .out(arrayStores(array).ancestryClusterData.clu, arrayStores(array).ancestryClusterData.klg, arrayStores(array).ancestryClusterData.log)
        .tag(s"${arrayStores(array).ancestryClusterData.base}.binKlustakwik".split("/").last)
  
    }
  
    val afrCodes = projectConfig.sampleFileAFRCodes match {
  
      case Some(s) => s"--afr-codes ${s.mkString(",")}"
      case None => ""
  
    }
  
    val amrCodes = projectConfig.sampleFileAMRCodes match {
  
      case Some(s) => s"--amr-codes ${s.mkString(",")}"
      case None => ""
  
    }
  
    val eurCodes = projectConfig.sampleFileEURCodes match {
  
      case Some(s) => s"--eur-codes ${s.mkString(",")}"
      case None => ""
  
    }
  
    val easCodes = projectConfig.sampleFileEASCodes match {
  
      case Some(s) => s"--eas-codes ${s.mkString(",")}"
      case None => ""
  
    }
  
    val sasCodes = projectConfig.sampleFileSASCodes match {
  
      case Some(s) => s"--sas-codes ${s.mkString(",")}"
      case None => ""
  
    }
  
    drmWith(imageName = s"${utils.image.imgR}") {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rPlotAncestryCluster}
        --pca-scores ${arrayStores(array).ancestryPcaData.scores}
        --update-pop ID POP ${arrayStores(array).ref1kgData.kgSamples.local.get}
        --update-group ID GROUP ${arrayStores(array).ref1kgData.kgSamples.local.get}
        --cluster ${arrayStores(array).ancestryClusterData.clu}
        --sample-file ${projectStores.sampleFile.local.get}
        --project-id ${projectConfig.projectId}
        --sample-id ${projectConfig.sampleFileId}
        --sr-race ${projectConfig.sampleFileSrRace}
        ${afrCodes}
        ${amrCodes}
        ${eurCodes}
        ${easCodes}
        ${sasCodes}
        --cluster-plots ${arrayStores(array).ancestryClusterData.plots}
        --xtabs ${arrayStores(array).ancestryClusterData.xtab}
        --plots-centers ${arrayStores(array).ancestryClusterData.centerPlots}
        --cluster-groups ${arrayStores(array).ancestryClusterData.groups}
        --ancestry-inferred ${arrayStores(array).ancestryData.inferred}
        --cluster-plots-no1kg ${arrayStores(array).ancestryClusterData.no1kgPlots}"""
        .in(arrayStores(array).ancestryPcaData.scores, arrayStores(array).ref1kgData.kgSamples.local.get, arrayStores(array).ancestryClusterData.clu, projectStores.sampleFile.local.get)
        .out(arrayStores(array).ancestryClusterData.plots, arrayStores(array).ancestryClusterData.xtab, arrayStores(array).ancestryClusterData.centerPlots, arrayStores(array).ancestryClusterData.groups, arrayStores(array).ancestryData.inferred, arrayStores(array).ancestryClusterData.no1kgPlots)
        .tag(s"${arrayStores(array).ancestryPcaData.scores}.rPlotAncestryCluster".split("/").last)
  
    }
  
  }

  def MergeInferredAncestry(): Unit = {
  
    val inferredList = projectConfig.Arrays.map(e => arrayStores(e).ancestryData.inferred).map(_.path).mkString(",")
  
    drmWith(imageName = s"${utils.image.imgR}") {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rAncestryClusterMerge}
        --ancestry-in $inferredList
        --out-table ${projectStores.ancestryInferred.local.get}
        --out-outliers ${projectStores.ancestryOutliers}"""
        .in(projectConfig.Arrays.map(e => arrayStores(e).ancestryData.inferred))
        .out(projectStores.ancestryInferred.local.get, projectStores.ancestryOutliers)
        .tag(s"${projectStores.ancestryInferred.local.get}".split("/").last)
    
    }
  
  }

}
