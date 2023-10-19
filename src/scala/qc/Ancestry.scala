object Ancestry extends loamstream.LoamFile {

  /**
    * AncestryPca
    *  Description:
    *    Merge with reference data on 5k Purcell AIMs
    *    Calculate PCs on merged file set
    *    Plot PCs and label by array data and reference ancestry groups
    *  Requires: Hail, FlashPCA2, R
    *  Notes:
    *     To perform ancestry inference and clustering with 1KG data, we must combine on common variants with reference data (clustering does not work when only using PCA loadings and projecting)
    */

  /**
    * AncestryGmm
    *  Description:
    *    Cluster with 1KG samples using Gaussian Mixture Modeling method and infer ancestry
    *    Plot clusters and inferred ancestry
    *  Requires: Klustakwik, R
    *  Notes:
    *     *.ancestry.gmm.inferred.tsv contains the final inferred ancestry for each sample, including OUTLIERS
    *     This file is array specific
    */

    /**
    * AncestryKnn
    *  Description:
    *    Cluster with 1KG samples using K-nearest neighbor method and infer ancestry
    *    Plot clusters and inferred ancestry
    *  Requires: R
    *  Notes:
    *     *.ancestry.knn.inferred.tsv contains the final inferred ancestry for each sample, including OUTLIERS
    *     This file is array specific
    */

  /**
    * MergeInferredAncestryGmm
    *  Description:
    *    Merge inferred ancestry from all arrays for Gmm method
    *  Requires: R
    */

  /**
    * MergeInferredAncestryKnn
    *  Description:
    *    Merge inferred ancestry from all arrays for Knn method
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
            --driver-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
            --executor-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
            --tmp-dir ${projectStores.tmpDir}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.local.get}
            --kg-vcf-in ${projectStores.kgPurcellVcf.local.get}
            --kg-sample ${projectStores.kgSample.local.get}
            --plink-out ${arrayStores(array).ref1kgData.plink.base.local.get}
            --kg-samples-out ${arrayStores(array).ref1kgData.kgSamples.local.get}
            --log ${arrayStores(array).ref1kgData.hailLog.local.get}"""
            .in(arrayStores(array).refData.mt.local.get, projectStores.kgPurcellVcf.local.get, projectStores.kgSample.local.get, projectStores.tmpDir)
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
  
  def AncestryGmm(array: ConfigArray): Unit = {
  
    drmWith(imageName = s"${utils.image.imgTools}") {
  
      cmd"""bash -c "(echo 20; sed '1d' ${arrayStores(array).ancestryPcaData.scores} | cut -f3- | sed 's/\t/ /g') > ${arrayStores(array).ancestryGmmData.fet}""""
        .in(arrayStores(array).ancestryPcaData.scores)
        .out(arrayStores(array).ancestryGmmData.fet)
        .tag(s"${arrayStores(array).ancestryGmmData.fet}".split("/").last)
  
    }
  
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.klustakwik.cpus, mem = projectConfig.resources.klustakwik.mem, maxRunTime = projectConfig.resources.klustakwik.maxRunTime) {
    
      cmd"""${utils.binary.binKlustakwik} ${arrayStores(array).ancestryGmmData.base} 1 -UseFeatures ${projectConfig.ancestryInferenceFeatures} -UseDistributional 0 > ${arrayStores(array).ancestryGmmData.log}"""
        .in(arrayStores(array).ancestryGmmData.fet)
        .out(arrayStores(array).ancestryGmmData.clu, arrayStores(array).ancestryGmmData.klg, arrayStores(array).ancestryGmmData.log)
        .tag(s"${arrayStores(array).ancestryGmmData.base}.binKlustakwik".split("/").last)
  
    }

    val srRace = projectConfig.sampleFileSrRace match {
  
      case Some(s) => s"--sr-race ${s}"
      case None => ""
  
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
        ${utils.r.rPlotAncestryGmm}
        --pca-scores ${arrayStores(array).ancestryPcaData.scores}
        --update-pop ID POP ${arrayStores(array).ref1kgData.kgSamples.local.get}
        --update-group ID GROUP ${arrayStores(array).ref1kgData.kgSamples.local.get}
        --cluster ${arrayStores(array).ancestryGmmData.clu}
        --sample-file ${projectStores.sampleFile.local.get}
        --project-id ${projectConfig.projectId}
        --sample-id ${projectConfig.sampleFileId}
        ${srRace}
        ${afrCodes}
        ${amrCodes}
        ${eurCodes}
        ${easCodes}
        ${sasCodes}
        --cluster-plots ${arrayStores(array).ancestryGmmData.plots}
        --xtabs ${arrayStores(array).ancestryGmmData.xtab}
        --plots-centers ${arrayStores(array).ancestryGmmData.centerPlots}
        --cluster-groups ${arrayStores(array).ancestryGmmData.groups}
        --ancestry-inferred ${arrayStores(array).ancestryData.inferredGmm}
        --cluster-plots-no1kg ${arrayStores(array).ancestryGmmData.no1kgPlots}"""
        .in(arrayStores(array).ancestryPcaData.scores, arrayStores(array).ref1kgData.kgSamples.local.get, arrayStores(array).ancestryGmmData.clu, projectStores.sampleFile.local.get)
        .out(arrayStores(array).ancestryGmmData.plots, arrayStores(array).ancestryGmmData.xtab, arrayStores(array).ancestryGmmData.centerPlots, arrayStores(array).ancestryGmmData.groups, arrayStores(array).ancestryData.inferredGmm, arrayStores(array).ancestryGmmData.no1kgPlots)
        .tag(s"${arrayStores(array).ancestryPcaData.scores}.rPlotAncestryGmm".split("/").last)
  
    }
  
  }

  def AncestryKnn(array: ConfigArray): Unit = {

    drmWith(imageName = s"${utils.image.imgR}") {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rAncestryKnn}
        --pca-scores ${arrayStores(array).ancestryPcaData.scores}
        --update-pop ID POP ${arrayStores(array).ref1kgData.kgSamples.local.get}
        --update-group ID GROUP ${arrayStores(array).ref1kgData.kgSamples.local.get}
        --sample-file ${projectStores.sampleFile.local.get}
        --project-id ${projectConfig.projectId}
        --sample-id ${projectConfig.sampleFileId}
        --out-predictions ${arrayStores(array).ancestryKnnData.predictions}
        --out-inferred ${arrayStores(array).ancestryData.inferredKnn}
        --out-plots ${arrayStores(array).ancestryKnnData.plots}"""
        .in(arrayStores(array).ancestryPcaData.scores, arrayStores(array).ref1kgData.kgSamples.local.get, projectStores.sampleFile.local.get)
        .out(arrayStores(array).ancestryKnnData.predictions, arrayStores(array).ancestryData.inferredKnn, arrayStores(array).ancestryKnnData.plots)
        .tag(s"${arrayStores(array).ancestryPcaData.scores}.rPlotAncestryKnn".split("/").last)
  
    }

  }

  def MergeInferredAncestryGmm(): Unit = {
  
    val inferredList = projectConfig.Arrays.map(e => arrayStores(e).ancestryData.inferredGmm).map(_.path).mkString(",")
  
    drmWith(imageName = s"${utils.image.imgR}") {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rAncestryGmmMerge}
        --ancestry-in $inferredList
        --out-table ${projectStores.ancestryInferredGmm.local.get}
        --out-outliers ${projectStores.ancestryOutliersGmm}"""
        .in(projectConfig.Arrays.map(e => arrayStores(e).ancestryData.inferredGmm))
        .out(projectStores.ancestryInferredGmm.local.get, projectStores.ancestryOutliersGmm)
        .tag(s"${projectStores.ancestryInferredGmm.local.get}".split("/").last)
    
    }
  
  }

  def MergeInferredAncestryKnn(): Unit = {

    val inferredList = projectConfig.Arrays.map(e => arrayStores(e).ancestryData.inferredKnn).map(_.path).mkString(",")
  
    drmWith(imageName = s"${utils.image.imgR}") {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rAncestryKnnMerge}
        --ancestry-in $inferredList
        --out-table ${projectStores.ancestryInferredKnn.local.get}"""
        .in(projectConfig.Arrays.map(e => arrayStores(e).ancestryData.inferredKnn))
        .out(projectStores.ancestryInferredKnn.local.get)
        .tag(s"${projectStores.ancestryInferredKnn.local.get}".split("/").last)
    
    }

  }

}
