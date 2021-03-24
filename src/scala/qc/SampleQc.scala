object SampleQc extends loamstream.LoamFile {

  /**
   * Sample QC Stats Calculation Step
   *  Description:
   *    Calculate sample by variant statistics
   *    Calculate PC-adjusted residuals of sample by variant statistics (PCARMs)
   *    Calculate PCs of PCARMs
   *    Individual PCARM clustering
   *    Combined PCARM clustering
   *    Flag samples for removal according to set config filters
   *  Requires: Hail, Klustakwik, R, Python
   */

  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  
  def SampleQc(array: ConfigArray): Unit = {
  
    projectConfig.hailCloud match {
  
      case true =>
  
        local {
        
          googleCopy(projectStores.ancestryInferred.local.get, projectStores.ancestryInferred.google.get)
        
        }
        
        google {
        
          hail"""${utils.python.pyHailSampleqc} --
            --hail-utils ${projectStores.hailUtils.google.get}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.google.get}
            --clusters-in ${projectStores.ancestryInferred.google.get}
            --qc-out ${arrayStores(array).sampleQcData.stats.google.get}
            --cloud
            --log ${arrayStores(array).sampleQcData.hailLog.google.get}"""
            .in(projectStores.hailUtils.google.get, arrayStores(array).refData.mt.google.get, projectStores.ancestryInferred.google.get)
            .out(arrayStores(array).sampleQcData.stats.google.get, arrayStores(array).sampleQcData.hailLog.google.get)
            .tag(s"${arrayStores(array).sampleQcData.stats.local.get}.google".split("/").last)
        
        }
        
        local {
  
          googleCopy(arrayStores(array).sampleQcData.stats.google.get, arrayStores(array).sampleQcData.stats.local.get)
          googleCopy(arrayStores(array).sampleQcData.hailLog.google.get, arrayStores(array).sampleQcData.hailLog.local.get)
        
        }
  
      case false =>
  
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
  
          cmd"""${utils.binary.binPython} ${utils.python.pyHailSampleqc}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refData.mt.local.get}
            --clusters-in ${projectStores.ancestryInferred.local.get}
            --qc-out ${arrayStores(array).sampleQcData.stats.local.get}
            --log ${arrayStores(array).sampleQcData.hailLog.local.get}"""
            .in(arrayStores(array).refData.mt.local.get, projectStores.ancestryInferred.local.get)
            .out(arrayStores(array).sampleQcData.stats.local.get, arrayStores(array).sampleQcData.hailLog.local.get)
            .tag(s"${arrayStores(array).sampleQcData.stats.local.get}".split("/").last)
  
        }
  
    }
  
    val nSampleMetricPcs = array.nSampleMetricPcs.getOrElse("") match { case "" => ""; case _ => s"--n-pcs ${array.nSampleMetricPcs.get}" }
  
    val sampleMetricCovars = array.sampleMetricCovars.getOrElse("") match { case "" => ""; case _ => s"""--covars "${array.sampleMetricCovars.get}"""" }
    
    drmWith(imageName = s"${utils.image.imgR}") {
  
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rCalcIstatsAdj}
        --sampleqc-stats ${arrayStores(array).sampleQcData.stats.local.get}
        --sample-file ${projectStores.sampleFile.local.get}
        --iid-col ${projectConfig.sampleFileId}
        ${nSampleMetricPcs}
        ${sampleMetricCovars}
        --pca-scores ${arrayStores(array).pcaData.scores}
        --incomplete-obs ${arrayStores(array).sampleQcData.incompleteObs}
        --out ${arrayStores(array).sampleQcData.statsAdj}"""
        .in(arrayStores(array).sampleQcData.stats.local.get, projectStores.sampleFile.local.get, arrayStores(array).pcaData.scores)
        .out(arrayStores(array).sampleQcData.incompleteObs, arrayStores(array).sampleQcData.statsAdj)
        .tag(s"${arrayStores(array).sampleQcData.statsAdj}".split("/").last)
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rIstatsAdjPca}
        --sampleqc-stats-adj ${arrayStores(array).sampleQcData.statsAdj}
        --corr-plots ${arrayStores(array).sampleQcData.corrPlots}
        --pca-loadings ${arrayStores(array).sampleQcData.pcaLoadings}
        --pca-scores-plots ${arrayStores(array).sampleQcData.pcaPlots}
        --pca-scores ${arrayStores(array).sampleQcData.pcaScores}"""
        .in(arrayStores(array).sampleQcData.statsAdj)
        .out(arrayStores(array).sampleQcData.corrPlots, arrayStores(array).sampleQcData.pcaLoadings, arrayStores(array).sampleQcData.pcaPlots, arrayStores(array).sampleQcData.pcaScores)
        .tag(s"${arrayStores(array).sampleQcData.statsAdj}.rIstatsAdjPca".split("/").last)
  
    }
    
    /**
     * Sample QC PCA Clustering Step
     *  Description: Cluster PCs of adjusted sample QC metrics
     *  Requires: Klustakwik, R
     */
    
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.klustakwik.cpus, mem = projectConfig.resources.klustakwik.mem, maxRunTime = projectConfig.resources.klustakwik.maxRunTime) {
  
      cmd"""${utils.bash.shKlustakwikPca} ${utils.binary.binKlustakwik} ${arrayStores(array).sampleQcData.pcaScores} ${arrayStores(array).sampleQcPcaClusterData.fet} ${arrayStores(array).sampleQcPcaClusterData.base} ${arrayStores(array).sampleQcPcaClusterData.log}"""
        .in(arrayStores(array).sampleQcData.pcaScores)
        .out(arrayStores(array).sampleQcPcaClusterData.fet, arrayStores(array).sampleQcPcaClusterData.clu, arrayStores(array).sampleQcPcaClusterData.klg, arrayStores(array).sampleQcPcaClusterData.log)
        .tag(s"${arrayStores(array).sampleQcPcaClusterData.base}.shKlustakwikPca".split("/").last)
  
    }
  
    drmWith(imageName = s"${utils.image.imgR}") {
  
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rIstatsPcsGmmClusterPlot}
        --pca-scores ${arrayStores(array).sampleQcData.pcaScores}
        --cluster ${arrayStores(array).sampleQcPcaClusterData.clu}
        --outliers ${arrayStores(array).sampleQcPcaClusterData.outliers}
        --plots ${arrayStores(array).sampleQcPcaClusterData.plots}
        --xtabs ${arrayStores(array).sampleQcPcaClusterData.xtab}
        --id ${projectConfig.projectId}"""
        .in(arrayStores(array).sampleQcData.pcaScores, arrayStores(array).sampleQcPcaClusterData.clu)
        .out(arrayStores(array).sampleQcPcaClusterData.outliers, arrayStores(array).sampleQcPcaClusterData.plots, arrayStores(array).sampleQcPcaClusterData.xtab)
        .tag(s"${arrayStores(array).sampleQcData.pcaScores}.rIstatsPcsGmmClusterPlot".split("/").last)
    
    }
    
    /**
     * Sample QC Individual Stats Clustering Step
     *  Description: Cluster PCs of adjusted sample QC metrics
     *  Requires: Klustakwik, R
     */
    
    for {
    
      (metric, metricData) <- arrayStores(array).sampleQcMetricClusterData
    
    } yield { 
  
      drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.klustakwik.cpus, mem = projectConfig.resources.klustakwik.mem, maxRunTime = projectConfig.resources.klustakwik.maxRunTime) {
  
        cmd"""${utils.bash.shKlustakwikMetric} ${utils.binary.binKlustakwik} ${arrayStores(array).sampleQcData.statsAdj} ${metric} ${metricData.base} ${metricData.fet} ${metricData.log}"""
          .in(arrayStores(array).sampleQcData.statsAdj)
          .out(metricData.fet, metricData.clu, metricData.klg, metricData.log)
          .tag(s"${metricData.base}.shKlustakwikMetric".split("/").last)
    
      }
    
    }
  
    val metricCluFiles = {
  
      for {
        m <- arrayStores(array).sampleQcMetricClusterData.map(e => e._1).toSeq
      } yield {
    
        m + "___" + s"""${arrayStores(array).sampleQcMetricClusterData(m).clu.toString.split("@")(1)}"""
    
      }
    
    }
  
    drmWith(imageName = s"${utils.image.imgR}") {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rIstatsAdjGmmPlotMetrics}
        --ind-clu-files "${metricCluFiles.mkString(",")}"
        --stats-unadj ${arrayStores(array).sampleQcData.stats.local.get}
        --stats-adj ${arrayStores(array).sampleQcData.statsAdj}
        --metric-pca-outliers ${arrayStores(array).sampleQcPcaClusterData.outliers}
        --boxplots ${arrayStores(array).sampleQcData.boxPlots}
        --discreteness ${arrayStores(array).sampleQcData.discreteness}
        --outliers-table ${arrayStores(array).sampleQcData.outliers}
        --metric-plots ${arrayStores(array).sampleQcData.metricPlots}"""
        .in(arrayStores(array).sampleQcMetricClusterData.map(e => e._2).map(_.clu).toSeq :+ arrayStores(array).sampleQcData.stats.local.get :+ arrayStores(array).sampleQcData.statsAdj :+ arrayStores(array).sampleQcPcaClusterData.outliers)
        .out(arrayStores(array).sampleQcData.boxPlots, arrayStores(array).sampleQcData.discreteness, arrayStores(array).sampleQcData.outliers, arrayStores(array).sampleQcData.metricPlots)
        .tag(s"${arrayStores(array).sampleQcData.stats.local.get}.rIstatsAdjGmmPlotMetrics".split("/").last)
  
    }
  
    /**
     * Restore Samples Step
     * Requires: Python
     */
  
    val ancestryOutliersKeep = array.ancestryOutliersKeep match {
      case Some(s) => s.mkString(",")
      case None => None
    }
  
    val duplicatesKeep = array.duplicatesKeep match {
      case Some(s) => s.mkString(",")
      case None => None
    }
  
    val famsizeKeep = array.famsizeKeep match {
      case Some(s) => s.mkString(",")
      case None => None
    }
  
    val sampleqcKeep = array.sampleqcKeep match {
      case Some(s) => s.mkString(",")
      case None => None
    }
  
    val sexcheckKeep = array.sexcheckKeep match {
      case Some(s) => s.mkString(",")
      case None => None
    }
    
    drmWith(imageName = s"${utils.image.imgPython2}") {
    
      cmd"""${utils.binary.binPython} ${utils.python.pyMakeSamplesRestoreTable}
        --ancestry-outliers-keep "${array.ancestryOutliersKeep.mkString(",")}"
        --duplicates-keep "${array.duplicatesKeep.mkString(",")}"
        --famsize-keep "${array.famsizeKeep.mkString(",")}"
        --sampleqc-keep "${array.sampleqcKeep.mkString(",")}"
        --sexcheck-keep "${array.sexcheckKeep.mkString(",")}"
        --out ${arrayStores(array).filterQc.samplesRestore}"""
        .out(arrayStores(array).filterQc.samplesRestore)
        .tag(s"${arrayStores(array).filterQc.samplesRestore}".split("/").last)
  
    }
    
    /**
     * Compile Sample Exclusions Step
     * Requires: Python
     */
    
    drmWith(imageName = s"${utils.image.imgPython2}") {
    
      cmd"""${utils.binary.binPython} ${utils.python.pyCompileExclusions}
        --ancestry-inferred ${projectStores.ancestryInferred.local.get}
        --kinship-related ${arrayStores(array).kinshipData.kin0}
        --kinship-famsizes ${arrayStores(array).kinshipData.famSizes}
        --sampleqc-outliers ${arrayStores(array).sampleQcData.outliers}
        --sampleqc-incomplete-obs ${arrayStores(array).sampleQcData.incompleteObs}
        --sexcheck-problems ${arrayStores(array).sexcheckData.problems.local.get}
        --restore ${arrayStores(array).filterQc.samplesRestore}
        --out ${arrayStores(array).filterQc.samplesExclude.local.get}"""
        .in(projectStores.ancestryInferred.local.get, arrayStores(array).kinshipData.kin0, arrayStores(array).kinshipData.famSizes, arrayStores(array).sampleQcData.outliers, arrayStores(array).sampleQcData.incompleteObs, arrayStores(array).sexcheckData.problems.local.get, arrayStores(array).filterQc.samplesRestore)
        .out(arrayStores(array).filterQc.samplesExclude.local.get)
        .tag(s"${arrayStores(array).filterQc.samplesExclude.local.get}".split("/").last)
    
    }
  
  }

}
