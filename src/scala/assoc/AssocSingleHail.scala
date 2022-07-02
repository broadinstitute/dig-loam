object AssocSingleHail extends loamstream.LoamFile {

  /**
   * Run Single Variant Assoc Analysis via Hail
   * 
   */
  import ProjectConfig._
  import ModelStores._
  import ArrayStores._
  import Fxns._
  import SchemaStores._
  import ProjectStores._
  import DirTree._
  
  def AssocSingleHail(configTest: ConfigTest, configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {
  
    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head
  
    val pheno = projectConfig.Phenos.filter(e => e.id == configModel.pheno).head

    projectConfig.hailCloud match {
    
      case true =>
        
        googleWith(projectConfig.cloudResources.mtCluster) {
        
          hail"""${utils.python.pyHailAssoc} --
            --hail-utils ${projectStores.hailUtils.google.get}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refMt.google.get}
            --pheno-in ${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.google.get}
            --iid-col ${array.phenoFileId}
            --pheno-analyzed ${configModel.finalPheno}
            --pcs-include ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.google.get}
            --test ${test.model.get}
            --covars-analyzed "${configModel.finalCovars}"
            --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.google.get}
            --cloud
            --log ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).hailLog.google.get}"""
              .in(projectStores.hailUtils.google.get, arrayStores(array).refMt.google.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.google.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.google.get)
              .out(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.google.get, modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).hailLog.google.get)
              .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get}.google".split("/").last)
        
        }
        
        local {
        
          googleCopy(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.google.get, modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get)
          googleCopy(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).hailLog.google.get, modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).hailLog.local.get)
        
        }
      
      case false =>
      
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyHailAssoc}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refMt.local.get}
            --pheno-in ${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get}
            --iid-col ${array.phenoFileId}
            --pheno-analyzed ${configModel.finalPheno}
            --pcs-include ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get}
            --test ${test.model.get}
            --covars-analyzed "${configModel.finalCovars}"
            --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get}
            --log ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).hailLog.local.get}"""
              .in(arrayStores(array).refMt.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get)
              .out(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).hailLog.local.get)
              .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get}".split("/").last)
        
        }
    
    }
  
    drmWith(imageName = s"${utils.image.imgTools}") {
  
      cmd"""${utils.binary.binTabix} -f -b 2 -e 2 ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get}"""
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).resultsTbi)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).resultsTbi}".split("/").last)
    
    }
  
    drmWith(imageName = s"${utils.image.imgPython2}") {
    
      cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
        --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get}
        --p pval
        --maf maf
        --mac mac
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.qqPlot}
        --out-low-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.qqPlotLowMaf}
        --out-mid-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.qqPlotMidMaf}
        --out-high-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.qqPlotHighMaf}"""
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.qqPlot, modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.qqPlotLowMaf, modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.qqPlotMidMaf, modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.qqPlotHighMaf)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.qqPlot}".split("/").last)
      
      cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
        --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get}
        --chr "#chr"
        --pos pos
        --p pval
        --mac mac
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.mhtPlot}"""
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.mhtPlot)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.mhtPlot}".split("/").last)
    
    }
  
    drmWith(imageName = s"${utils.image.imgPython2}") {
    
      cmd"""${utils.binary.binPython} ${utils.python.pyTopResults}
        --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get}
        --n 1000
        --p pval
        --mac mac 
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top1000Results}"""
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top1000Results)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top1000Results}".split("/").last)
    
    }
  
    drmWith(imageName = s"${utils.image.imgEnsemblVep}", cores = projectConfig.resources.vep.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.vep.maxRunTime) {
  
      cmd"""${utils.bash.shAnnotateResults}
        ${arrayStores(array).refSitesVcf}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top1000Results}
        ${projectConfig.resources.vep.cpus}
        ${projectStores.fasta}
        ${projectStores.vepCacheDir}
        ${projectStores.vepPluginsDir}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top1000ResultsAnnot}
        ${projectConfig.referenceGenome}"""
      .in(arrayStores(array).refSitesVcf, modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top1000Results, projectStores.fasta, projectStores.vepCacheDir, projectStores.vepPluginsDir)
      .out(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top1000ResultsAnnot)
      .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top1000ResultsAnnot}".split("/").last)
  
    }
  
    var top20In = Seq(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top1000ResultsAnnot)
    var hiLdStrings = Seq[String]()
  
    configModel.knowns match {
      case Some(_) =>
        for {
          k <- configModel.knowns.get
        } yield {
          top20In = top20In :+ projectStores.knownStores(projectConfig.Knowns.filter(e => e.id == k).head).hiLd.local.get
          hiLdStrings = hiLdStrings ++ Seq(s"${projectStores.knownStores(projectConfig.Knowns.filter(e => e.id == k).head).hiLd.toString.split("@")(1)}")
        }
      case None => ()
    }
  
    drmWith(imageName = s"${utils.image.imgR}", cores = projectConfig.resources.standardR.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.standardR.maxRunTime) {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rTop20}
        --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top1000ResultsAnnot}
        --chr "#chr"
        --pos pos
        --known-loci "${hiLdStrings.mkString(",")}"
        --p pval
        --test ${test}
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top20AnnotAlignedRisk}"""
        .in(top20In)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top20AnnotAlignedRisk)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.top20AnnotAlignedRisk}".split("/").last)
    
    }
  
    projectConfig.maxSigRegions match {
  
      case Some(s) =>
      
        drmWith(imageName = s"${utils.image.imgPython2}", cores = projectConfig.resources.standardPython.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.standardPython.maxRunTime) {
          
          cmd"""${utils.binary.binPython} ${utils.python.pyExtractTopRegions}
            --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get}
            --chr "#chr"
            --pos pos
            --p pval
            --max-regions ${s}
            --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.sigRegions}"""
            .in(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get)
            .out(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.sigRegions)
            .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.sigRegions}".split("/").last)
        
        }
      
      case None =>
      
        drmWith(imageName = s"${utils.image.imgPython2}", cores = projectConfig.resources.standardPython.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.standardPython.maxRunTime) {
          
          cmd"""${utils.binary.binPython} ${utils.python.pyExtractTopRegions}
            --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get}
            --chr "#chr"
            --pos pos
            --p pval
            --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.sigRegions}"""
            .in(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get)
            .out(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.sigRegions)
            .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.sigRegions}".split("/").last)
        
        }
  
    }
    
    //drmWith(imageName = s"${utils.image.imgLocuszoom}", cores = projectConfig.resources.locuszoom.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.locuszoom.maxRunTime) {
    //
    //  cmd"""${utils.bash.shRegPlot} 
    //    ${utils.binary.binTabix}
    //    ${utils.binary.binLocuszoom}
    //    ${utils.binary.binGhostscript}
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.sigRegions}
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get}
    //    EUR
    //    hg19
    //    1000G_Nov2014
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.regPlotsBase}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).results.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.sigRegions)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.regPlotsPdf)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle(configTest).summary.regPlotsPdf}".split("/").last)
    //
    //}
  
  }

}
