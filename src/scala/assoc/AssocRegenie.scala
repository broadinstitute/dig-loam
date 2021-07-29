object AssocRegenie extends loamstream.LoamFile {

  /**
   * Run Masked Group Assoc Analysis via Regenie
   * 
   */
  import ProjectConfig._
  import ModelStores._
  import ArrayStores._
  import Fxns._
  import SchemaStores._
  import ProjectStores._
  import DirTree._
  
  def AssocRegenieStep1(configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {

    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head

    val btString = projectConfig.Phenos.filter(e => e.id == configModel.pheno).head.binary match {
      case true => "--bt"
      case false => ""
    }

    val lowmemString = projectConfig.regenieLowmem match {
      case true => "--lowmem"
      case false => ""
    }

    drmWith(imageName = s"${utils.image.imgRegenie}", cores = projectConfig.resources.regenieStep1.cpus, mem = projectConfig.resources.regenieStep1.mem, maxRunTime = projectConfig.resources.regenieStep1.maxRunTime) {

      cmd"""${utils.bash.shRegenieStep1}
        --regenie ${utils.binary.binRegenie}
        --bed ${arrayStores(array).prunedPlink.base}
        --covar-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}
        --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
        --block-size ${projectConfig.regenieBlockSize.get}
        ${btString}
        ${lowmemString}
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.base}
        --log ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.log}"""
        
        .in(arrayStores(array).prunedPlink.data :+ modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars :+ modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.log, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.loco, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.base}".split("/").last)

    }

  }

  def AssocRegenieStep2Single(test: String, configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {

    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head

    val btString = projectConfig.Phenos.filter(e => e.id == configModel.pheno).head.binary match {
      case true => "--bt"
      case false => ""
    }

    drmWith(imageName = s"${utils.image.imgRegenie}", cores = projectConfig.resources.regenieStep2Single.cpus, mem = projectConfig.resources.regenieStep2Single.mem, maxRunTime = projectConfig.resources.regenieStep2Single.maxRunTime) {

      for {

        chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).chrs.keys.toList

      } yield {

        cmd"""${utils.bash.shRegenieStep2Single}
          --regenie ${utils.binary.binRegenie}
          --bgzip ${utils.binary.binBgzip}
          --bgen ${arrayStores(array).cleanBgen.get.data.local.get}
          --sample ${arrayStores(array).cleanBgen.get.sample.local.get}
          --covar-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}
          --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
          --pheno-name ${configModel.pheno}
          --block-size ${projectConfig.regenieBlockSize.get}
          ${btString}
          --chr ${chr}
          --pred ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList}
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).chrs(chr).base}"""
          .in(arrayStores(array).cleanBgen.get.data.local.get, arrayStores(array).cleanBgen.get.sample.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).chrs(chr).log, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).chrs(chr).results)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).chrs(chr).results}".split("/").last)

      }

    }

    val maskResultsFile = s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).chrs(expandChrList(array.chrs).head).results.toString.split("@")(1).replace("chr" + expandChrList(array.chrs).head, "chr___CHR___")}"""
          
    val resultsFiles = for {
      chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).chrs.keys.toList
    } yield {
      modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).chrs(chr).results
    }
    
    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""${utils.bash.shMergeRegenieSingleResults}
         --results ${maskResultsFile}
         --chrs ${expandChrList(array.chrs).mkString(",")}
         --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results}"""
        .in(resultsFiles)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results}".split("/").last)
    
    }

    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""${utils.binary.binTabix} -f -b 2 -e 2 ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results}"""
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).resultsTbi)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).resultsTbi}".split("/").last)
    
    }
    
    //drmWith(imageName = s"${utils.image.imgPython2}") {
    //
    //  cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results}
    //    --p P
    //    --maf MAF
    //    --mac MAC
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.qqPlot}
    //    --out-low-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.qqPlotLowMaf}
    //    --out-mid-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.qqPlotMidMaf}
    //    --out-high-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.qqPlotHighMaf}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.qqPlot, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.qqPlotLowMaf, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.qqPlotMidMaf, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.qqPlotHighMaf)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.qqPlot}".split("/").last)
    //  
    //  cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results}
    //    --chr "#CHROM"
    //    --pos GENPOS
    //    --p P
    //    --mac MAC
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.mhtPlot}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.mhtPlot)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.mhtPlot}".split("/").last)
    //
    //}
    //
    //drmWith(imageName = s"${utils.image.imgPython2}") {
    //
    //  cmd"""${utils.binary.binPython} ${utils.python.pyTopResults}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results}
    //    --n 1000
    //    --p P
    //    --mac MAC
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top1000Results}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top1000Results)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top1000Results}".split("/").last)
    //
    //}
    //
    //drmWith(imageName = s"${utils.image.imgEnsemblVep}", cores = projectConfig.resources.vep.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.vep.maxRunTime) {
    //
    //  cmd"""${utils.bash.shAnnotateResults}
    //    ${arrayStores(array).refSitesVcf}
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top1000Results}
    //    ${projectConfig.resources.vep.cpus}
    //    ${projectStores.fasta}
    //    ${projectStores.vepCacheDir}
    //    ${projectStores.vepPluginsDir}
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top1000ResultsAnnot}"""
    //  .in(arrayStores(array).refSitesVcf, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top1000Results, projectStores.fasta, projectStores.vepCacheDir, projectStores.vepPluginsDir)
    //  .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top1000ResultsAnnot)
    //  .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top1000ResultsAnnot}".split("/").last)
    //
    //}
    //
    //var top20In = Seq(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top1000ResultsAnnot)
    //var hiLdStrings = Seq[String]()
    //
    //configModel.knowns match {
    //  case Some(_) =>
    //    for {
    //      k <- configModel.knowns.get
    //    } yield {
    //      top20In = top20In :+ projectStores.knownStores(projectConfig.Knowns.filter(e => e.id == k).head).hiLd.local.get
    //      hiLdStrings = hiLdStrings ++ Seq(s"${projectStores.knownStores(projectConfig.Knowns.filter(e => e.id == k).head).hiLd.toString.split("@")(1)}")
    //    }
    //  case None => ()
    //}
    //
    //drmWith(imageName = s"${utils.image.imgR}", cores = projectConfig.resources.standardR.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.standardR.maxRunTime) {
    //
    //  cmd"""${utils.binary.binRscript} --vanilla --verbose
    //    ${utils.r.rTop20}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top1000ResultsAnnot}
    //    --chr "#chr"
    //    --pos pos
    //    --known-loci "${hiLdStrings.mkString(",")}"
    //    --p pval
    //    --test ${test}
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top20AnnotAlignedRisk}"""
    //    .in(top20In)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top20AnnotAlignedRisk)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.top20AnnotAlignedRisk}".split("/").last)
    //
    //}
    //
    //projectConfig.maxSigRegions match {
    //
    //  case Some(s) =>
    //  
    //    drmWith(imageName = s"${utils.image.imgPython2}", cores = projectConfig.resources.standardPython.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.standardPython.maxRunTime) {
    //      
    //      cmd"""${utils.binary.binPython} ${utils.python.pyExtractTopRegions}
    //        --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results}
    //        --chr "#CHROM"
    //        --pos GENPOS
    //        --p P
    //        --max-regions ${s}
    //        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.sigRegions}"""
    //        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results)
    //        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.sigRegions)
    //        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.sigRegions}".split("/").last)
    //    
    //    }
    //  
    //  case None =>
    //  
    //    drmWith(imageName = s"${utils.image.imgPython2}", cores = projectConfig.resources.standardPython.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.standardPython.maxRunTime) {
    //      
    //      cmd"""${utils.binary.binPython} ${utils.python.pyExtractTopRegions}
    //        --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results}
    //        --chr "#CHROM"
    //        --pos GENPOS
    //        --p P
    //        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.sigRegions}"""
    //        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results)
    //        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.sigRegions)
    //        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.sigRegions}".split("/").last)
    //    
    //    }
    //
    //}
    //
    //drmWith(imageName = s"${utils.image.imgLocuszoom}", cores = projectConfig.resources.locuszoom.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.locuszoom.maxRunTime) {
    //
    //  cmd"""${utils.bash.shRegPlot} 
    //    ${utils.binary.binTabix}
    //    ${utils.binary.binLocuszoom}
    //    ${utils.binary.binGhostscript}
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.sigRegions}
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results}
    //    EUR
    //    hg19
    //    1000G_Nov2014
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.regPlotsBase}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).results, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.sigRegions)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.regPlotsPdf)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(test).summary.regPlotsPdf}".split("/").last)
    //
    //}

  }

  def AssocRegenieStep2Group(test: String, configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {

    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head

    val pheno = projectConfig.Phenos.filter(e => e.id == configModel.pheno).head

    val btString = projectConfig.Phenos.filter(e => e.id == configModel.pheno).head.binary match {
      case true => "--bt"
      case false => ""
    }

    val annoFile = schemaStores((configSchema, configCohorts)).regenie.get.annotations.phenos.keys.toList.contains(pheno) match {
      case true => schemaStores((configSchema, configCohorts)).regenie.get.annotations.phenos(pheno).local.get
      case false => schemaStores((configSchema, configCohorts)).regenie.get.annotations.base.local.get
    }

    val setList = schemaStores((configSchema, configCohorts)).regenie.get.setlist.phenos.keys.toList.contains(pheno) match {
      case true => schemaStores((configSchema, configCohorts)).regenie.get.setlist.phenos(pheno).local.get
      case false => schemaStores((configSchema, configCohorts)).regenie.get.setlist.base.local.get
    }

    val maskDef = schemaStores((configSchema, configCohorts)).regenie.get.masks.phenos.keys.toList.contains(pheno) match {
      case true => schemaStores((configSchema, configCohorts)).regenie.get.masks.phenos(pheno).local.get
      case false => schemaStores((configSchema, configCohorts)).regenie.get.masks.base.local.get
    }

    drmWith(imageName = s"${utils.image.imgRegenie}", cores = projectConfig.resources.regenieStep2Group.cpus, mem = projectConfig.resources.regenieStep2Group.mem, maxRunTime = projectConfig.resources.regenieStep2Group.maxRunTime) {

      for {

        chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).chrs.keys.toList

      } yield {

        cmd"""${utils.bash.shRegenieStep2Group}
          --regenie ${utils.binary.binRegenie}
          --bgzip ${utils.binary.binBgzip}
          --bgen ${arrayStores(array).cleanBgen.get.data.local.get}
          --sample ${arrayStores(array).cleanBgen.get.sample.local.get}
          --covar-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}
          --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
          --pheno-name ${configModel.pheno}
          --block-size ${projectConfig.regenieBlockSize.get}
          ${btString}
          --chr ${chr}
          --pred ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList}
          --anno-file ${annoFile}
		  --set-list ${setList}
		  --mask-def ${maskDef}
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).chrs(chr).base}"""
          .in(arrayStores(array).cleanBgen.get.data.local.get, arrayStores(array).cleanBgen.get.sample.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).chrs(chr).log, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).chrs(chr).results)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).chrs(chr).results}".split("/").last)

      }

    }

    val maskResultsFile = s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).chrs(expandChrList(array.chrs).head).results.toString.split("@")(1).replace("chr" + expandChrList(array.chrs).head, "chr___CHR___")}"""
          
    val resultsFiles = for {
      chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).chrs.keys.toList
    } yield {
      modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).chrs(chr).results
    }
    
    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""${utils.bash.shMergeRegenieGroupResults}
         --results ${maskResultsFile}
         --chrs ${expandChrList(array.chrs).mkString(",")}
         --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results}"""
        .in(resultsFiles)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results}".split("/").last)
    
    }

    //drmWith(imageName = s"${utils.image.imgPython2}") {
    //  
    //  cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results}
    //    --p P
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.qqPlot}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.qqPlot)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.qqPlot}".split("/").last)
    //  
    //  cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results}
    //    --chr "#CHROM"
    //    --pos "GENPOS"
    //    --p P
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.mhtPlot}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.mhtPlot)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.mhtPlot}".split("/").last)
    //  
    //}
    //
    //drmWith(imageName = s"${utils.image.imgPython2}") {
    //
    //  cmd"""${utils.binary.binPython} ${utils.python.pyTopRegenieGroupResults}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results}
    //    --group-id-map ${projectStores.geneIdMap.local.get}
    //    --n 20
    //    --p P 
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.top20Results}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results, projectStores.geneIdMap.local.get)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.top20Results)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.top20Results}".split("/").last)
    //
    //}

  }

}
