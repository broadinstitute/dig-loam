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

    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {

      cmd"""${utils.bash.shRegenieStep0}
        --plink ${utils.binary.binPlink}
        --bfile ${arrayStores(array).prunedPlink.base}
        --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step0.base}
        """
        .in(arrayStores(array).prunedPlink.data :+ modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step0.exclude)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step0.exclude}".split("/").last)

    }

    drmWith(imageName = s"${utils.image.imgRegenie}", cores = projectConfig.resources.regenieStep1.cpus, mem = projectConfig.resources.regenieStep1.mem, maxRunTime = projectConfig.resources.regenieStep1.maxRunTime) {

      cmd"""${utils.bash.shRegenieStep1}
        --regenie ${utils.binary.binRegenie}
        --bed ${arrayStores(array).prunedPlink.base}
        --covar-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}
        --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
        --exclude ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step0.exclude}
        ${btString}
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.base}
        --log ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.log}"""
        .in(arrayStores(array).prunedPlink.data :+ modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars :+ modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno :+ modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step0.exclude)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.log, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.loco, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.base}".split("/").last)

    }

  }

  def AssocRegenieStep2Single(configTest: ConfigTest, configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {

    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head

    drmWith(imageName = s"${utils.image.imgRegenie}", cores = projectConfig.resources.regenieStep2Single.cpus, mem = projectConfig.resources.regenieStep2Single.mem, maxRunTime = projectConfig.resources.regenieStep2Single.maxRunTime) {

      for {

        chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs.keys.toList

      } yield {

        cmd"""${utils.bash.shRegenieStep2Single}
          --regenie ${utils.binary.binRegenie}
          --bgzip ${utils.binary.binBgzip}
          --bgen ${arrayStores(array).bgen.get.data.local.get}
          --sample ${arrayStores(array).bgen.get.sample.local.get}
          --covar-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}
          --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
          --pheno-name ${configModel.finalPheno}
          --cli-options "${configTest.cliOpts.get}"
          --chr ${chr}
          --pred ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList}
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(chr).base}"""
          .in(arrayStores(array).bgen.get.data.local.get, arrayStores(array).bgen.get.sample.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(chr).log, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(chr).results)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(chr).results}".split("/").last)

      }

    }

    val maskResultsFile = s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(expandChrList(array.chrs).head).results.toString.split("@")(1).replace("chr" + expandChrList(array.chrs).head, "chr___CHR___")}"""
          
    val resultsFiles = for {
      chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs.keys.toList
    } yield {
      modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(chr).results
    }
    
    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""${utils.bash.shMergeRegenieSingleResults}
         --results ${maskResultsFile}
         --chrs ${expandChrList(array.chrs).mkString(",")}
         --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results}"""
        .in(resultsFiles)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results}".split("/").last)
    
    }

    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""${utils.binary.binTabix} -f -b 2 -e 2 ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results}"""
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).resultsTbi)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).resultsTbi}".split("/").last)
    
    }
    
    //drmWith(imageName = s"${utils.image.imgPython2}") {
    //
    //  cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results}
    //    --p P
    //    --maf MAF
    //    --mac MAC
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.qqPlot}
    //    --out-low-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.qqPlotLowMaf}
    //    --out-mid-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.qqPlotMidMaf}
    //    --out-high-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.qqPlotHighMaf}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.qqPlot, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.qqPlotLowMaf, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.qqPlotMidMaf, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.qqPlotHighMaf)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.qqPlot}".split("/").last)
    //  
    //  cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results}
    //    --chr "#CHROM"
    //    --pos GENPOS
    //    --p P
    //    --mac MAC
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.mhtPlot}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.mhtPlot)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.mhtPlot}".split("/").last)
    //
    //}
    //
    //drmWith(imageName = s"${utils.image.imgPython2}") {
    //
    //  cmd"""${utils.binary.binPython} ${utils.python.pyTopResults}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results}
    //    --n 1000
    //    --p P
    //    --mac MAC
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top1000Results}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top1000Results)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top1000Results}".split("/").last)
    //
    //}
    //
    //drmWith(imageName = s"${utils.image.imgEnsemblVep}", cores = projectConfig.resources.vep.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.vep.maxRunTime) {
    //
    //  cmd"""${utils.bash.shAnnotateResults}
    //    ${arrayStores(array).refSitesVcf}
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top1000Results}
    //    ${projectConfig.resources.vep.cpus}
    //    ${projectStores.fasta}
    //    ${projectStores.vepCacheDir}
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top1000ResultsAnnot}
    //    ${projectStores.referenceGenome}"""
    //  .in(arrayStores(array).refSitesVcf, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top1000Results, projectStores.fasta, projectStores.vepCacheDir)
    //  .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top1000ResultsAnnot)
    //  .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top1000ResultsAnnot}".split("/").last)
    //
    //}
    //
    //var top20In = Seq(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top1000ResultsAnnot)
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
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top1000ResultsAnnot}
    //    --chr "#chr"
    //    --pos pos
    //    --known-loci "${hiLdStrings.mkString(",")}"
    //    --p pval
    //    --test ${test}
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top20AnnotAlignedRisk}"""
    //    .in(top20In)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top20AnnotAlignedRisk)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.top20AnnotAlignedRisk}".split("/").last)
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
    //        --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results}
    //        --chr "#CHROM"
    //        --pos GENPOS
    //        --p P
    //        --max-regions ${s}
    //        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.sigRegions}"""
    //        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results)
    //        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.sigRegions)
    //        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.sigRegions}".split("/").last)
    //    
    //    }
    //  
    //  case None =>
    //  
    //    drmWith(imageName = s"${utils.image.imgPython2}", cores = projectConfig.resources.standardPython.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.standardPython.maxRunTime) {
    //      
    //      cmd"""${utils.binary.binPython} ${utils.python.pyExtractTopRegions}
    //        --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results}
    //        --chr "#CHROM"
    //        --pos GENPOS
    //        --p P
    //        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.sigRegions}"""
    //        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results)
    //        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.sigRegions)
    //        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.sigRegions}".split("/").last)
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
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.sigRegions}
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results}
    //    EUR
    //    hg19
    //    1000G_Nov2014
    //    ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.regPlotsBase}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.sigRegions)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.regPlotsPdf)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary.regPlotsPdf}".split("/").last)
    //
    //}

  }

  def AssocRegenieStep2Group(configTest: ConfigTest, mask: MaskFilter, configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {

    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head

    val pheno = projectConfig.Phenos.filter(e => e.id == configModel.pheno).head

    val annoFile = schemaStores((configSchema, configCohorts)).regenie.get.annotations(mask).phenos.keys.toList.contains(pheno) match {
      case true => schemaStores((configSchema, configCohorts)).regenie.get.annotations(mask).phenos(pheno).local.get
      case false => schemaStores((configSchema, configCohorts)).regenie.get.annotations(mask).base.local.get
    }

    val setList = schemaStores((configSchema, configCohorts)).regenie.get.setlist.phenos.keys.toList.contains(pheno) match {
      case true => schemaStores((configSchema, configCohorts)).regenie.get.setlist.phenos(pheno).local.get
      case false => schemaStores((configSchema, configCohorts)).regenie.get.setlist.base.local.get
    }

    val maskDef = schemaStores((configSchema, configCohorts)).regenie.get.masks(mask).phenos.keys.toList.contains(pheno) match {
      case true => schemaStores((configSchema, configCohorts)).regenie.get.masks(mask).phenos(pheno).local.get
      case false => schemaStores((configSchema, configCohorts)).regenie.get.masks(mask).base.local.get
    }

    drmWith(imageName = s"${utils.image.imgRegenie}", cores = projectConfig.resources.regenieStep2Group.cpus, mem = projectConfig.resources.regenieStep2Group.mem, maxRunTime = projectConfig.resources.regenieStep2Group.maxRunTime) {

      //for {
	  //
      //  chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs.keys.toList
	  //
      //} yield {
	  //
      //  cmd"""${utils.bash.shRegenieStep2Group}
      //    --regenie ${utils.binary.binRegenie}
      //    --bgzip ${utils.binary.binBgzip}
      //    --bgen ${arrayStores(array).bgen.get.data.local.get}
      //    --sample ${arrayStores(array).bgen.get.sample.local.get}
      //    --covar-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}
      //    --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
      //    --pheno-name ${configModel.finalPheno}
      //    --cli-options "${configTest.cliOpts.get}"
      //    --group-stats
      //    --chr ${chr}
      //    --pred ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList}
      //    --anno-file ${annoFile}
      //    --set-list ${setList}
      //    --mask-def ${maskDef}
      //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(chr).base}"""
      //    .in(arrayStores(array).bgen.get.data.local.get, arrayStores(array).bgen.get.sample.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList)
      //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(chr).log, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(chr).results)
      //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(chr).results}".split("/").last)
	  //
      //}

      cmd"""${utils.bash.shRegenieStep2Group}
          --regenie ${utils.binary.binRegenie}
          --bgzip ${utils.binary.binBgzip}
          --bgen ${arrayStores(array).bgen.get.data.local.get}
          --sample ${arrayStores(array).bgen.get.sample.local.get}
          --covar-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}
          --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
          --pheno-name ${configModel.finalPheno}
          --cli-options "${configTest.cliOpts.get}"
          --group-stats
          --pred ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList}
          --anno-file ${annoFile}
          --set-list ${setList}
          --mask-def ${maskDef}
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).base}"""
          .in(arrayStores(array).bgen.get.data.local.get, arrayStores(array).bgen.get.sample.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).log, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results}".split("/").last)

    }

    //val maskResultsFile = s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(expandChrList(array.chrs).head).results.toString.split("@")(1).replace("chr" + expandChrList(array.chrs).head, "chr___CHR___")}"""
    //      
    //val resultsFiles = for {
    //  chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs.keys.toList
    //} yield {
    //  modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(chr).results
    //}
    //
    //drmWith(imageName = s"${utils.image.imgTools}") {
    //
    //  cmd"""${utils.bash.shMergeRegenieGroupResults}
    //     --results ${maskResultsFile}
    //     --chrs ${expandChrList(array.chrs).mkString(",")}
    //     --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results}"""
    //    .in(resultsFiles)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results}".split("/").last)
    //
    //}





    //drmWith(imageName = s"${utils.image.imgPython2}") {
    //  
    //  cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results}
    //    --p P
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.qqPlot}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.qqPlot)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.qqPlot}".split("/").last)
    //  
    //  cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results}
    //    --chr "#CHROM"
    //    --pos "GENPOS"
    //    --p P
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.mhtPlot}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.mhtPlot)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.mhtPlot}".split("/").last)
    //  
    //}
    //
    //drmWith(imageName = s"${utils.image.imgPython2}") {
    //
    //  cmd"""${utils.binary.binPython} ${utils.python.pyTopRegenieGroupResults}
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results}
    //    --group-id-map ${projectStores.geneIdMap.local.get}
    //    --n 20
    //    --p P 
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.top20Results}"""
    //    .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results, projectStores.geneIdMap.local.get)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.top20Results)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.top20Results}".split("/").last)
    //
    //}

  }

}
