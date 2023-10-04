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

    val cliString = configModel.regenieStep1CliOpts match {
      case "" => ""
      case _ => s"""--cli-options "${configModel.regenieStep1CliOpts}""""
    }

    drmWith(imageName = s"${utils.image.imgRegenie}", cores = projectConfig.resources.regenieStep1.cpus, mem = projectConfig.resources.regenieStep1.mem, maxRunTime = projectConfig.resources.regenieStep1.maxRunTime) {

      cmd"""${utils.bash.shRegenieStep1}
        --regenie ${utils.binary.binRegenie}
        --bed ${arrayStores(array).prunedPlink.base}
        --covar-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}
        --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
        --exclude ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step0.exclude}
        ${cliString}
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.base}
        --log ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.log}"""
        .in(arrayStores(array).prunedPlink.data :+ modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars :+ modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno :+ modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step0.exclude)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.log, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.loco, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.base}".split("/").last)

    }

  }

  def AssocRegenieStep2Single(configTest: ConfigTest, configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {

    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head
    val phenos = projectConfig.Phenos.filter(e => configModel.pheno.contains(e.id))
    val phenosAnalyzed = projectConfig.Phenos.filter(e => configModel.pheno.contains(e.id)).map(e => e.idAnalyzed)

    drmWith(imageName = s"${utils.image.imgRegenie}", cores = projectConfig.resources.regenieStep2Single.cpus, mem = projectConfig.resources.regenieStep2Single.mem, maxRunTime = projectConfig.resources.regenieStep2Single.maxRunTime) {

      for {

        chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs.keys.toList

      } yield {

        val phenoResultsOut = for {
          p <- phenos
        } yield {
          modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(chr).results(p)
        }

        cmd"""${utils.bash.shRegenieStep2Single}
          --regenie ${utils.binary.binRegenie}
          --bgzip ${utils.binary.binBgzip}
          --bgen ${arrayStores(array).bgen.get.data.local.get}
          --sample ${arrayStores(array).bgen.get.sample.local.get}
          --covar-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}
          --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
          --pheno-names "${phenosAnalyzed}"
          --cli-options "${configTest.cliOpts.get}"
          --chr ${chr}
          --pred ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList}
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(chr).base}"""
          .in(arrayStores(array).bgen.get.data.local.get, arrayStores(array).bgen.get.sample.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList)
          .out(phenoResultsOut :+ modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(chr).log)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(chr).base}".split("/").last)

      }

    }

    for {

      pheno <- phenos

    } yield {

      val maskResultsFile = s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(expandChrList(array.chrs).head).results(pheno).toString.split("@")(1).replace("chr" + expandChrList(array.chrs).head, "chr___CHR___")}"""
          
      val resultsFiles = for {
        chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs.keys.toList
      } yield {
        modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).chrs(chr).results(pheno)
      }
      
      drmWith(imageName = s"${utils.image.imgTools}") {
      
        cmd"""${utils.bash.shMergeRegenieSingleResults}
           --results ${maskResultsFile}
           --chrs ${expandChrList(array.chrs).mkString(",")}
           --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results(pheno)}"""
          .in(resultsFiles)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results(pheno))
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results(pheno)}".split("/").last)
      
      }
	  
      drmWith(imageName = s"${utils.image.imgTools}") {
      
        cmd"""${utils.binary.binTabix} -f -b 2 -e 2 ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results(pheno)}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results(pheno))
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).resultsTbi(pheno))
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).resultsTbi(pheno)}".split("/").last)
      
      }
    
      drmWith(imageName = s"${utils.image.imgPython2}") {
      
        cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results(pheno)}
          --p P
          --eaf A1FREQ
          --n N
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).qqPlot}
          --out-low-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).qqPlotLowMaf}
          --out-mid-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).qqPlotMidMaf}
          --out-high-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).qqPlotHighMaf}
          > ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).qqPlotLog}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results(pheno))
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).qqPlot, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).qqPlotLowMaf, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).qqPlotMidMaf, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).qqPlotHighMaf, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).qqPlotLog)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).qqPlot}".split("/").last)
        
        cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results(pheno)}
          --chr "#CHROM"
          --pos GENPOS
          --p P
          --eaf A1FREQ
          --n N
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).mhtPlot}
          > ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).mhtPlotLog}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results(pheno))
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).mhtPlot, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).mhtPlotLog)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).mhtPlot}".split("/").last)
      
      }
    
      drmWith(imageName = s"${utils.image.imgPython2}") {
      
        cmd"""${utils.binary.binPython} ${utils.python.pyTopResults}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results(pheno)}
          --show 1000
          --p P
          --eaf A1FREQ
          --n N
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top1000Results}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).results(pheno))
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top1000Results)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top1000Results}".split("/").last)
      
      }

      drmWith(imageName = s"${utils.image.imgEnsemblVep}", cores = projectConfig.resources.vep.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.vep.maxRunTime) {
      
        cmd"""${utils.bash.shAnnotateResults}
          ${arrayStores(array).refSitesVcf}
          ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top1000Results}
          ${projectConfig.resources.vep.cpus}
          ${projectStores.fasta}
          ${projectStores.vepCacheDir}
          ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top1000ResultsAnnot}
          ${projectConfig.referenceGenome}"""
        .in(arrayStores(array).refSitesVcf, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top1000Results, projectStores.fasta, projectStores.vepCacheDir)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top1000ResultsAnnot)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top1000ResultsAnnot}".split("/").last)
      
      }

    }
    
    //var top20In = Seq(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top1000ResultsAnnot)
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
    //    --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top1000ResultsAnnot}
    //    --chr "#CHROM"
    //    --pos GENPOS
    //    --known-loci "${hiLdStrings.mkString(",")}"
    //    --p P
    //    --model ${configTest.model}
    //    --platform ${configTest.platform}
    //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top20AnnotAlignedRisk}"""
    //    .in(top20In)
    //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top20AnnotAlignedRisk)
    //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle(configTest).summary(pheno).top20AnnotAlignedRisk}".split("/").last)
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
    val phenos = projectConfig.Phenos.filter(e => configModel.pheno.contains(e.id))
    val phenosAnalyzed = projectConfig.Phenos.filter(e => configModel.pheno.contains(e.id)).map(e => e.idAnalyzed)

    drmWith(imageName = s"${utils.image.imgRegenie}", cores = projectConfig.resources.regenieStep2Group.cpus, mem = projectConfig.resources.regenieStep2Group.mem, maxRunTime = projectConfig.resources.regenieStep2Group.maxRunTime) {

      for {
	  
        chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs.keys.toList
	  
      } yield {

        val phenoResultsOut = for {
          p <- phenos
        } yield {
          modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(chr).results(p)
        }
	  
        cmd"""${utils.bash.shRegenieStep2Group}
          --regenie ${utils.binary.binRegenie}
          --bgzip ${utils.binary.binBgzip}
          --bgen ${arrayStores(array).bgen.get.data.local.get}
          --sample ${arrayStores(array).bgen.get.sample.local.get}
          --covar-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}
          --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
          --pheno-names "${phenosAnalyzed}"
          --cli-options "${configTest.cliOpts.get}"
          --chr ${chr}
          --pred ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList}
          --anno-file ${schemaStores((configSchema, configCohorts)).regenie.get.annotations(mask).local.get}
          --set-list ${schemaStores((configSchema, configCohorts)).regenie.get.setlist.local.get}
          --mask-def ${schemaStores((configSchema, configCohorts)).regenie.get.masks(mask).local.get}
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(chr).base}"""
          .in(arrayStores(array).bgen.get.data.local.get, arrayStores(array).bgen.get.sample.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList, schemaStores((configSchema, configCohorts)).regenie.get.annotations(mask).local.get, schemaStores((configSchema, configCohorts)).regenie.get.masks(mask).local.get, schemaStores((configSchema, configCohorts)).regenie.get.setlist.local.get)
          .out(phenoResultsOut :+ modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(chr).log)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(chr).base}".split("/").last)
	  
      }

      //cmd"""${utils.bash.shRegenieStep2Group}
      //    --regenie ${utils.binary.binRegenie}
      //    --bgzip ${utils.binary.binBgzip}
      //    --bgen ${arrayStores(array).bgen.get.data.local.get}
      //    --sample ${arrayStores(array).bgen.get.sample.local.get}
      //    --covar-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}
      //    --pheno-file ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
      //    --pheno-name ${pheno.idAnalyzed}
      //    --cli-options "${configTest.cliOpts.get}"
      //    --group-stats
      //    --pred ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList}
      //    --anno-file ${annoFile}
      //    --set-list ${setList}
      //    --mask-def ${maskDef}
      //    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).base}"""
      //    .in(arrayStores(array).bgen.get.data.local.get, arrayStores(array).bgen.get.sample.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.step1.predList)
      //    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).log, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results)
      //    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results}".split("/").last)

    }

    for {

      pheno <- phenos

    } yield {

      val maskResultsFile = s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(expandChrList(array.chrs).head).results(pheno).toString.split("@")(1).replace("chr" + expandChrList(array.chrs).head, "chr___CHR___")}"""
            
      val resultsFiles = for {
        chr <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs.keys.toList
      } yield {
        modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).chrs(chr).results(pheno)
      }
      
      drmWith(imageName = s"${utils.image.imgTools}") {
      
        cmd"""${utils.bash.shMergeRegenieGroupResults}
           --results ${maskResultsFile}
           --chrs ${expandChrList(array.chrs).mkString(",")}
           --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results(pheno)}"""
          .in(resultsFiles)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results(pheno))
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results(pheno)}".split("/").last)
      
      }

      drmWith(imageName = s"${utils.image.imgPython2}") {
        
        cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results(pheno)}
          --p P
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).qqPlot}
          > ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).qqPlotLog}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results(pheno))
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).qqPlot, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).qqPlotLog)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).qqPlot}".split("/").last)
        
        cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results(pheno)}
          --chr "CHROM"
          --pos "GENPOS"
          --p P
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).mhtPlot}
          > ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).mhtPlotLog}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results(pheno))
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).mhtPlot, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).mhtPlotLog)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).mhtPlot}".split("/").last)
        
      }
      
      drmWith(imageName = s"${utils.image.imgPython2}") {
      
        cmd"""${utils.binary.binPython} ${utils.python.pyTopRegenieGroupResults}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results(pheno)}
          --group-id-map ${projectStores.geneIdMap.local.get}
          --n 20
          --p P 
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).top20Results}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results(pheno), projectStores.geneIdMap.local.get)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).top20Results)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary(pheno).top20Results}".split("/").last)
      
      }

    }

  }

}
