object PrepareModel extends loamstream.LoamFile {

  /**
   * Prepare Model Cohorts
   * 
   */
  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  import MetaStores._
  import SchemaStores._
  import ModelStores._
  import Fxns._
  
  final case class CfgException(s: String) extends Exception(s)
  
  def PrepareModel(configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {
  
    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head
    val pheno = projectConfig.Phenos.filter(e => e.id == configModel.pheno).head
  
    val metaPriorSamplesString = configMeta match {
      case Some(s) =>
        var mpss = s"--cohorts ${configCohorts.map(e => e.id).mkString(",")}"
        val x = for {
          c <- configMeta.get.cohorts.takeWhile(_ != configCohorts.head.id)
        } yield {
          s"${modelStores((configModel, configSchema, Seq(projectConfig.Cohorts.filter(e => e.id == c).head), configMeta)).samplesAvailable.toString.split("@")(1)}"
        }
        x.size match {
          case n if n > 0 => mpss = mpss + " --meta-prior-samples " + x.mkString(",") + " --meta-cohorts " + configMeta.get.cohorts.mkString(",") + " --cckinship " + s"${metaKinshipStores(configMeta.get).kin0.toString.split("@")(1)}"
          case _ => ""
        }
        mpss
      case None => ""
    }
    
    val modelCohortSamplesAvailableIn = configMeta match {
      case Some(s) => 
        val x = for {
          c <- configMeta.get.cohorts.takeWhile(_ != configCohorts.head.id)
        } yield {
          modelStores((configModel, configSchema, Seq(projectConfig.Cohorts.filter(e => e.id == c).head), configMeta)).samplesAvailable
        }
        (x.toSeq ++ arrayStores(array).filteredPlink.data.local.get) :+ arrayStores(array).phenoFile.local.get :+ metaKinshipStores(configMeta.get).kin0 :+ arrayStores(array).ancestryMap :+ arrayStores(array).sampleQcStats :+ arrayStores(array).kin0
      case None =>
        arrayStores(array).filteredPlink.data.local.get :+ arrayStores(array).phenoFile.local.get :+ arrayStores(array).ancestryMap :+ arrayStores(array).sampleQcStats :+ arrayStores(array).kin0
    }
    
    val keepRelated = famTests.intersect(configModel.tests).size match {
      case n if n > 0 => "--keep-related"
      case _ => ""
    }
  
    drmWith(imageName = s"${utils.image.imgR}") {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rModelCohortSamplesAvailable}
        --pheno-in ${arrayStores(array).phenoFile.local.get}
        --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
        --ancestry-in ${arrayStores(array).ancestryMap}
        --cohorts "${configModel.cohorts.mkString(",")}"
        ${metaPriorSamplesString}
        --pheno-col ${configModel.pheno}
        --sex-col ${array.qcSampleFileSrSex}
        --iid-col ${array.phenoFileId}
        --sampleqc-in ${arrayStores(array).sampleQcStats}
        --kinship-in ${arrayStores(array).kin0}
        ${keepRelated}
        --covars "${configModel.covars}"
        --out-id-map ${modelStores((configModel, configSchema, configCohorts, configMeta)).sampleMap}
        --out-cohorts-map ${modelStores((configModel, configSchema, configCohorts, configMeta)).cohortMap.local.get}
        --out-pheno-prelim ${modelStores((configModel, configSchema, configCohorts, configMeta)).phenoPrelim}
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).samplesAvailable}
        > ${modelStores((configModel, configSchema, configCohorts, configMeta)).samplesAvailableLog}"""
        .in(modelCohortSamplesAvailableIn)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).sampleMap, modelStores((configModel, configSchema, configCohorts, configMeta)).cohortMap.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).phenoPrelim, modelStores((configModel, configSchema, configCohorts, configMeta)).samplesAvailable, modelStores((configModel, configSchema, configCohorts, configMeta)).samplesAvailableLog)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).samplesAvailable}".split("/").last)
    
    }
    
    val trans = configModel.trans match {
      case Some(s) => configModel.trans.get
      case None => "N/A"
    }

    val modelType = pheno.binary match {
      case true => "binary"
      case false => "quantitative"
    }
    
    drmWith(imageName = s"${utils.image.imgFlashPca}", cores = projectConfig.resources.flashPca.cpus, mem = projectConfig.resources.flashPca.mem, maxRunTime = projectConfig.resources.flashPca.maxRunTime) {
    
      cmd"""${utils.bash.shFlashPca}
        ${utils.binary.binFlashPca}
        ${utils.binary.binRscript}
        ${utils.binary.binPlink}
        ${utils.r.rGeneratePheno}
        ${projectConfig.resources.flashPca.cpus}
        ${arrayStores(array).prunedPlink.base}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).samplesAvailable}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcaBase}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcaScores}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcaEigenVecs}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcaLoadings}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcaEigenVals}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcaPve}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcaMeansd}
        ${configModel.maxPcaOutlierIterations}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).phenoPrelim}
        ${configModel.pheno}
        ${array.phenoFileId}
        "${trans}"
        ${modelType}
        "${configModel.covars}"
        ${projectConfig.minPCs}
        ${projectConfig.maxPCs}
        ${projectConfig.nStddevs}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get}
        ${modelStores((configModel, configSchema, configCohorts, configMeta)).outliers}
        ${projectConfig.resources.flashPca.mem * 0.9 * 1000}
        > ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcaLog}"""
        .in(arrayStores(array).prunedPlink.data :+ modelStores((configModel, configSchema, configCohorts, configMeta)).samplesAvailable :+ modelStores((configModel, configSchema, configCohorts, configMeta)).phenoPrelim)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).pcaScores, modelStores((configModel, configSchema, configCohorts, configMeta)).pcaEigenVecs, modelStores((configModel, configSchema, configCohorts, configMeta)).pcaLoadings, modelStores((configModel, configSchema, configCohorts, configMeta)).pcaEigenVals, modelStores((configModel, configSchema, configCohorts, configMeta)).pcaPve, modelStores((configModel, configSchema, configCohorts, configMeta)).pcaMeansd, modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).outliers, modelStores((configModel, configSchema, configCohorts, configMeta)).pcaLog)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get}".split("/").last)
    
    }

    configCohorts.size match {
    
      case x if x > 1 =>
    
        drmWith(imageName = s"${utils.image.imgPython2}", cores = projectConfig.resources.standardPython.cpus, mem = projectConfig.resources.standardPython.mem, maxRunTime = projectConfig.resources.standardPython.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyPhenoDistPlot}
            --pheno ${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get}
            --pheno-name ${configModel.pheno}
            --iid-col ${array.phenoFileId}
            --cohorts-map ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
            --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).phenoDistPlot}"""
            .in(modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get, schemaStores((configSchema, configCohorts)).cohortMap.local.get)
            .out(modelStores((configModel, configSchema, configCohorts, configMeta)).phenoDistPlot)
            .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).phenoDistPlot}".split("/").last)
        
        }
    
      case y if y == 1 =>
    
        drmWith(imageName = s"${utils.image.imgPython2}", cores = projectConfig.resources.standardPython.cpus, mem = projectConfig.resources.standardPython.mem, maxRunTime = projectConfig.resources.standardPython.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyPhenoDistPlot}
            --pheno ${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get}
            --pheno-name ${configModel.pheno}
            --iid-col ${array.phenoFileId}
            --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).phenoDistPlot}"""
            .in(modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get, schemaStores((configSchema, configCohorts)).cohortMap.local.get)
            .out(modelStores((configModel, configSchema, configCohorts, configMeta)).phenoDistPlot)
            .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).phenoDistPlot}".split("/").last)
        
        }
    
      case _ => ()
    
    }

    val transString = configModel.trans match {
      case Some(_) => s"--trans ${configModel.trans.get}"
      case None => ""
    }

    configModel.assocPlatforms.contains("epacts") match {
    
      case true =>
    
        drmWith(imageName = s"${utils.image.imgR}") {
        
          cmd"""${utils.binary.binRscript} --vanilla --verbose
            ${utils.r.rConvertPhenoToEpactsPed}
            --pheno ${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get}
            --pcs ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get}
            --pheno-col ${configModel.pheno}
            --iid-col ${array.phenoFileId}
            --sex-col ${array.qcSampleFileSrSex}
            --male-code ${array.qcSampleFileMaleCode}
            --female-code ${array.qcSampleFileFemaleCode}
            ${transString}
            --covars "${configModel.covars}"
            --model-vars ${modelStores((configModel, configSchema, configCohorts, configMeta)).epacts.get.modelVars}
            --ped ${modelStores((configModel, configSchema, configCohorts, configMeta)).epacts.get.ped}"""
            .in(modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get)
            .out(modelStores((configModel, configSchema, configCohorts, configMeta)).epacts.get.ped, modelStores((configModel, configSchema, configCohorts, configMeta)).epacts.get.modelVars)
            .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).epacts.get.ped}".split("/").last)
        
        }
    
      case false => ()
    
    }

    configModel.assocPlatforms.contains("regenie") match {
    
      case true =>
    
        drmWith(imageName = s"${utils.image.imgR}") {
        
          cmd"""${utils.binary.binRscript} --vanilla --verbose
            ${utils.r.rConvertPhenoToRegeniePhenoCovars}
            --pheno ${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get}
            --pcs ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get}
            --pheno-col ${configModel.pheno}
            --iid-col ${array.phenoFileId}
            --sex-col ${array.qcSampleFileSrSex}
            --male-code ${array.qcSampleFileMaleCode}
            --female-code ${array.qcSampleFileFemaleCode}
            ${transString}
            --covars "${configModel.covars}"
            --pheno-out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}
            --covars-out ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars}"""
            .in(modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get)
            .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.covars)
            .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.pheno}".split("/").last)
        
        }
    
      case false => ()
    
    }

    pheno.binary match {

      case true => ()

      case false =>
    
        drmWith(imageName = s"${utils.image.imgR}", cores = projectConfig.resources.standardR.cpus, mem = projectConfig.resources.standardR.mem, maxRunTime = projectConfig.resources.standardR.maxRunTime) {
        
          cmd"""${utils.binary.binRscript} --vanilla --verbose
            ${utils.r.rNullModelResidualPlot}
            --pheno-in ${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get}
            --pheno-col ${configModel.pheno}
            --covars "${configModel.covars}"
            --pcs-include ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get}
            --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).residualPlots.get.base}"""
            .in(modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get)
            .out(modelStores((configModel, configSchema, configCohorts, configMeta)).residualPlots.get.resVsFit, modelStores((configModel, configSchema, configCohorts, configMeta)).residualPlots.get.resVsLev, modelStores((configModel, configSchema, configCohorts, configMeta)).residualPlots.get.qq, modelStores((configModel, configSchema, configCohorts, configMeta)).residualPlots.get.sqrtresVsFit)
            .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).residualPlots.get.base}".split("/").last)
        
        }

    }

    
    //var filters = Seq[String]()
    //var cohortFilters = Seq[String]()
    //var knockoutFilters = Seq[String]()
    //var masks = Seq[String]()
    //var filterFields = Seq[String]()
    //configModel.filters match {
    //  case Some(l) =>
    //    filters = filters ++ variantFiltersToPrintableList(cfg = projectConfig, filters = l)
    //    filterFields = filterFields ++ getFilterFields(cfg = projectConfig, filters = l)
    //  case None => ()
    //}
    //(configModel.design, configModel.cohortFilters) match {
    //  case ("full", Some(l)) =>
    //    for {
    //      cf <- l if configCohorts.map(e => e.id).contains(cf.cohort)
    //    } yield {
    //      cohortFilters = cohortFilters ++ variantFiltersToPrintableList(cfg = projectConfig, filters = cf.filters, id = Some(cf.cohort))
    //      filterFields = filterFields ++ getFilterFields(cfg = projectConfig, filters = cf.filters)
    //    }
    //  case ("strat", Some(l)) =>
    //    for {
    //      cf <- l if configCohorts.head.id == cf.cohort
    //    } yield {
    //      filters = filters ++ variantFiltersToPrintableList(cfg = projectConfig, filters = cf.filters)
    //      filterFields = filterFields ++ getFilterFields(cfg = projectConfig, filters = cf.filters)
    //    }
    //  case _ => ()
    //}
    //(configModel.design, configModel.knockoutFilters) match {
    //  case ("full", Some(l)) =>
    //    for {
    //      cf <- l if configCohorts.map(e => e.id).contains(cf.cohort)
    //    } yield {
    //      knockoutFilters = knockoutFilters ++ variantFiltersToPrintableList(cfg = projectConfig, filters = cf.filters, id = Some(cf.cohort))
    //      filterFields = filterFields ++ getFilterFields(cfg = projectConfig, filters = cf.filters)
    //    }
    //  case _ => ()
    //}
    //configModel.masks match {
    //  case Some(l) =>
    //    for {
    //      mf <- l
    //    } yield {
    //      masks = masks ++ variantFiltersToPrintableList(cfg = projectConfig, filters = mf.filters, id = Some(mf.id))
    //      filterFields = filterFields ++ getFilterFields(cfg = projectConfig, filters = mf.filters)
    //    }
    //  case None => ()
    //}
    //
    //val fString = filters.size match {
    //
    //  case n if n > 0 => s"""echo "${filters.mkString("\n")}" > """
    //  case _ => "touch "
    //
    //}
    //
    //drmWith(imageName = s"${utils.image.imgTools}") {
    //
    //  cmd"""${fString} ${modelStores((configModel, configCohorts, configMeta)).filters.local.get}"""
    //    .out(modelStores((configModel, configCohorts, configMeta)).filters.local.get)
    //    .tag(s"${modelStores((configModel, configCohorts, configMeta)).filters.local.get}".split("/").last)
    //
    //}
    //
    //modelStores((configModel, configCohorts, configMeta)).cohortFilters.local match {
    //
    //  case Some(_) =>
    //
    //    val cfString = cohortFilters.size match {
    //    
    //      case n if n > 0 => s"""echo "${cohortFilters.mkString("\n")}" > """
    //      case _ => "touch "
    //    
    //    }
    //    
    //    drmWith(imageName = s"${utils.image.imgTools}") {
    //    
    //      cmd"""${cfString} ${modelStores((configModel, configCohorts, configMeta)).cohortFilters.local.get}"""
    //        .out(modelStores((configModel, configCohorts, configMeta)).cohortFilters.local.get)
    //        .tag(s"${modelStores((configModel, configCohorts, configMeta)).cohortFilters.local.get}".split("/").last)
    //    
    //    }
    //
    //  case None => ()
    //
    //}
    //
    //modelStores((configModel, configCohorts, configMeta)).knockoutFilters.local match {
    //
    //  case Some(_) =>
    //
    //    val kfString = knockoutFilters.size match {
    //    
    //      case n if n > 0 => s"""echo "${knockoutFilters.mkString("\n")}" > """
    //      case _ => "touch "
    //    
    //    }
    //    
    //    drmWith(imageName = s"${utils.image.imgTools}") {
    //    
    //      cmd"""${kfString} ${modelStores((configModel, configCohorts, configMeta)).knockoutFilters.local.get}"""
    //        .out(modelStores((configModel, configCohorts, configMeta)).knockoutFilters.local.get)
    //        .tag(s"${modelStores((configModel, configCohorts, configMeta)).knockoutFilters.local.get}".split("/").last)
    //    
    //    }
    //
    //  case None => ()
    //
    //}
    //
    //val mString = masks.size match {
    //
    //  case n if n > 0 => s"""echo "${masks.mkString("\n")}" > """
    //  case _ => "touch "
    //
    //}
    //
    //drmWith(imageName = s"${utils.image.imgTools}") {
    //
    //  cmd"""${mString} ${modelStores((configModel, configCohorts, configMeta)).masks.local.get}"""
    //    .out(modelStores((configModel, configCohorts, configMeta)).masks.local.get)
    //    .tag(s"${modelStores((configModel, configCohorts, configMeta)).masks.local.get}".split("/").last)
    //
    //}
    //
    //val binary = pheno.binary match {
    //  case true => "--binary"
    //  case false => ""
    //}
    //
    //projectConfig.hailCloud match {
    //
    //  case true =>
    //
    //    local {
    //    
    //      googleCopy(modelStores((configModel, configCohorts, configMeta)).pheno.local.get, modelStores((configModel, configCohorts, configMeta)).pheno.google.get)
    //    
    //    }
    //    
    //    googleWith(projectConfig.cloudResources.mtCluster) {
    //    
    //      hail"""${utils.python.pyHailModelVariantStats} --
    //        --hail-utils ${projectStores.hailUtils.google.get}
    //        --reference-genome ${projectConfig.referenceGenome}
    //        --mt-in ${arrayStores(array).refData.mt.google.get}
    //        --pheno-in ${modelStores((configModel, configCohorts, configMeta)).pheno.google.get}
    //        --iid-col ${projectConfig.phenoFileId}
    //        --pheno-col ${configModel.pheno}
    //        --variants-stats-out ${modelStores((configModel, configCohorts, configMeta)).variantsStats.base.google.get}
    //        --variants-stats-ht-out ${modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.base.google.get}
    //        ${binary}
    //        --cloud
    //        --log ${modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.base.google.get}"""
    //          .in(projectStores.hailUtils.google.get, arrayStores(array).refData.mt.google.get, modelStores((configModel, configCohorts, configMeta)).pheno.google.get)
    //          .out(modelStores((configModel, configCohorts, configMeta)).variantsStats.base.google.get, modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.base.google.get, modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.base.google.get)
    //          .tag(s"${modelStores((configModel, configCohorts, configMeta)).variantsStats.base.local.get}.google".split("/").last)
    //    
    //    }
    //    
    //    local {
    //    
    //      googleCopy(modelStores((configModel, configCohorts, configMeta)).variantsStats.base.google.get, modelStores((configModel, configCohorts, configMeta)).variantsStats.base.local.get)
    //      googleCopy(modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.base.google.get, modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.base.local.get)
    //    
    //    }
    //
    //  case false =>
    //
    //    drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.filterModelVariantsHail.cpus, mem = projectConfig.resources.filterModelVariantsHail.mem, maxRunTime = projectConfig.resources.filterModelVariantsHail.maxRunTime) {
    //    
    //      cmd"""${utils.binary.binPython} ${utils.python.pyHailModelVariantStats}
    //        --reference-genome ${projectConfig.referenceGenome}
    //        --mt-in ${arrayStores(array).refData.mt.local.get}
    //        --pheno-in ${modelStores((configModel, configCohorts, configMeta)).pheno.local.get}
    //        --iid-col ${projectConfig.phenoFileId}
    //        --pheno-col ${configModel.pheno}
    //        --variants-stats-out ${modelStores((configModel, configCohorts, configMeta)).variantsStats.base.local.get}
    //        --variants-stats-ht-out ${modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.base.local.get}
    //        ${binary}
    //        --log ${modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.base.local.get}"""
    //          .in(arrayStores(array).refData.mt.local.get, modelStores((configModel, configCohorts, configMeta)).pheno.local.get)
    //          .out(modelStores((configModel, configCohorts, configMeta)).variantsStats.base.local.get, modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.base.local.get, modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.base.local.get)
    //          .tag(s"${modelStores((configModel, configCohorts, configMeta)).variantsStats.base.local.get}".split("/").last)
    //    
    //    }
    //
    //}
    //
    //(configModel.design, configModel.filterCohorts.size) match {
    //
    //  case ("full", n) if n > 0 =>
    //
    //    projectConfig.hailCloud match {
    //      
    //      case true =>
    //    
    //        local {
    //    
    //          googleCopy(modelStores((configModel, configCohorts, configMeta)).cohortMap.local.get, modelStores((configModel, configCohorts, configMeta)).cohortMap.google.get)
    //    
    //        }
    //    
    //      case false => ()
    //    
    //    }
    //    
    //    for {
    //    
    //      cohort <- configCohorts if configModel.filterCohorts.contains(cohort.id)
    //    
    //    } yield {
    //    
    //      projectConfig.hailCloud match {
    //      
    //        case true =>
    //      
    //          googleWith(projectConfig.cloudResources.mtCluster) {
    //        
    //            hail"""${utils.python.pyHailModelVariantStats} --
    //              --hail-utils ${projectStores.hailUtils.google.get}
    //              --reference-genome ${projectConfig.referenceGenome}
    //              --mt-in ${arrayStores(array).refData.mt.google.get}
    //              --pheno-in ${modelStores((configModel, configCohorts, configMeta)).pheno.google.get}
    //              --iid-col ${projectConfig.phenoFileId}
    //              --pheno-col ${configModel.pheno}
    //              --cohorts-map-in ${modelStores((configModel, configCohorts, configMeta)).cohortMap.google.get}
    //              --cohort ${cohort.id}
    //              --variants-stats-out ${modelStores((configModel, configCohorts, configMeta)).variantsStats.cohorts(cohort).google.get}
    //              --variants-stats-ht-out ${modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts(cohort).google.get}
    //              ${binary}
    //              --cloud
    //              --log ${modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.cohorts(cohort).google.get}"""
    //                .in(projectStores.hailUtils.google.get, arrayStores(array).refData.mt.google.get, modelStores((configModel, configCohorts, configMeta)).pheno.google.get, modelStores((configModel, configCohorts, configMeta)).cohortMap.google.get)
    //                .out(modelStores((configModel, configCohorts, configMeta)).variantsStats.cohorts(cohort).google.get, modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts(cohort).google.get, modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.cohorts(cohort).google.get)
    //                .tag(s"${modelStores((configModel, configCohorts, configMeta)).variantsStats.cohorts(cohort).local.get}.google".split("/").last)
    //          
    //          }
    //          
    //          local {
    //          
    //            googleCopy(modelStores((configModel, configCohorts, configMeta)).variantsStats.cohorts(cohort).google.get, modelStores((configModel, configCohorts, configMeta)).variantsStats.cohorts(cohort).local.get)
    //            googleCopy(modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.cohorts(cohort).google.get, modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.cohorts(cohort).local.get)
    //          
    //          }
    //    
    //        case false =>
    //        
    //          drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.filterModelVariantsHail.cpus, mem = projectConfig.resources.filterModelVariantsHail.mem, maxRunTime = projectConfig.resources.filterModelVariantsHail.maxRunTime) {
    //          
    //              cmd"""${utils.binary.binPython} ${utils.python.pyHailModelVariantStats}
    //                --reference-genome ${projectConfig.referenceGenome}
    //                --mt-in ${arrayStores(array).refData.mt.local.get}
    //                --pheno-in ${modelStores((configModel, configCohorts, configMeta)).pheno.local.get}
    //                --iid-col ${projectConfig.phenoFileId}
    //                --pheno-col ${configModel.pheno}
    //                --cohorts-map-in ${modelStores((configModel, configCohorts, configMeta)).cohortMap.local.get}
    //                --cohort ${cohort.id}
    //                --variants-stats-out ${modelStores((configModel, configCohorts, configMeta)).variantsStats.cohorts(cohort).local.get}
    //                --variants-stats-ht-out ${modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts(cohort).local.get}
    //                ${binary}
    //                --log ${modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.cohorts(cohort).local.get}"""
    //                  .in(arrayStores(array).refData.mt.local.get, modelStores((configModel, configCohorts, configMeta)).pheno.local.get, modelStores((configModel, configCohorts, configMeta)).cohortMap.local.get)
    //                  .out(modelStores((configModel, configCohorts, configMeta)).variantsStats.cohorts(cohort).local.get, modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts(cohort).local.get, modelStores((configModel, configCohorts, configMeta)).variantsStatsHailLog.cohorts(cohort).local.get)
    //                  .tag(s"${modelStores((configModel, configCohorts, configMeta)).variantsStats.cohorts(cohort).local.get}".split("/").last)
    //            
    //          }
    //    
    //      }
    //    
    //    }
    //
    //  case _ => ()
    //
    //}
    //
    //projectConfig.hailCloud match {
    //
    //  case true =>
    //
    //    local {
    //
    //      googleCopy(modelStores((configModel, configCohorts, configMeta)).filters.local.get, modelStores((configModel, configCohorts, configMeta)).filters.google.get)
    //      googleCopy(modelStores((configModel, configCohorts, configMeta)).masks.local.get, modelStores((configModel, configCohorts, configMeta)).masks.google.get)
    //
    //    }
    //
    //    configModel.design match {
    //
    //      case "full" =>
    //
    //        local {
    //
    //          googleCopy(modelStores((configModel, configCohorts, configMeta)).cohortFilters.local.get, modelStores((configModel, configCohorts, configMeta)).cohortFilters.google.get)
    //          googleCopy(modelStores((configModel, configCohorts, configMeta)).knockoutFilters.local.get, modelStores((configModel, configCohorts, configMeta)).knockoutFilters.google.get)
    //
    //        }
    //
    //      case _ => ()
    //
    //    }
    //
    //    val cohortStatsInString = {
    //      modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts.size match {
    //        case n if n > 0 =>
    //          val x = "--cohort-stats-in"
    //          val y = for {
    //            (k, v) <- modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts
    //          } yield {
    //            s"""${k.id},${v.google.get.toString.split("@")(1)}"""
    //          }
    //          x + " " + y.mkString(" ")
    //        case _ => ""
    //      }
    //    }
    //
    //    val cohortFiltersInString = configModel.design match {
    //      case "full" => s"""--cohort-filters ${modelStores((configModel, configCohorts, configMeta)).cohortFilters.google.get.toString.split("@")(1)}"""
    //      case _ => ""
    //    }
    //
    //    val knockoutFiltersInString = configModel.design match {
    //      case "full" => s"""--knockout-filters ${modelStores((configModel, configCohorts, configMeta)).knockoutFilters.google.get.toString.split("@")(1)}"""
    //      case _ => ""
    //    }
    //
    //    var cohortStatsIn = Seq(projectStores.hailUtils.google.get, modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.base.google.get, arrayStores(array).refData.annotationsHt.google.get, modelStores((configModel, configCohorts, configMeta)).filters.google.get, modelStores((configModel, configCohorts, configMeta)).masks.google.get, arrayStores(array).filterPostQc.variantsExclude.google.get)
    //    
    //    modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts.size match {
    //      case n if n > 0 =>
    //        cohortStatsIn = cohortStatsIn ++ {
    //          for {
    //            (k, v) <- modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts
    //          } yield {
    //            v.google.get
    //          }
    //        }
    //      case _ => ()
    //    }
    //    
    //    configModel.design match {
    //        case "full" => cohortStatsIn = cohortStatsIn ++ Seq(modelStores((configModel, configCohorts, configMeta)).cohortFilters.google.get, modelStores((configModel, configCohorts, configMeta)).knockoutFilters.google.get)
    //        case _ => ()
    //    }
    //    
    //    googleWith(projectConfig.cloudResources.mtCluster) {
    //    
    //      hail"""${utils.python.pyHailFilterModelVariants} --
    //        --cloud
    //        --hail-utils ${projectStores.hailUtils.google.get}
    //        --reference-genome ${projectConfig.referenceGenome}
    //        --full-stats-in ${modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.base.google.get}
    //        ${cohortStatsInString}
    //        --annotation ${arrayStores(array).refData.annotationsHt.google.get}
    //        --filters ${modelStores((configModel, configCohorts, configMeta)).filters.google.get}
    //        ${cohortFiltersInString}
    //        ${knockoutFiltersInString}
    //        --masks ${modelStores((configModel, configCohorts, configMeta)).masks.google.get}
    //        --variants-remove ${arrayStores(array).filterPostQc.variantsExclude.google.get}
    //        --design ${configModel.design}
    //        --variant-filters-out ${modelStores((configModel, configCohorts, configMeta)).variantFilterTable.google.get}
    //        --ht-checkpoint ${modelStores((configModel, configCohorts, configMeta)).variantFilterHtCheckpoint.google.get}
    //        --variant-filters-ht-out ${modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.google.get}
    //        --log ${modelStores((configModel, configCohorts, configMeta)).variantFilterHailLog.google.get}"""
    //          .in(cohortStatsIn)
    //          .out(modelStores((configModel, configCohorts, configMeta)).variantFilterTable.google.get, modelStores((configModel, configCohorts, configMeta)).variantFilterHtCheckpoint.google.get, modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.google.get, modelStores((configModel, configCohorts, configMeta)).variantFilterHailLog.google.get)
    //          .tag(s"${modelStores((configModel, configCohorts, configMeta)).variantFilterTable.local.get}.google".split("/").last)
    //    
    //    }
    //    
    //    local {
    //    
    //      googleCopy(modelStores((configModel, configCohorts, configMeta)).variantFilterTable.google.get, modelStores((configModel, configCohorts, configMeta)).variantFilterTable.local.get)
    //      googleCopy(modelStores((configModel, configCohorts, configMeta)).variantFilterHailLog.google.get, modelStores((configModel, configCohorts, configMeta)).variantFilterHailLog.local.get)
    //    
    //    }
    //
    //  case false =>
    //
    //    val cohortStatsInString = {
    //      modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts.size match {
    //        case n if n > 0 =>
    //          val x = "--cohort-stats-in"
    //          val y = for {
    //            (k, v) <- modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts
    //          } yield {
    //            s"""${k.id},${v.local.get.toString.split("@")(1)}"""
    //          }
    //          x + " " + y.mkString(" ")
    //        case _ => ""
    //      }
    //    }
    //    
    //    val cohortFiltersInString = configModel.design match {
    //      case "full" => s"""--cohort-filters ${modelStores((configModel, configCohorts, configMeta)).cohortFilters.local.get.toString.split("@")(1)}"""
    //      case _ => ""
    //    }
    //    
    //    val knockoutFiltersInString = configModel.design match {
    //      case "full" => s"""--knockout-filters ${modelStores((configModel, configCohorts, configMeta)).knockoutFilters.local.get.toString.split("@")(1)}"""
    //      case _ => ""
    //    }
    //    
    //    var cohortStatsIn = Seq(modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.base.local.get, arrayStores(array).refData.annotationsHt.local.get, modelStores((configModel, configCohorts, configMeta)).filters.local.get, modelStores((configModel, configCohorts, configMeta)).masks.local.get, arrayStores(array).filterPostQc.variantsExclude.local.get)
    //    
    //    modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts.size match {
    //      case n if n > 0 =>
    //        cohortStatsIn = cohortStatsIn ++ {
    //          for {
    //            (k, v) <- modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.cohorts
    //          } yield {
    //            v.local.get
    //          }
    //        }
    //      case _ => ()
    //    }
    //    
    //    configModel.design match {
    //      case "full" => cohortStatsIn = cohortStatsIn ++ Seq(modelStores((configModel, configCohorts, configMeta)).cohortFilters.local.get, modelStores((configModel, configCohorts, configMeta)).knockoutFilters.local.get)
    //      case _ => ()
    //    }
    //    
    //    drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.filterModelVariantsHail.cpus, mem = projectConfig.resources.filterModelVariantsHail.mem, maxRunTime = projectConfig.resources.filterModelVariantsHail.maxRunTime) {
    //    
    //      cmd"""${utils.binary.binPython} ${utils.python.pyHailFilterModelVariants}
    //        --reference-genome ${projectConfig.referenceGenome}
    //        --full-stats-in ${modelStores((configModel, configCohorts, configMeta)).variantsStatsHt.base.local.get}
    //        ${cohortStatsInString}
    //        --annotation ${arrayStores(array).refData.annotationsHt.local.get}
    //        --filters ${modelStores((configModel, configCohorts, configMeta)).filters.local.get}
    //        ${cohortFiltersInString}
    //        ${knockoutFiltersInString}
    //        --masks ${modelStores((configModel, configCohorts, configMeta)).masks.local.get}
    //        --variants-remove ${arrayStores(array).filterPostQc.variantsExclude.local.get}
    //        --design ${configModel.design}
    //        --variant-filters-out ${modelStores((configModel, configCohorts, configMeta)).variantFilterTable.local.get}
    //        --ht-checkpoint ${modelStores((configModel, configCohorts, configMeta)).variantFilterHtCheckpoint.local.get}
    //        --variant-filters-ht-out ${modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.local.get}
    //        --log ${modelStores((configModel, configCohorts, configMeta)).variantFilterHailLog.local.get}"""
    //          .in(cohortStatsIn)
    //          .out(modelStores((configModel, configCohorts, configMeta)).variantFilterTable.local.get, modelStores((configModel, configCohorts, configMeta)).variantFilterHtCheckpoint.local.get, modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.local.get, modelStores((configModel, configCohorts, configMeta)).variantFilterHailLog.local.get)
    //          .tag(s"${modelStores((configModel, configCohorts, configMeta)).variantFilterTable.local.get}".split("/").last)
    //    
    //    }
    //
    //}
    //
    //groupTests.intersect(configModel.tests).size match {
    //
    //  case n if n > 0 =>
    //
    //    projectConfig.hailCloud match {
    //    
    //      case true =>
    //
    //        val maskedGroupFilesString = {
    //          modelStores((configModel, configCohorts, configMeta)).groupFile match {
    //            case Some(s) =>
    //              modelStores((configModel, configCohorts, configMeta)).groupFile.get.masks.size match {
    //                case n if n > 0 =>
    //                  val x = "--masked-groupfiles-out"
    //                  val y = for {
    //                    (k, v) <- modelStores((configModel, configCohorts, configMeta)).groupFile.get.masks
    //                  } yield {
    //                    s"""${k.id},${v.google.get.toString.split("@")(1)}"""
    //                  }
    //                  x + " " + y.mkString(" ")
    //                case _ => ""
    //              }
    //            case None => ""
    //          }
    //        }
    //
    //        var generateGroupfileOut = Seq(modelStores((configModel, configCohorts, configMeta)).groupFile.get.base.google.get, modelStores((configModel, configCohorts, configMeta)).groupFileHailLog.get.google.get)
    //
    //        modelStores((configModel, configCohorts, configMeta)).groupFile match {
    //          case Some(s) =>
    //            modelStores((configModel, configCohorts, configMeta)).groupFile.get.masks.size match {
    //                case n if n > 0 =>
    //                  generateGroupfileOut = generateGroupfileOut ++ {
    //                    for {
    //                      (k, v) <- modelStores((configModel, configCohorts, configMeta)).groupFile.get.masks
    //                    } yield {
    //                      v.google.get
    //                    }
    //                  }
    //                case _ => ()
    //            }
    //          case None => ()
    //        }
    //
    //        googleWith(projectConfig.cloudResources.mtCluster) {
    //        
    //          hail"""${utils.python.pyHailGenerateGroupfile} --
    //            --cloud
    //            --hail-utils ${projectStores.hailUtils.google.get}
    //            ${maskedGroupFilesString}
    //            --filter-table-in ${modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.google.get}
    //            --groupfile-out ${modelStores((configModel, configCohorts, configMeta)).groupFile.get.base.google.get}
    //            --log ${modelStores((configModel, configCohorts, configMeta)).groupFileHailLog.get.google.get}"""
    //            .in(projectStores.hailUtils.google.get, modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.google.get)
    //            .out(generateGroupfileOut)
    //            .tag(s"${modelStores((configModel, configCohorts, configMeta)).groupFile.get.base.local.get}.google".split("/").last)
    //  
    //        }
    //
    //        local {
    //        
    //          googleCopy(modelStores((configModel, configCohorts, configMeta)).groupFile.get.base.google.get, modelStores((configModel, configCohorts, configMeta)).groupFile.get.base.local.get)
    //          googleCopy(modelStores((configModel, configCohorts, configMeta)).groupFileHailLog.get.google.get, modelStores((configModel, configCohorts, configMeta)).groupFileHailLog.get.local.get)
    //        
    //        }
    //        
    //        modelStores((configModel, configCohorts, configMeta)).groupFile match {
    //          case Some(s) =>
    //            modelStores((configModel, configCohorts, configMeta)).groupFile.get.masks.size match {
    //              case n if n > 0 =>
    //                for {
    //                  (k, v) <- modelStores((configModel, configCohorts, configMeta)).groupFile.get.masks
    //                } yield {
    //                  local {
    //                    googleCopy(v.google.get, v.local.get)
    //                  }
    //                }
    //              case _ => ()
    //            }
    //          case None => ()
    //        }
    //
    //      case false =>
    //
    //        val maskedGroupFilesString = {
    //          modelStores((configModel, configCohorts, configMeta)).groupFile match {
    //            case Some(s) =>
    //              modelStores((configModel, configCohorts, configMeta)).groupFile.get.masks.size match {
    //                case n if n > 0 =>
    //                  val x = "--masked-groupfiles-out"
    //                  val y = for {
    //                    (k, v) <- modelStores((configModel, configCohorts, configMeta)).groupFile.get.masks
    //                  } yield {
    //                    s"""${k.id},${v.local.get.toString.split("@")(1)}"""
    //                  }
    //                  x + " " + y.mkString(" ")
    //                case _ => ""
    //              }
    //            case None => ""
    //          }
    //        }
    //        
    //        var generateGroupfileOut = Seq(modelStores((configModel, configCohorts, configMeta)).groupFile.get.base.local.get, modelStores((configModel, configCohorts, configMeta)).groupFileHailLog.get.local.get)
    //        
    //        modelStores((configModel, configCohorts, configMeta)).groupFile match {
    //          case Some(s) =>
    //            modelStores((configModel, configCohorts, configMeta)).groupFile.get.masks.size match {
    //              case n if n > 0 =>
    //                generateGroupfileOut = generateGroupfileOut ++ {
    //                  for {
    //                    (k, v) <- modelStores((configModel, configCohorts, configMeta)).groupFile.get.masks
    //                  } yield {
    //                    v.local.get
    //                  }
    //                }
    //              case _ => ()
    //            }
    //          case None => ()
    //        }
    //        
    //        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.filterModelVariantsHail.cpus, mem = projectConfig.resources.filterModelVariantsHail.mem, maxRunTime = projectConfig.resources.filterModelVariantsHail.maxRunTime) {
    //        
    //          cmd"""${utils.binary.binPython} ${utils.python.pyHailGenerateGroupfile}
    //            ${maskedGroupFilesString}
    //            --filter-table-in ${modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.local.get}
    //            --groupfile-out ${modelStores((configModel, configCohorts, configMeta)).groupFile.get.base.local.get}
    //            --log ${modelStores((configModel, configCohorts, configMeta)).groupFileHailLog.get.local.get}"""
    //            .in(modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.local.get)
    //            .out(generateGroupfileOut)
    //            .tag(s"${modelStores((configModel, configCohorts, configMeta)).groupFile.get.base.local.get}".split("/").last)
    //        
    //        }
    //  
    //    }
    //
    //  case _ => ()
    //
    //}
    //
    //modelStores((configModel, configCohorts, configMeta)).vcf match {
    //
    //  case Some(s) =>
    //
    //    projectConfig.hailCloud match {
    //    
    //      case true =>
    //
    //        googleWith(projectConfig.cloudResources.mtCluster) {
    //        
    //          hail"""${utils.python.pyHailGenerateModelVcf} --
    //            --cloud
    //            --hail-utils ${projectStores.hailUtils.google.get}
    //            --mt-in ${arrayStores(array).refData.mt.google.get}
    //            --cohorts-map-in ${modelStores((configModel, configCohorts, configMeta)).cohortMap.google.get}
    //            --filter-table-in ${modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.google.get}
    //            --vcf-out ${modelStores((configModel, configCohorts, configMeta)).vcf.get.data.google.get}
    //            --log ${modelStores((configModel, configCohorts, configMeta)).vcfHailLog.google.get}"""
    //            .in(projectStores.hailUtils.google.get, arrayStores(array).refData.mt.google.get, modelStores((configModel, configCohorts, configMeta)).cohortMap.google.get, modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.google.get)
    //            .out(modelStores((configModel, configCohorts, configMeta)).vcf.get.data.google.get, modelStores((configModel, configCohorts, configMeta)).vcfHailLog.google.get)
    //            .tag(s"${modelStores((configModel, configCohorts, configMeta)).vcf.get.data.local.get}.google".split("/").last)
    //  
    //        }
    //
    //        local {
    //
    //          googleCopy(modelStores((configModel, configCohorts, configMeta)).vcf.get.data.google.get, modelStores((configModel, configCohorts, configMeta)).vcf.get.data.local.get)
    //          googleCopy(modelStores((configModel, configCohorts, configMeta)).vcfHailLog.google.get, modelStores((configModel, configCohorts, configMeta)).vcfHailLog.local.get)
    //
    //        }
    //
    //      case false =>
    //
    //        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.filterModelVariantsHail.cpus, mem = projectConfig.resources.filterModelVariantsHail.mem, maxRunTime = projectConfig.resources.filterModelVariantsHail.maxRunTime) {
    //        
    //          cmd"""${utils.binary.binPython} ${utils.python.pyHailGenerateModelVcf}
    //            --mt-in ${arrayStores(array).refData.mt.local.get}
    //            --cohorts-map-in ${modelStores((configModel, configCohorts, configMeta)).cohortMap.local.get}
    //            --filter-table-in ${modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.local.get}
    //            --vcf-out ${modelStores((configModel, configCohorts, configMeta)).vcf.get.data.local.get}
    //            --log ${modelStores((configModel, configCohorts, configMeta)).vcfHailLog.local.get}"""
    //            .in(arrayStores(array).refData.mt.local.get, modelStores((configModel, configCohorts, configMeta)).cohortMap.local.get, modelStores((configModel, configCohorts, configMeta)).variantFilterHailTable.local.get)
    //            .out(modelStores((configModel, configCohorts, configMeta)).vcf.get.data.local.get, modelStores((configModel, configCohorts, configMeta)).vcfHailLog.local.get)
    //            .tag(s"${modelStores((configModel, configCohorts, configMeta)).vcf.get.data.local.get}".split("/").last)
    //  
    //        }
    //
    //    }
    //
    //    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.tabixClean.cpus, mem = projectConfig.resources.tabixClean.mem, maxRunTime = projectConfig.resources.tabixClean.maxRunTime) {
    //
    //      cmd"""${utils.binary.binTabix} -p vcf ${modelStores((configModel, configCohorts, configMeta)).vcf.get.data.local.get}"""
    //        .in(modelStores((configModel, configCohorts, configMeta)).vcf.get.data.local.get)
    //        .out(modelStores((configModel, configCohorts, configMeta)).vcf.get.tbi.local.get)
    //        .tag(s"${modelStores((configModel, configCohorts, configMeta)).vcf.get.tbi.local.get}".split("/").last)
    //    
    //    }
    //
    //  case None => ()
    //
    //}
  
  }

}
