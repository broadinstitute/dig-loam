object CrossCohort extends loamstream.LoamFile {

  /**
    * Cross Cohort Relatedness Step
    *  Description: Check for cross cohort relatedness
    *  Requires: Plink, King
    */
  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  import MetaStores._
  
  def CrossCohortCommonVars(configMeta: ConfigMeta): Unit = {
  
    val bimStringList = {
      for {
        cohort <- configMeta.cohorts
      } yield {
        arrayStores(projectConfig.Arrays.filter(e => e.id == projectConfig.Cohorts.filter(g => g.id == cohort).head.array).head).filteredData.plink.base.local.get.toString + ".bim"
      }
    }.distinct
  
    val dataList = {
      for {
        cohort <- configMeta.cohorts
      } yield {
        arrayStores(projectConfig.Arrays.filter(e => e.id == projectConfig.Cohorts.filter(g => g.id == cohort).head.array).head).filteredData.plink.data.local.get
      }
    }.flatten.distinct
  
    drmWith(imageName = s"${utils.image.imgTools}") {
  
      cmd"""${utils.bash.shCrossCohortCommonVariants} ${bimStringList.mkString(",")} ${metaKinshipStores(configMeta).commonVariants}"""
      .in(dataList)
      .out(metaKinshipStores(configMeta).commonVariants)
      .tag(s"${metaKinshipStores(configMeta).commonVariants}".split("/").last)
  
    }
  
  }
  
  def CrossCohortPrep(configMeta: ConfigMeta, configCohort: ConfigCohort): Unit = {
  
    val arrayCfg = projectConfig.Arrays.filter(e => e.id == configCohort.array).head
  
    val stratCol = {
      configCohort.stratCol match {
        case Some(a) => "--strat-col " + a
        case _ => ""
      }
    }
    
    val stratCodes = {
      configCohort.stratCodes match {
        case Some(a) => "--strat-codes " + a.mkString(",")
        case _ => ""
      }
    }
  
    drmWith(imageName = s"${utils.image.imgR}") {
  
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rMetaCohortSamples}
        --fam-in ${arrayStores(arrayCfg).filteredData.plink.base.local.get}.fam
        --ancestry-in ${projectStores.ancestryInferred.local.get}
        --ancestry-keep "${configCohort.ancestry.mkString(",")}"
        --pheno-in ${projectStores.phenoFile.local.get}
        --iid-col ${projectConfig.phenoFileId}
        ${stratCol}
        ${stratCodes}
        --out ${metaKinshipStores(configMeta).metaCohort(configCohort).samples}"""
      .in(arrayStores(arrayCfg).filteredData.plink.data.local.get :+ projectStores.phenoFile.local.get :+ projectStores.ancestryInferred.local.get)
      .out(metaKinshipStores(configMeta).metaCohort(configCohort).samples)
      .tag(s"${metaKinshipStores(configMeta).metaCohort(configCohort).samples}".split("/").last)
  
    }
  
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
  
      cmd"""${utils.binary.binPlink} --bfile ${arrayStores(arrayCfg).filteredData.plink.base.local.get} --allow-no-sex --extract ${metaKinshipStores(configMeta).commonVariants} --keep ${metaKinshipStores(configMeta).metaCohort(configCohort).samples} --make-bed --out ${metaKinshipStores(configMeta).metaCohort(configCohort).base} --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1"""
      .in(arrayStores(arrayCfg).filteredData.plink.data.local.get :+ metaKinshipStores(configMeta).metaCohort(configCohort).samples :+ metaKinshipStores(configMeta).commonVariants)
      .out(metaKinshipStores(configMeta).metaCohort(configCohort).data)
      .tag(s"${metaKinshipStores(configMeta).metaCohort(configCohort).base}".split("/").last)
  
    }
  
    drmWith(imageName = s"${utils.image.imgTools}") {
  
      cmd"""awk -v c=${configCohort.id} '{print $$1"_"c"t"$$2"_"c"t"$$3"t"$$4"t"$$5"t"$$6}' ${metaKinshipStores(configMeta).metaCohort(configCohort).base}.fam > ${metaKinshipStores(configMeta).metaCohort(configCohort).famMerge}"""
      .in(metaKinshipStores(configMeta).metaCohort(configCohort).data)
      .out(metaKinshipStores(configMeta).metaCohort(configCohort).famMerge)
      .tag(s"${metaKinshipStores(configMeta).metaCohort(configCohort).famMerge}".split("/").last)
  
    }
  
  }
  
  def CrossCohortKinship(configMeta: ConfigMeta): Unit = {
  
    val mergeString = metaKinshipStores(configMeta).metaCohort.map(e => e._2.base.toString + ".bed " + e._2.base.toString + ".bim " + e._2.famMerge.toString.split("@")(1)).mkString("n")
  
    drmWith(imageName = s"${utils.image.imgTools}") {
  
      cmd"""echo "${mergeString}" > ${metaKinshipStores(configMeta).mergeList}"""
      .out(metaKinshipStores(configMeta).mergeList)
      .tag(s"${metaKinshipStores(configMeta).mergeList}".split("/").last)
  
    }
  
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
  
      cmd"""${utils.binary.binPlink} --merge-list ${metaKinshipStores(configMeta).mergeList} --allow-no-sex --chr 1-22 --maf 0.01 --geno 0.02 --make-bed --out ${metaKinshipStores(configMeta).baseFiltered} --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1"""
      .in((metaKinshipStores(configMeta).metaCohort.map(e => e._2).flatMap(e => e.data).toSeq ++ metaKinshipStores(configMeta).metaCohort.map(e => e._2.famMerge).toSeq) :+ metaKinshipStores(configMeta).mergeList)
      .out(metaKinshipStores(configMeta).dataFiltered)
      .tag(s"${metaKinshipStores(configMeta).baseFiltered}".split("/").last)
  
    }
  
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.king.cpus, mem = projectConfig.resources.king.mem, maxRunTime = projectConfig.resources.king.maxRunTime) {
  
      cmd"""${utils.bash.shKing} ${utils.binary.binKing} ${metaKinshipStores(configMeta).baseFiltered}.bed ${metaKinshipStores(configMeta).baseKinship.toString} ${projectConfig.resources.king.cpus} ${metaKinshipStores(configMeta).log} ${metaKinshipStores(configMeta).kin0}"""
      .in(metaKinshipStores(configMeta).dataFiltered)
      .out(metaKinshipStores(configMeta).log, metaKinshipStores(configMeta).kin0)
      .tag(s"${metaKinshipStores(configMeta).baseFiltered}.shKing".split("/").last)
  
    }
  
  }
  
  //def CrossCohortExclude(configMeta: ConfigMeta, configCohort: ConfigCohort): Unit = {
  //
  //  drmWith(imageName = s"${utils.image.imgR}") {
  //  
  //    cmd"""${utils.binary.binRscript} --vanilla --verbose
  //      ${utils.r.rExcludeCrossArray}
  //      --kinship ${metaKinshipStores(configMeta).kin0}
  //      --samples ${metaKinshipStores(configMeta).metaCohort(configCohort).samples}
  //      --meta-cohorts ${configMeta.cohorts.mkString(",")}
  //      --cohort ${configCohort.id}
  //      --out ${metaKinshipStores(configMeta).metaCohort(configCohort).kinshipSamplesExclude}
  //      """
  //      .in(metaKinshipStores(configMeta).kin0, metaKinshipStores(configMeta).metaCohort(configCohort).samples)
  //      .out(metaKinshipStores(configMeta).metaCohort(configCohort).kinshipSamplesExclude)
  //      .tag(s"${metaKinshipStores(configMeta).metaCohort(configCohort).kinshipSamplesExclude}".split("/").last)
  //  
  //  }
  //
  //}

}
