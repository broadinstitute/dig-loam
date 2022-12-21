object PrepareRegenie extends loamstream.LoamFile {

  /**
   * Prepare Regenie Input Files
   * 
   */
  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  import SchemaStores._
  import MetaStores._
  import Fxns._
  import Collections._
  import DirTree._
  
  final case class CfgException(s: String) extends Exception(s)
  
  def PrepareRegenie(configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort]): Unit = {

    //val masksString = configSchema.masks match {
    //  case Some(_) =>
    //    configSchema.masks.get.size match {
    //      case n if n > 0 =>
    //        val x = "--masks"
    //        val y = for {
    //          k <- configSchema.masks.get
    //        } yield {
    //          s"""${k.id}"""
    //        }
    //        x + " " + y.mkString(",")
    //      case _ => ""
    //    }
    //  case None => ""
    //}

    drmWith(imageName = s"${utils.image.imgPython3}", cores = projectConfig.resources.generateRegenieGroupfiles.cpus, mem = projectConfig.resources.generateRegenieGroupfiles.mem, maxRunTime = projectConfig.resources.generateRegenieGroupfiles.maxRunTime) {
    
      cmd"""${utils.binary.binPython} ${utils.python.pyGenerateRegenieGroupfiles}
        --filters ${schemaStores((configSchema, configCohorts)).variantFilterTable.local.get}
        --setlist-out ${schemaStores((configSchema, configCohorts)).regenie.get.setlist.local.get}"""
        .in(schemaStores((configSchema, configCohorts)).variantFilterTable.local.get)
        .out(schemaStores((configSchema, configCohorts)).regenie.get.setlist.local.get)
        .tag(s"${schemaStores((configSchema, configCohorts)).regenie.get.setlist.local.get}".split("/").last)
	
    }

    //for {
    //
    //  pheno <- binaryFilterPhenos
    //
    //} yield {
	//
    //  drmWith(imageName = s"${utils.image.imgPython3}", cores = projectConfig.resources.generateRegenieGroupfiles.cpus, mem = projectConfig.resources.generateRegenieGroupfiles.mem, maxRunTime = projectConfig.resources.generateRegenieGroupfiles.maxRunTime) {
    //  
    //    cmd"""${utils.binary.binPython} ${utils.python.pyGenerateRegenieGroupfiles}
    //      --filters ${schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).local.get}
    //      --setlist-out ${schemaStores((configSchema, configCohorts)).regenie.get.setlist.phenos(pheno).local.get}"""
    //      .in(schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).local.get)
    //      .out(schemaStores((configSchema, configCohorts)).regenie.get.setlist.phenos(pheno).local.get)
    //      .tag(s"${schemaStores((configSchema, configCohorts)).regenie.get.setlist.phenos(pheno).local.get}".split("/").last)
    //  
    //  }
	//
    //}

    configSchema.masks match {

      case Some(_) =>

        for {

          mask <- configSchema.masks.get

        } yield {

            drmWith(imageName = s"${utils.image.imgPython3}", cores = projectConfig.resources.generateRegenieGroupfiles.cpus, mem = projectConfig.resources.generateRegenieGroupfiles.mem, maxRunTime = projectConfig.resources.generateRegenieGroupfiles.maxRunTime) {
            
              cmd"""${utils.binary.binPython} ${utils.python.pyGenerateRegenieGroupfiles}
                --mask ${mask.id}
                --filters ${schemaStores((configSchema, configCohorts)).variantFilterTable.local.get}
                --annotations-out ${schemaStores((configSchema, configCohorts)).regenie.get.annotations(mask).local.get}
                --masks-out ${schemaStores((configSchema, configCohorts)).regenie.get.masks(mask).local.get}"""
                .in(schemaStores((configSchema, configCohorts)).variantFilterTable.local.get)
                .out(schemaStores((configSchema, configCohorts)).regenie.get.annotations(mask).local.get, schemaStores((configSchema, configCohorts)).regenie.get.masks(mask).local.get)
                .tag(s"${schemaStores((configSchema, configCohorts)).regenie.get.annotations(mask).local.get}".split("/").last)
	        
            }
    
            //for {
            //
            //  pheno <- binaryFilterPhenos
            //
            //} yield {
            //  
            //  drmWith(imageName = s"${utils.image.imgPython3}", cores = projectConfig.resources.generateRegenieGroupfiles.cpus, mem = projectConfig.resources.generateRegenieGroupfiles.mem, maxRunTime = projectConfig.resources.generateRegenieGroupfiles.maxRunTime) {
            //  
            //    cmd"""${utils.binary.binPython} ${utils.python.pyGenerateRegenieGroupfiles}
            //      --mask ${mask.id}
            //      --filters ${schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).local.get}
            //      --annotations-out ${schemaStores((configSchema, configCohorts)).regenie.get.annotations(mask).phenos(pheno).local.get}
            //      --masks-out ${schemaStores((configSchema, configCohorts)).regenie.get.masks(mask).phenos(pheno).local.get}"""
            //      .in(schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).local.get)
            //      .out(schemaStores((configSchema, configCohorts)).regenie.get.annotations(mask).phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).regenie.get.masks(mask).phenos(pheno).local.get)
            //      .tag(s"${schemaStores((configSchema, configCohorts)).regenie.get.annotations(mask).phenos(pheno).local.get}".split("/").last)
            //  
            //  }
            //  
            //}
        }

      case None => ()

    }
  }
}
