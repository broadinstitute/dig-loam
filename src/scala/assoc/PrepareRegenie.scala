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

    val masksString = configSchema.masks match {
      case Some(_) =>
        configSchema.masks.get.size match {
          case n if n > 0 =>
            val x = "--masks"
            val y = for {
              k <- configSchema.masks.get
            } yield {
              s"""${k.id}"""
            }
            x + " " + y.mkString(",")
          case _ => ""
        }
      case None => ""
    }
    
    drmWith(imageName = s"${utils.image.imgPython3}", cores = projectConfig.resources.generateRegenieGroupfiles.cpus, mem = projectConfig.resources.generateRegenieGroupfiles.mem, maxRunTime = projectConfig.resources.generateRegenieGroupfiles.maxRunTime) {
    
      cmd"""${utils.binary.binPython} ${utils.python.pyGenerateRegenieGroupfiles}
        ${masksString}
        --filters ${schemaStores((configSchema, configCohorts)).variantFilterTable.base.local.get}
        --setlist-out ${schemaStores((configSchema, configCohorts)).regenie.get.setlist.base.local.get}
        --annotations-out ${schemaStores((configSchema, configCohorts)).regenie.get.annotations.base.local.get}
        --masks-out ${schemaStores((configSchema, configCohorts)).regenie.get.masks.base.local.get}"""
        .in(schemaStores((configSchema, configCohorts)).variantFilterTable.base.local.get)
        .out(schemaStores((configSchema, configCohorts)).regenie.get.setlist.base.local.get, schemaStores((configSchema, configCohorts)).regenie.get.annotations.base.local.get, schemaStores((configSchema, configCohorts)).regenie.get.masks.base.local.get)
        .tag(s"${schemaStores((configSchema, configCohorts)).regenie.get.setlist.base.local.get}".split("/").last)

    }
    
    for {
    
      pheno <- binaryFilterPhenos
    
    } yield {
    
      val masksString = schemaFilterFields.filter(e => e.schema.id == configSchema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
        case n if n > 0 =>
          configSchema.masks match {
            case Some(_) =>
              configSchema.masks.get.size match {
                case n if n > 0 =>
                  val x = "--masks"
                  val y = for {
                    k <- configSchema.masks.get
                  } yield {
                    s"""${k.id}"""
                  }
                  x + " " + y.mkString(",")
                case _ => ""
              }
            case None => ""
          }
        case _ => ""
      }
      
      drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
      
        cmd"""${utils.binary.binPython} ${utils.python.pyGenerateRegenieGroupfiles}
          ${masksString}
          --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).local.get}
          --setlist-out ${schemaStores((configSchema, configCohorts)).regenie.get.setlist.phenos(pheno).local.get}
          --annotations-out ${schemaStores((configSchema, configCohorts)).regenie.get.annotations.phenos(pheno).local.get}
          --masks-out ${schemaStores((configSchema, configCohorts)).regenie.get.masks.phenos(pheno).local.get}"""
          .in(schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).local.get)
          .out(schemaStores((configSchema, configCohorts)).regenie.get.setlist.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).regenie.get.annotations.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).regenie.get.masks.phenos(pheno).local.get)
          .tag(s"${schemaStores((configSchema, configCohorts)).regenie.get.setlist.phenos(pheno).local.get}".split("/").last)
      
      }
      
    }
    
  }

}
