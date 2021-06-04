object PrepareEpacts extends loamstream.LoamFile {

  /**
   * Prepare Epacts Input Files
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
  
  def PrepareEpacts(configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort]): Unit = {
  
    projectConfig.hailCloud match {
    
      case true =>
    
        val maskedGroupFilesString = schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks.size match {
          case n if n > 0 =>
            val x = "--masked-groupfiles-out"
            val y = for {
              (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks
            } yield {
              s"""${k.id},${v.google.get.toString.split("@")(1)}"""
            }
            x + " " + y.mkString(" ")
          case _ => ""
        }
    
        var generateGroupfileOut = Seq(schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.google.get, schemaStores((configSchema, configCohorts)).epacts.get.hailLog.base.google.get)
    
        schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks.size match {
            case n if n > 0 =>
              generateGroupfileOut = generateGroupfileOut ++ {
                for {
                  (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks
                } yield {
                  v.google.get
                }
              }
            case _ => ()
        }
    
        //val regenieMasksString = schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks.size match {
        //  case n if n > 0 =>
        //    val x = "--masks"
        //    val y = for {
        //      (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks
        //    } yield {
        //      s"""${k.id}"""
        //    }
        //    x + " " + y.mkString(",")
        //  case _ => ""
        //}
    
        googleWith(projectConfig.cloudResources.mtCluster) {
        
          hail"""${utils.python.pyHailGenerateGroupfile} --
            --cloud
            --hail-utils ${projectStores.hailUtils.google.get}
            ${maskedGroupFilesString}
            --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.google.get}
            --groupfile-out ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.google.get}
            --log ${schemaStores((configSchema, configCohorts)).epacts.get.hailLog.base.google.get}"""
            .in(projectStores.hailUtils.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.google.get)
            .out(generateGroupfileOut)
            .tag(s"${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.local.get}.google".split("/").last)
    
          //hail"""${utils.python.pyHailGenerateRegenieGroupfiles} --
          //  --cloud
          //  --hail-utils ${projectStores.hailUtils.google.get}
          //  ${regenieMasksString}
          //  --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.google.get}
          //  --setlist-out ${schemaStores((configSchema, configCohorts)).regenie.setlist.base.google.get}
          //  --annotations-out ${schemaStores((configSchema, configCohorts)).regenie.annotations.base.google.get}
          //  --masks-out ${schemaStores((configSchema, configCohorts)).regenie.masks.base.google.get}
          //  --log ${schemaStores((configSchema, configCohorts)).regenieHailLog.base.google.get}"""
          //  .in(projectStores.hailUtils.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.google.get)
          //  .out(schemaStores((configSchema, configCohorts)).regenie.setlist.base.google.get, schemaStores((configSchema, configCohorts)).regenie.annotations.base.google.get, schemaStores((configSchema, configCohorts)).regenie.masks.base.google.get, schemaStores((configSchema, configCohorts)).regenieHailLog.base.google.get)
          //  .tag(s"${schemaStores((configSchema, configCohorts)).regenie.setlist.base.google.get}.google".split("/").last)
    
        }
    
        local {
        
          googleCopy(schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.google.get, schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.local.get)
          googleCopy(schemaStores((configSchema, configCohorts)).epacts.get.hailLog.base.google.get, schemaStores((configSchema, configCohorts)).epacts.get.hailLog.base.local.get)
          //googleCopy(schemaStores((configSchema, configCohorts)).regenie.setlist.base.google.get, schemaStores((configSchema, configCohorts)).regenie.setlist.base.local.get)
          //googleCopy(schemaStores((configSchema, configCohorts)).regenie.annotations.base.google.get, schemaStores((configSchema, configCohorts)).regenie.annotations.base.local.get)
          //googleCopy(schemaStores((configSchema, configCohorts)).regenie.masks.base.google.get, schemaStores((configSchema, configCohorts)).regenie.masks.base.local.get)
          //googleCopy(schemaStores((configSchema, configCohorts)).regenieHailLog.base.google.get, schemaStores((configSchema, configCohorts)).regenieHailLog.base.local.get)
        
        }
        
        schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks.size match {
          case n if n > 0 =>
            for {
              (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks
            } yield {
              local {
                googleCopy(v.google.get, v.local.get)
              }
            }
          case _ => ()
        }
    
      case false =>
    
        val maskedGroupFilesString = schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks.size match {
          case n if n > 0 =>
            val x = "--masked-groupfiles-out"
            val y = for {
              (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks
            } yield {
              s"""${k.id},${v.local.get.toString.split("@")(1)}"""
            }
            x + " " + y.mkString(" ")
          case _ => ""
        }
        
        var generateGroupfileOut = Seq(schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.local.get, schemaStores((configSchema, configCohorts)).epacts.get.hailLog.base.local.get)
        
        schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks.size match {
          case n if n > 0 =>
            generateGroupfileOut = generateGroupfileOut ++ {
              for {
                (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks
              } yield {
                v.local.get
              }
            }
          case _ => ()
        }
    
        //val regenieMasksString = schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks.size match {
        //  case n if n > 0 =>
        //    val x = "--masks"
        //    val y = for {
        //      (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks
        //    } yield {
        //      s"""${k.id}"""
        //    }
        //    x + " " + y.mkString(",")
        //  case _ => ""
        //}
    
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyHailGenerateGroupfile}
            ${maskedGroupFilesString}
            --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.local.get}
            --groupfile-out ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.local.get}
            --log ${schemaStores((configSchema, configCohorts)).epacts.get.hailLog.base.local.get}"""
            .in(schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.local.get)
            .out(generateGroupfileOut)
            .tag(s"${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.local.get}".split("/").last)
    
          //cmd"""${utils.binary.binPython} ${utils.python.pyHailGenerateRegenieGroupfiles}
          //  ${regenieMasksString}
          //  --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.local.get}
          //  --setlist-out ${schemaStores((configSchema, configCohorts)).regenie.setlist.base.local.get}
          //  --annotations-out ${schemaStores((configSchema, configCohorts)).regenie.annotations.base.local.get}
          //  --masks-out ${schemaStores((configSchema, configCohorts)).regenie.masks.base.local.get}
          //  --log ${schemaStores((configSchema, configCohorts)).regenieHailLog.base.local.get}"""
          //  .in(schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.local.get)
          //  .out(schemaStores((configSchema, configCohorts)).regenie.setlist.base.local.get, schemaStores((configSchema, configCohorts)).regenie.annotations.base.local.get, schemaStores((configSchema, configCohorts)).regenie.masks.base.local.get, schemaStores((configSchema, configCohorts)).regenieHailLog.base.local.get)
          //  .tag(s"${schemaStores((configSchema, configCohorts)).regenie.setlist.base.local.get}".split("/").last)
        
        }
    
    }
    
    for {
    
      pheno <- binaryFilterPhenos
    
    } yield {
    
      projectConfig.hailCloud match {
    
        case true =>
      
          val maskedGroupFilesString = schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks.size match {
            case n if n > 0 =>
              val x = "--masked-groupfiles-out"
              val y = for {
                (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks
              } yield {
                s"""${k.id},${v.google.get.toString.split("@")(1)}"""
              }
              x + " " + y.mkString(" ")
            case _ => ""
          }
      
          var generateGroupfileOut = Seq(schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.google.get, schemaStores((configSchema, configCohorts)).epacts.get.hailLog.phenos(pheno).google.get)
      
          schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks.size match {
              case n if n > 0 =>
                generateGroupfileOut = generateGroupfileOut ++ {
                  for {
                    (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks
                  } yield {
                    v.google.get
                  }
                }
              case _ => ()
          }
    
          //val regenieMasksString = schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks.size match {
          //  case n if n > 0 =>
          //    val x = "--masks"
          //    val y = for {
          //      (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks
          //    } yield {
          //      s"""${k.id}"""
          //    }
          //    x + " " + y.mkString(",")
          //  case _ => ""
          //}
      
          googleWith(projectConfig.cloudResources.mtCluster) {
          
            hail"""${utils.python.pyHailGenerateGroupfile} --
              --cloud
              --hail-utils ${projectStores.hailUtils.google.get}
              ${maskedGroupFilesString}
              --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).google.get}
              --groupfile-out ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.google.get}
              --log ${schemaStores((configSchema, configCohorts)).epacts.get.hailLog.phenos(pheno).google.get}"""
              .in(projectStores.hailUtils.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).google.get)
              .out(generateGroupfileOut)
              .tag(s"${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.local.get}.google".split("/").last)
    
            //hail"""${utils.python.pyHailGenerateRegenieGroupfiles} --
            //  --cloud
            //  --hail-utils ${projectStores.hailUtils.google.get}
            //  ${regenieMasksString}
            //  --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).google.get}
            //  --setlist-out ${schemaStores((configSchema, configCohorts)).regenie.setlist.phenos(pheno).google.get}
            //  --annotations-out ${schemaStores((configSchema, configCohorts)).regenie.annotations.phenos(pheno).google.get}
            //  --masks-out ${schemaStores((configSchema, configCohorts)).regenie.masks.phenos(pheno).google.get}
            //  --log ${schemaStores((configSchema, configCohorts)).regenieHailLog.phenos(pheno).google.get}"""
            //  .in(projectStores.hailUtils.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).google.get)
            //  .out(schemaStores((configSchema, configCohorts)).regenie.setlist.phenos(pheno).google.get, schemaStores((configSchema, configCohorts)).regenie.annotations.phenos(pheno).google.get, schemaStores((configSchema, configCohorts)).regenie.masks.phenos(pheno).google.get, schemaStores((configSchema, configCohorts)).regenieHailLog.phenos(pheno).google.get)
            //  .tag(s"${schemaStores((configSchema, configCohorts)).regenie.setlist.phenos(pheno).google.get}.google".split("/").last)
      
          }
      
          local {
          
            googleCopy(schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.google.get, schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.local.get)
            googleCopy(schemaStores((configSchema, configCohorts)).epacts.get.hailLog.phenos(pheno).google.get, schemaStores((configSchema, configCohorts)).epacts.get.hailLog.phenos(pheno).local.get)
          
          }
          
          schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks.size match {
            case n if n > 0 =>
              for {
                (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks
              } yield {
                local {
                  googleCopy(v.google.get, v.local.get)
                }
              }
            case _ => ()
          }
      
        case false =>
      
          val maskedGroupFilesString = schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks.size match {
            case n if n > 0 =>
              val x = "--masked-groupfiles-out"
              val y = for {
                (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks
              } yield {
                s"""${k.id},${v.local.get.toString.split("@")(1)}"""
              }
              x + " " + y.mkString(" ")
            case _ => ""
          }
          
          var generateGroupfileOut = Seq(schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.local.get, schemaStores((configSchema, configCohorts)).epacts.get.hailLog.phenos(pheno).local.get)
          
          schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks.size match {
            case n if n > 0 =>
              generateGroupfileOut = generateGroupfileOut ++ {
                for {
                  (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks
                } yield {
                  v.local.get
                }
              }
            case _ => ()
          }
    
          //val regenieMasksString = schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks.size match {
          //  case n if n > 0 =>
          //    val x = "--masks"
          //    val y = for {
          //      (k, v) <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks
          //    } yield {
          //      s"""${k.id}"""
          //    }
          //    x + " " + y.mkString(",")
          //  case _ => ""
          //}
      
          drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
          
            cmd"""${utils.binary.binPython} ${utils.python.pyHailGenerateGroupfile}
              ${maskedGroupFilesString}
              --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).local.get}
              --groupfile-out ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.local.get}
              --log ${schemaStores((configSchema, configCohorts)).epacts.get.hailLog.phenos(pheno).local.get}"""
              .in(schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).local.get)
              .out(generateGroupfileOut)
              .tag(s"${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.local.get}".split("/").last)
    
            //cmd"""${utils.binary.binPython} ${utils.python.pyHailGenerateRegenieGroupfiles}
            //  ${regenieMasksString}
            //  --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).local.get}
            //  --setlist-out ${schemaStores((configSchema, configCohorts)).regenie.setlist.phenos(pheno).local.get}
            //  --annotations-out ${schemaStores((configSchema, configCohorts)).regenie.annotations.phenos(pheno).local.get}
            //  --masks-out ${schemaStores((configSchema, configCohorts)).regenie.masks.phenos(pheno).local.get}
            //  --log ${schemaStores((configSchema, configCohorts)).regenieHailLog.phenos(pheno).local.get}"""
            //  .in(schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).local.get)
            //  .out(schemaStores((configSchema, configCohorts)).regenie.setlist.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).regenie.annotations.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).regenie.masks.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).regenieHailLog.phenos(pheno).local.get)
            //  .tag(s"${schemaStores((configSchema, configCohorts)).regenie.setlist.phenos(pheno).local.get}".split("/").last)
        
          
          }
      
      }
    
    }

  }

}
