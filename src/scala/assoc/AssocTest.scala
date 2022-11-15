object AssocTest extends loamstream.LoamFile {

  /**
   * Run Assoc Analysis
   * 
   */
  import ProjectConfig._
  import ModelStores._
  import ArrayStores._
  import Fxns._
  import SchemaStores._
  import ProjectStores._
  import DirTree._
  import AssocSingleHail._
  import AssocGroupEpacts._
  import AssocRegenie._ 
  //import MinPVal._ 

  def AssocTest(configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {

    val pheno = projectConfig.Phenos.filter(e => e.id == configModel.pheno).head

    projectConfig.Tests.filter(e => configModel.tests.get.contains(e.id)).filter(e => e.platform == "hail").size match {

      case n if n > 0 =>

        projectConfig.hailCloud match {
        
          case true =>
        
            local {
            
              googleCopy(modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.google.get)
              googleCopy(modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.google.get)
            
            }
        
          case false => ()
        
        }

        for {
        
          test <- modelStores((configModel, configSchema, configCohorts, configMeta)).hail.get.assocSingle.keys
        
        } yield {
        
          AssocSingleHail(test, configModel, configSchema, configCohorts, None)
        
        }

      case _ => ()

    }

    projectConfig.Tests.filter(e => configModel.tests.get.contains(e.id)).filter(e => e.grouped == true && e.platform == "epacts").size match {

      case n if n > 0 =>
      
        //val groupCountMap = scala.collection.mutable.Map[String, Int]()
        //
        //val gFile = schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
        //  case true => s"""${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.local.get.toString.split("@")(1)}"""
        //  case false => s"""${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.local.get.toString.split("@")(1)}"""
        //}
        //try {
        //  val gFileList = fileToList(checkPath(gFile))
        //  println(s"""calculating group variant counts for group file: ${gFile}""")
        //  for {
        //    group <- gFileList
        //  } yield {
        //    val geneName = group.split("\t")(0)
        //    val N = group.split("\t").tail.size
        //    groupCountMap(geneName) = N
        //  }
        //}
        //catch {
        //  case x: CfgException =>
        //    println(s"""skipping group variant count calculation due to missing group file: ${gFile}""")
        //}
        //
        //for {
        //
        //  test <- modelStores((configModel, configSchema, configCohorts, configMeta)).epacts.get.assocGroup.keys
        //
        //} yield {
        //
        //  AssocGroupEpacts(test, groupCountMap, configModel, configSchema, configCohorts, None)
        //
        //}

        configSchema.masks.size match {

          case m if m > 0 =>

            val groupCountMap = scala.collection.mutable.Map[String, Int]()
            
            schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
              case true => 
                for {
                  gm <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks.keys.toList
                } yield {
                  val gFile = s"""${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks(gm).local.get.toString.split("@")(1)}"""
                  try {
                    val gFileList = fileToList(checkPath(gFile))
                    println(s"""calculating group variant counts for group file: ${gFile}""")
                    for {
                      group <- gFileList
                    } yield {
                      val geneName = group.split("\t")(0)
                      val N = group.split("\t").tail.size
  	            	groupCountMap.keys.toList.contains(geneName) match {
                        case true =>
                          groupCountMap(geneName) < N match {
                            case true => groupCountMap(geneName) = N
                            case false => ()
                          }
                        case false => groupCountMap(geneName) = N
                      }
                    }
                  }
                  catch {
                    case x: CfgException =>
                      println(s"""skipping group variant count calculation due to missing group file: ${gFile}""")
                  }
                }
              case false =>
                for {
                  gm <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks.keys.toList
                } yield {
                  val gFile = s"""${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks(gm).local.get.toString.split("@")(1)}"""
                  try {
                    val gFileList = fileToList(checkPath(gFile))
                    println(s"""calculating group variant counts for group file: ${gFile}""")
                    for {
                      group <- gFileList
                    } yield {
                      val geneName = group.split("\t")(0)
                      val N = group.split("\t").tail.size
  	            	groupCountMap.keys.toList.contains(geneName) match {
                        case true =>
                          groupCountMap(geneName) < N match {
                            case true => groupCountMap(geneName) = N
                            case false => ()
                          }
                        case false => groupCountMap(geneName) = N
                      }
                    }
                  }
                  catch {
                    case x: CfgException =>
                      println(s"""skipping group variant count calculation due to missing group file: ${gFile}""")
                  }
                }
            }
            
            for {
            
              test <- modelStores((configModel, configSchema, configCohorts, configMeta)).epacts.get.assocGroup.keys
            
            } yield {
            
              AssocGroupEpacts(test, groupCountMap, configModel, configSchema, configCohorts, None)
            
            }

          case _ => ()

        }
      
      case _ => ()

    }

    projectConfig.Tests.filter(e => configModel.tests.get.contains(e.id)).filter(e => e.platform == "regenie").size match {

      case n if n > 0 =>
        
        AssocRegenieStep1(configModel, configSchema, configCohorts, None)

      case _ => ()
        
    }

    projectConfig.Tests.filter(e => configModel.tests.get.contains(e.id)).filter(e => e.grouped == false && e.platform == "regenie").size match {

      case n if n > 0 =>

        for {
           
          test <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocSingle.keys
        
        } yield {

          AssocRegenieStep2Single(test, configModel, configSchema, configCohorts, None)

        }

      case _ => ()
        
    }

    projectConfig.Tests.filter(e => configModel.tests.get.contains(e.id)).filter(e => e.grouped == true && e.platform == "regenie").size match {

      case n if n > 0 =>

        for {
           
          test <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup.keys
          mask <- modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).keys
        
        } yield {

          AssocRegenieStep2Group(test, mask, configModel, configSchema, configCohorts, None)
          //MinPVal(test, mask, configModel, configSchema, configCohorts, None)

        }

      case _ => ()
        
    }
  
  }

}
