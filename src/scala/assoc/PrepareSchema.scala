object PrepareSchema extends loamstream.LoamFile {

  /**
   * Prepare Model Cohorts
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
  
  def PrepareSchema(configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort]): Unit = {
  
    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head
    
    val stratStrings = {
      for {
        cohort <- configCohorts
      } yield {
        var s = "--strat " + cohort.id + " " + cohort.ancestry.mkString(",")
        cohort.stratCol match {
          case Some(a) => s = s + " " + a
          case None => s = s + " " + """"N/A""""
        }
        cohort.stratCodes match {
          case Some(a) => s = s + " " + a.mkString(",")
          case None => s = s + " " + """"N/A""""
        }
        s
      }
    }

    var cohortSamplesAvailableIn = arrayStores(array).filteredPlink.data :+ arrayStores(array).phenoFile.local.get :+ arrayStores(array).ancestryMap

    arrayStores(array).samplesExclude.size match {
      case n if n > 0 =>
        cohortSamplesAvailableIn = cohortSamplesAvailableIn ++ {
          for {
            f <- arrayStores(array).samplesExclude
          } yield {
            f.local.get
          }
        }
      case _ => ()
    }

    val samplesExcludeString = {
      arrayStores(array).samplesExclude.size match {
        case n if n > 0 =>
          val x = "--samples-exclude"
          val y = for {
            f <- arrayStores(array).samplesExclude
          } yield {
            s"""${f.local.get.toString.split("@")(1)}"""
          }
          x + " " + y.mkString(",")
        case _ => ""
      }
    }

    drmWith(imageName = s"${utils.image.imgR}") {
  
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rSchemaCohortSamplesAvailable}
        --pheno-in ${arrayStores(array).phenoFile.local.get}
        --fam-in ${arrayStores(array).filteredPlink.base}.fam
        --ancestry-in ${arrayStores(array).ancestryMap}
        ${stratStrings.mkString(" ")}
        --iid-col ${array.phenoFileId}
        ${samplesExcludeString}
        --out-id-map ${schemaStores((configSchema, configCohorts)).sampleMap}
        --out-cohorts-map ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
        --out ${schemaStores((configSchema, configCohorts)).samplesAvailable}
        > ${schemaStores((configSchema, configCohorts)).samplesAvailableLog}"""
        .in(cohortSamplesAvailableIn)
        .out(schemaStores((configSchema, configCohorts)).sampleMap, schemaStores((configSchema, configCohorts)).cohortMap.local.get, schemaStores((configSchema, configCohorts)).samplesAvailable, schemaStores((configSchema, configCohorts)).samplesAvailableLog)
        .tag(s"${schemaStores((configSchema, configCohorts)).samplesAvailable}".split("/").last)
    
    }
    
    projectConfig.hailCloud match {
    
      case true =>
    
        local {
  
          googleCopy(schemaStores((configSchema, configCohorts)).cohortMap.local.get, schemaStores((configSchema, configCohorts)).cohortMap.google.get)
        
        }
        
        googleWith(projectConfig.cloudResources.mtCluster) {
        
          hail"""${utils.python.pyHailSchemaVariantStats} --
            --hail-utils ${projectStores.hailUtils.google.get}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).mt.get.google.get}
            --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.google.get}
            --variants-stats-out ${schemaStores((configSchema, configCohorts)).variantsStats.base.google.get}
            --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get}
            --cloud
            --log ${schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.google.get}"""
              .in(projectStores.hailUtils.google.get, arrayStores(array).mt.get.google.get, schemaStores((configSchema, configCohorts)).cohortMap.google.get)
              .out(schemaStores((configSchema, configCohorts)).variantsStats.base.google.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.google.get)
              .tag(s"${schemaStores((configSchema, configCohorts)).variantsStats.base.local.get}.google".split("/").last)
        
        }
        
        local {
        
          googleCopy(schemaStores((configSchema, configCohorts)).variantsStats.base.google.get, schemaStores((configSchema, configCohorts)).variantsStats.base.local.get)
          googleCopy(schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.google.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.local.get)
        
        }
    
      case false =>
    
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyHailSchemaVariantStats}
            --driver-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
            --executor-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
            --tmp-dir ${projectStores.tmpDir}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).mt.get.local.get}
            --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
            --variants-stats-out ${schemaStores((configSchema, configCohorts)).variantsStats.base.local.get}
            --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get}
            --log ${schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.local.get}"""
              .in(arrayStores(array).mt.get.local.get, schemaStores((configSchema, configCohorts)).cohortMap.local.get, projectStores.tmpDir)
              .out(schemaStores((configSchema, configCohorts)).variantsStats.base.local.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.local.get)
              .tag(s"${schemaStores((configSchema, configCohorts)).variantsStats.base.local.get}".split("/").last)
        
        }
    
    }
    
    (configSchema.design, configSchema.filterCohorts.size) match {
    
      case ("full", n) if n > 0 =>
        
        for {
        
          cohort <- configCohorts if configSchema.filterCohorts.contains(cohort.id)
        
        } yield {
        
          projectConfig.hailCloud match {
          
            case true =>
          
              googleWith(projectConfig.cloudResources.mtCluster) {
            
                hail"""${utils.python.pyHailSchemaVariantStats} --
                  --hail-utils ${projectStores.hailUtils.google.get}
                  --reference-genome ${projectConfig.referenceGenome}
                  --mt-in ${arrayStores(array).mt.get.google.get}
                  --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.google.get}
                  --cohort ${cohort.id}
                  --variants-stats-out ${schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).google.get}
                  --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts(cohort).google.get}
                  --cloud
                  --log ${schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).google.get}"""
                    .in(projectStores.hailUtils.google.get, arrayStores(array).mt.get.google.get, schemaStores((configSchema, configCohorts)).cohortMap.google.get)
                    .out(schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).google.get)
                    .tag(s"${schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).local.get}.google".split("/").last)
              
              }
              
              local {
              
                googleCopy(schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).local.get)
                googleCopy(schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).local.get)
              
              }
        
            case false =>
            
              drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
              
                  cmd"""${utils.binary.binPython} ${utils.python.pyHailSchemaVariantStats}
                    --driver-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
                    --executor-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
                    --tmp-dir ${projectStores.tmpDir}
                    --reference-genome ${projectConfig.referenceGenome}
                    --mt-in ${arrayStores(array).mt.get.local.get}
                    --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
                    --cohort ${cohort.id}
                    --variants-stats-out ${schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).local.get}
                    --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts(cohort).local.get}
                    --log ${schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).local.get}"""
                      .in(arrayStores(array).mt.get.local.get, schemaStores((configSchema, configCohorts)).cohortMap.local.get, projectStores.tmpDir)
                      .out(schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).local.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts(cohort).local.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).local.get)
                      .tag(s"${schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).local.get}".split("/").last)
                
              }
        
          }
        
        }
    
      case _ => ()
    
    }
  
    var filters = Seq[String]()
    var cohortFilters = Seq[String]()
    var knockoutFilters = Seq[String]()
    var masks = Seq[String]()
    configSchema.filters match {
      case Some(l) =>
        filters = filters ++ variantFiltersToPrintableList(cfg = projectConfig, filters = l)
      case None => ()
    }
    (configSchema.design, configSchema.cohortFilters) match {
      case ("full", Some(l)) =>
        for {
          cf <- l if configCohorts.map(e => e.id).contains(cf.cohort)
        } yield {
          cohortFilters = cohortFilters ++ variantFiltersToPrintableList(cfg = projectConfig, filters = cf.filters, id = Some(cf.cohort))
        }
      case ("strat", Some(l)) =>
        for {
          cf <- l if configCohorts.head.id == cf.cohort
        } yield {
          filters = filters ++ variantFiltersToPrintableList(cfg = projectConfig, filters = cf.filters)
        }
      case _ => ()
    }
    (configSchema.design, configSchema.knockoutFilters) match {
      case ("full", Some(l)) =>
        for {
          cf <- l if configCohorts.map(e => e.id).contains(cf.cohort)
        } yield {
          knockoutFilters = knockoutFilters ++ variantFiltersToPrintableList(cfg = projectConfig, filters = cf.filters, id = Some(cf.cohort))
        }
      case _ => ()
    }
    configSchema.masks match {
      case Some(l) =>
        for {
          mf <- l
        } yield {
          masks = masks ++ variantFiltersToPrintableList(cfg = projectConfig, filters = mf.filters, id = Some(mf.id))
        }
      case None => ()
    }
  
    val fString = filters.size match {
    
      case n if n > 0 => s"""${filters.mkString("\n")}"""
      case _ => ""
    
    }

    writeText(text = s"${fString}", filename = s"${schemaStores((configSchema, configCohorts)).filters.local.get.toString.split("@")(1)}")
    
    val cfString = cohortFilters.size match {
    
      case n if n > 0 => s"""${cohortFilters.mkString("\n")}"""
      case _ => ""
    
    }

    writeText(text = s"${cfString}", filename = s"${schemaStores((configSchema, configCohorts)).cohortFilters.local.get.toString.split("@")(1)}")
    
    val kfString = knockoutFilters.size match {
    
      case n if n > 0 => s"""${knockoutFilters.mkString("\n")}"""
      case _ => ""
    
    }

    writeText(text = s"${kfString}", filename = s"${schemaStores((configSchema, configCohorts)).knockoutFilters.local.get.toString.split("@")(1)}")
    
    val mString = masks.size match {
    
      case n if n > 0 => s"""${masks.mkString("\n")}"""
      case _ => ""
    
    }

    writeText(text = s"${mString}", filename = s"${schemaStores((configSchema, configCohorts)).masks.local.get.toString.split("@")(1)}")
    
    val userAnnotationsInString = projectConfig.hailCloud match {

      case true =>

        projectConfig.annotationTables.size match {
          case n if n > 0 =>
            val x = "--user-annotations"
            val y = for {
              a <- projectConfig.annotationTables
            } yield {
              s"""${a.id},${a.includeGeneInIdx.toString.toLowerCase},${projectStores.annotationStores(a).google.get.toString.split("@")(1)}"""
            }
            x + " " + y.mkString(" ")
          case _ => ""
        }

      case false =>

        projectConfig.annotationTables.size match {
          case n if n > 0 =>
            val x = "--user-annotations"
            val y = for {
              a <- projectConfig.annotationTables
            } yield {
              s"""${a.id},${a.includeGeneInIdx.toString.toLowerCase},${projectStores.annotationStores(a).local.get.toString.split("@")(1)}"""
            }
            x + " " + y.mkString(" ")
          case _ => ""
        }

    }

    val userAnnotationsIn = projectConfig.hailCloud match {

      case true =>

        projectConfig.annotationTables.size match {
          case n if n > 0 =>
            for {
              a <- projectConfig.annotationTables
            } yield {
              projectStores.annotationStores(a).google.get
            }
          case _ => Seq()
        }

      case false =>

        projectConfig.annotationTables.size match {
          case n if n > 0 =>
            for {
              a <- projectConfig.annotationTables
            } yield {
              projectStores.annotationStores(a).local.get
            }
          case _ => Seq()
        }

    }
  
    projectConfig.hailCloud match {
    
      case true =>
    
        local {
    
          googleCopy(schemaStores((configSchema, configCohorts)).filters.local.get, schemaStores((configSchema, configCohorts)).filters.google.get)
          googleCopy(schemaStores((configSchema, configCohorts)).masks.local.get, schemaStores((configSchema, configCohorts)).masks.google.get)
          googleCopy(schemaStores((configSchema, configCohorts)).cohortFilters.local.get, schemaStores((configSchema, configCohorts)).cohortFilters.google.get)
          googleCopy(schemaStores((configSchema, configCohorts)).knockoutFilters.local.get, schemaStores((configSchema, configCohorts)).knockoutFilters.google.get)
        
        }
    
        var cohortStatsInString = {
          schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts.size match {
            case n if n > 0 =>
              val x = "--cohort-stats-in"
              val y = for {
                (k, v) <- schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts
              } yield {
                s"""${k.id},${v.google.get.toString.split("@")(1)}"""
              }
              x + " " + y.mkString(" ")
            case _ => ""
          }
        }

        
    
        var cohortStatsIn = Seq(projectStores.hailUtils.google.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get, schemaStores((configSchema, configCohorts)).filters.google.get, schemaStores((configSchema, configCohorts)).masks.google.get, schemaStores((configSchema, configCohorts)).cohortFilters.google.get, schemaStores((configSchema, configCohorts)).knockoutFilters.google.get)

        arrayStores(array).annotationsHt match {
          case Some(s) =>
            cohortStatsInString = cohortStatsInString + s""" --annotation ${s.google.get.toString.split("@")(1)}"""
            cohortStatsIn = cohortStatsIn :+ s.google.get
          case None => ()
        }
        
        schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts.size match {
          case n if n > 0 =>
            cohortStatsIn = cohortStatsIn ++ {
              for {
                (k, v) <- schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts
              } yield {
                v.google.get
              }
            }
          case _ => ()
        }

        arrayStores(array).variantsExclude.size match {
          case n if n > 0 =>
            cohortStatsIn = cohortStatsIn ++ {
              for {
                f <- arrayStores(array).variantsExclude
              } yield {
                f.google.get
              }
            }
          case _ => ()
        }

        val variantsRemoveString = {
          arrayStores(array).variantsExclude.size match {
            case n if n > 0 =>
              val x = "--variants-remove"
              val y = for {
                f <- arrayStores(array).variantsExclude
              } yield {
                s"""${f.google.get.toString.split("@")(1)}"""
              }
              x + " " + y.mkString(",")
            case _ => ""
          }
        }
  
        googleWith(projectConfig.cloudResources.variantHtCluster) {
        
          hail"""${utils.python.pyHailFilterSchemaVariants} --
            --cloud
            --hail-utils ${projectStores.hailUtils.google.get}
            --reference-genome ${projectConfig.referenceGenome}
            --full-stats-in ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get}
            ${cohortStatsInString}
            ${userAnnotationsInString}
            --filters ${schemaStores((configSchema, configCohorts)).filters.google.get}
            --cohort-filters ${schemaStores((configSchema, configCohorts)).cohortFilters.google.get}
            --knockout-filters ${schemaStores((configSchema, configCohorts)).knockoutFilters.google.get}
            --masks ${schemaStores((configSchema, configCohorts)).masks.google.get}
            ${variantsRemoveString}
            --variant-filters-out ${schemaStores((configSchema, configCohorts)).variantFilterTable.google.get}
            --variant-filters-ht-out ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.google.get}
            --log ${schemaStores((configSchema, configCohorts)).variantFilterHailLog.google.get}"""
              .in(cohortStatsIn ++ userAnnotationsIn)
              .out(schemaStores((configSchema, configCohorts)).variantFilterTable.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailLog.google.get)
              .tag(s"${schemaStores((configSchema, configCohorts)).variantFilterTable.local.get}.google".split("/").last)
        
        }
        
        local {
        
          googleCopy(schemaStores((configSchema, configCohorts)).variantFilterTable.google.get, schemaStores((configSchema, configCohorts)).variantFilterTable.local.get)
          googleCopy(schemaStores((configSchema, configCohorts)).variantFilterHailLog.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailLog.local.get)
        
        }
    
      case false =>
    
        var cohortStatsInString = {
          schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts.size match {
            case n if n > 0 =>
              val x = "--cohort-stats-in"
              val y = for {
                (k, v) <- schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts
              } yield {
                s"""${k.id},${v.local.get.toString.split("@")(1)}"""
              }
              x + " " + y.mkString(" ")
            case _ => ""
          }
        }
        
        var cohortStatsIn = Seq(schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get, schemaStores((configSchema, configCohorts)).filters.local.get, schemaStores((configSchema, configCohorts)).masks.local.get,schemaStores((configSchema, configCohorts)).cohortFilters.local.get, schemaStores((configSchema, configCohorts)).knockoutFilters.local.get, projectStores.tmpDir)

        arrayStores(array).annotationsHt match {
          case Some(s) =>
            cohortStatsInString = cohortStatsInString + s""" --annotation ${s.local.get.toString.split("@")(1)}"""
            cohortStatsIn = cohortStatsIn :+ s.local.get
          case None => ()
        }
        
        schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts.size match {
          case n if n > 0 =>
            cohortStatsIn = cohortStatsIn ++ {
              for {
                (k, v) <- schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts
              } yield {
                v.local.get
              }
            }
          case _ => ()
        }

        arrayStores(array).variantsExclude.size match {
          case n if n > 0 =>
            cohortStatsIn = cohortStatsIn ++ {
              for {
                f <- arrayStores(array).variantsExclude
              } yield {
                f.local.get
              }
            }
          case _ => ()
        }

        val variantsRemoveString = {
          arrayStores(array).variantsExclude.size match {
            case n if n > 0 =>
              val x = "--variants-remove"
              val y = for {
                f <- arrayStores(array).variantsExclude
              } yield {
                s"""${f.local.get.toString.split("@")(1)}"""
              }
              x + " " + y.mkString(",")
            case _ => ""
          }
        }
  
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.tableHail.cpus, mem = projectConfig.resources.tableHail.mem, maxRunTime = projectConfig.resources.tableHail.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyHailFilterSchemaVariants}
            --driver-memory ${(projectConfig.resources.tableHail.mem*0.9*1000).toInt}m
            --executor-memory ${(projectConfig.resources.tableHail.mem*0.9*1000).toInt}m
            --tmp-dir ${projectStores.tmpDir}
            --reference-genome ${projectConfig.referenceGenome}
            --full-stats-in ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get}
            ${cohortStatsInString}
            ${userAnnotationsInString}
            --filters ${schemaStores((configSchema, configCohorts)).filters.local.get}
            --cohort-filters ${schemaStores((configSchema, configCohorts)).cohortFilters.local.get}
            --knockout-filters ${schemaStores((configSchema, configCohorts)).knockoutFilters.local.get}
            --masks ${schemaStores((configSchema, configCohorts)).masks.local.get}
            ${variantsRemoveString}
            --variant-filters-out ${schemaStores((configSchema, configCohorts)).variantFilterTable.local.get}
            --variant-filters-ht-out ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.local.get}
            --log ${schemaStores((configSchema, configCohorts)).variantFilterHailLog.local.get}"""
              .in(cohortStatsIn ++ userAnnotationsIn)
              .out(schemaStores((configSchema, configCohorts)).variantFilterTable.local.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.local.get, schemaStores((configSchema, configCohorts)).variantFilterHailLog.local.get)
              .tag(s"${schemaStores((configSchema, configCohorts)).variantFilterTable.local.get}".split("/").last)
        
        }
    
    }
  
    schemaStores((configSchema, configCohorts)).vcf match {
    
      case Some(_) =>
    
        projectConfig.hailCloud match {
        
          case true =>
    
            googleWith(projectConfig.cloudResources.mtCluster) {
            
              hail"""${utils.python.pyHailGenerateModelVcf} --
                --cloud
                --hail-utils ${projectStores.hailUtils.google.get}
                --mt-in ${arrayStores(array).mt.get.google.get}
                --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.google.get}
                --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.google.get}
                --vcf-out ${schemaStores((configSchema, configCohorts)).vcf.get.data.google.get}
                --log ${schemaStores((configSchema, configCohorts)).vcfHailLog.google.get}"""
                .in(projectStores.hailUtils.google.get, arrayStores(array).mt.get.google.get, schemaStores((configSchema, configCohorts)).cohortMap.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.google.get)
                .out(schemaStores((configSchema, configCohorts)).vcf.get.data.google.get, schemaStores((configSchema, configCohorts)).vcfHailLog.google.get)
                .tag(s"${schemaStores((configSchema, configCohorts)).vcf.get.data.local.get}.google".split("/").last)
      
            }
    
            local {
    
              googleCopy(schemaStores((configSchema, configCohorts)).vcf.get.data.google.get, schemaStores((configSchema, configCohorts)).vcf.get.data.local.get)
              googleCopy(schemaStores((configSchema, configCohorts)).vcfHailLog.google.get, schemaStores((configSchema, configCohorts)).vcfHailLog.local.get)
    
            }
    
          case false =>
    
            drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
            
              cmd"""${utils.binary.binPython} ${utils.python.pyHailGenerateModelVcf}
                --driver-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
                --executor-memory ${(projectConfig.resources.matrixTableHail.mem*0.9*1000).toInt}m
                --mt-in ${arrayStores(array).mt.get.local.get}
                --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
                --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.local.get}
                --vcf-out ${schemaStores((configSchema, configCohorts)).vcf.get.data.local.get}
                --log ${schemaStores((configSchema, configCohorts)).vcfHailLog.local.get}"""
                .in(arrayStores(array).mt.get.local.get, schemaStores((configSchema, configCohorts)).cohortMap.local.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.local.get)
                .out(schemaStores((configSchema, configCohorts)).vcf.get.data.local.get, schemaStores((configSchema, configCohorts)).vcfHailLog.local.get)
                .tag(s"${schemaStores((configSchema, configCohorts)).vcf.get.data.local.get}".split("/").last)
      
            }
    
        }
    
        drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.tabix.cpus, mem = projectConfig.resources.tabix.mem, maxRunTime = projectConfig.resources.tabix.maxRunTime) {
    
          cmd"""${utils.binary.binTabix} -p vcf ${schemaStores((configSchema, configCohorts)).vcf.get.data.local.get}"""
            .in(schemaStores((configSchema, configCohorts)).vcf.get.data.local.get)
            .out(schemaStores((configSchema, configCohorts)).vcf.get.tbi.local.get)
            .tag(s"${schemaStores((configSchema, configCohorts)).vcf.get.tbi.local.get}".split("/").last)
        
        }
    
      case None => ()
    
    }

    //schemaStores((configSchema, configCohorts)).epacts match {
	//
    //  case Some(_) =>
	//
    //    import PrepareEpacts._
	//
    //    PrepareEpacts(configSchema, configCohorts)
	//
    //  case None => ()
	//
    //}

    schemaStores((configSchema, configCohorts)).regenie match {

      case Some(_) =>

        import PrepareRegenie._

        PrepareRegenie(configSchema, configCohorts)

      case None => ()

    }
  
  }

}
