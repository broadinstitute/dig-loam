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

    drmWith(imageName = s"${utils.image.imgR}") {
  
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rSchemaCohortSamplesAvailable}
        --pheno-in ${arrayStores(array).sampleFile}
        --fam-in ${arrayStores(array).filteredPlink.base.local.get}.fam
        --ancestry-in ${arrayStores(array).ancestryMap}
        ${stratStrings.mkString(" ")}
        --iid-col ${array.qcSampleFileId}
        --samples-exclude-qc ${arrayStores(array).qcSamplesExclude}
        --samples-exclude-postqc ${arrayStores(array).postQcSamplesExclude}
        --out-id-map ${schemaStores((configSchema, configCohorts)).sampleMap}
        --out-cohorts-map ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
        --out ${schemaStores((configSchema, configCohorts)).samplesAvailable}
        > ${schemaStores((configSchema, configCohorts)).samplesAvailableLog}"""
        .in(arrayStores(array).filteredPlink.data.local.get :+ arrayStores(array).sampleFile :+ arrayStores(array).ancestryMap :+ arrayStores(array).qcSamplesExclude :+ arrayStores(array).postQcSamplesExclude)
        .out(schemaStores((configSchema, configCohorts)).sampleMap, schemaStores((configSchema, configCohorts)).cohortMap.local.get, schemaStores((configSchema, configCohorts)).samplesAvailable, schemaStores((configSchema, configCohorts)).samplesAvailableLog)
        .tag(s"${schemaStores((configSchema, configCohorts)).samplesAvailable}".split("/").last)
    
    }
  
    //val binaryFilterPhenos = schemaFilterFields.filter(e => e.schema.id == configSchema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
    //  case n if n > 0 => projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == configSchema.id).map(g => g.pheno).contains(e.id))
    //  case _ => Seq[ConfigPheno]()
    //}
    
    projectConfig.hailCloud match {
    
      case true =>
    
        local {
  
          googleCopy(schemaStores((configSchema, configCohorts)).cohortMap.local.get, schemaStores((configSchema, configCohorts)).cohortMap.google.get)
        
        }
        
        googleWith(projectConfig.cloudResources.mtCluster) {
        
          hail"""${utils.python.pyHailSchemaVariantStats} --
            --hail-utils ${projectStores.hailUtils.google.get}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refMt.google.get}
            --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.google.get}
            --variants-stats-out ${schemaStores((configSchema, configCohorts)).variantsStats.base.google.get}
            --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get}
            --cloud
            --log ${schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.google.get}"""
              .in(projectStores.hailUtils.google.get, arrayStores(array).refMt.google.get, schemaStores((configSchema, configCohorts)).cohortMap.google.get)
              .out(schemaStores((configSchema, configCohorts)).variantsStats.base.google.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.google.get)
              .tag(s"${schemaStores((configSchema, configCohorts)).variantsStats.base.local.get}.google".split("/").last)
        
        }
        
        local {
        
          googleCopy(schemaStores((configSchema, configCohorts)).variantsStats.base.google.get, schemaStores((configSchema, configCohorts)).variantsStats.base.local.get)
          googleCopy(schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.google.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.local.get)
        
        }
  
        for {
  
          pheno <- binaryFilterPhenos
  
        } yield {
  
          googleWith(projectConfig.cloudResources.mtCluster) {
  
            hail"""${utils.python.pyHailSchemaVariantCaseCtrlStats} --
              --hail-utils ${projectStores.hailUtils.google.get}
              --reference-genome ${projectConfig.referenceGenome}
              --mt-in ${arrayStores(array).refMt.google.get}
              --pheno-in ${arrayStores(array).phenoFile.google.get}
  	          --pheno-col ${pheno.id}
              --iid-col ${array.phenoFileId}
              --diff-miss-min-expected-cell-count ${projectConfig.diffMissMinExpectedCellCount}
              --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.google.get}
              --variants-stats-out ${schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).base.google.get}
              --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).base.google.get}
              --cloud
              --log ${schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).base.google.get}"""
                .in(projectStores.hailUtils.google.get, arrayStores(array).refMt.google.get, arrayStores(array).phenoFile.google.get, schemaStores((configSchema, configCohorts)).cohortMap.google.get)
                .out(schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).base.google.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).base.google.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).base.google.get)
                .tag(s"${schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).base.local.get}.google".split("/").last)
          
          }
  
          local {
  
            googleCopy(schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).base.google.get, schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).base.local.get)
            googleCopy(schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).base.google.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).base.local.get)
          
          }
  
        }
    
      case false =>
    
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyHailSchemaVariantStats}
            --reference-genome ${projectConfig.referenceGenome}
            --mt-in ${arrayStores(array).refMt.local.get}
            --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
            --variants-stats-out ${schemaStores((configSchema, configCohorts)).variantsStats.base.local.get}
            --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get}
            --log ${schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.local.get}"""
              .in(arrayStores(array).refMt.local.get, schemaStores((configSchema, configCohorts)).cohortMap.local.get)
              .out(schemaStores((configSchema, configCohorts)).variantsStats.base.local.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.base.local.get)
              .tag(s"${schemaStores((configSchema, configCohorts)).variantsStats.base.local.get}".split("/").last)
        
        }
  
        for {
  
          pheno <- binaryFilterPhenos
  
        } yield {
  
          drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
  
            cmd"""${utils.binary.binPython} ${utils.python.pyHailSchemaVariantCaseCtrlStats}
              --reference-genome ${projectConfig.referenceGenome}
              --mt-in ${arrayStores(array).refMt.local.get}
              --pheno-in ${arrayStores(array).phenoFile.local.get}
  	          --pheno-col ${pheno.id}
              --iid-col ${array.phenoFileId}
              --diff-miss-min-expected-cell-count ${projectConfig.diffMissMinExpectedCellCount}
              --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
              --variants-stats-out ${schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).base.local.get}
              --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).base.local.get}
              --log ${schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).base.local.get}"""
                .in(arrayStores(array).refMt.local.get, arrayStores(array).phenoFile.local.get, schemaStores((configSchema, configCohorts)).cohortMap.local.get)
                .out(schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).base.local.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).base.local.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).base.local.get)
                .tag(s"${schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).base.local.get}".split("/").last)
          
          }
  
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
                  --mt-in ${arrayStores(array).refMt.google.get}
                  --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.google.get}
                  --cohort ${cohort.id}
                  --variants-stats-out ${schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).google.get}
                  --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts(cohort).google.get}
                  --cloud
                  --log ${schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).google.get}"""
                    .in(projectStores.hailUtils.google.get, arrayStores(array).refMt.google.get, schemaStores((configSchema, configCohorts)).cohortMap.google.get)
                    .out(schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).google.get)
                    .tag(s"${schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).local.get}.google".split("/").last)
              
              }
              
              local {
              
                googleCopy(schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).local.get)
                googleCopy(schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).local.get)
              
              }
  
              for {
  
                pheno <- binaryFilterPhenos
              
              } yield {
              
                googleWith(projectConfig.cloudResources.mtCluster) {
              
                  hail"""${utils.python.pyHailSchemaVariantCaseCtrlStats} --
                    --hail-utils ${projectStores.hailUtils.google.get}
                    --reference-genome ${projectConfig.referenceGenome}
                    --mt-in ${arrayStores(array).refMt.google.get}
                    --pheno-in ${arrayStores(array).phenoFile.google.get}
  	                --pheno-col ${pheno.id}
                    --iid-col ${array.phenoFileId}
                    --diff-miss-min-expected-cell-count ${projectConfig.diffMissMinExpectedCellCount}
                    --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.google.get}
                    --variants-stats-out ${schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).cohorts(cohort).google.get}
                    --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).cohorts(cohort).google.get}
                    --cloud
                    --log ${schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).cohorts(cohort).google.get}"""
                      .in(projectStores.hailUtils.google.get, arrayStores(array).refMt.google.get, arrayStores(array).phenoFile.google.get, schemaStores((configSchema, configCohorts)).cohortMap.google.get)
                      .out(schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).cohorts(cohort).google.get)
                      .tag(s"${schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).cohorts(cohort).local.get}.google".split("/").last)
                
                }
              
                local {
              
                  googleCopy(schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).cohorts(cohort).local.get)
                  googleCopy(schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).cohorts(cohort).google.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).cohorts(cohort).local.get)
                
                }
              
              }
        
            case false =>
            
              drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
              
                  cmd"""${utils.binary.binPython} ${utils.python.pyHailSchemaVariantStats}
                    --reference-genome ${projectConfig.referenceGenome}
                    --mt-in ${arrayStores(array).refMt.local.get}
                    --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
                    --cohort ${cohort.id}
                    --variants-stats-out ${schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).local.get}
                    --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts(cohort).local.get}
                    --log ${schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).local.get}"""
                      .in(arrayStores(array).refMt.local.get, schemaStores((configSchema, configCohorts)).cohortMap.local.get)
                      .out(schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).local.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.cohorts(cohort).local.get, schemaStores((configSchema, configCohorts)).variantsStatsHailLog.cohorts(cohort).local.get)
                      .tag(s"${schemaStores((configSchema, configCohorts)).variantsStats.cohorts(cohort).local.get}".split("/").last)
                
              }
  
              for {
  
                pheno <- binaryFilterPhenos
              
              } yield {
              
                drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
              
                  cmd"""${utils.binary.binPython} ${utils.python.pyHailSchemaVariantCaseCtrlStats}
                    --reference-genome ${projectConfig.referenceGenome}
                    --mt-in ${arrayStores(array).refMt.local.get}
                    --pheno-in ${arrayStores(array).phenoFile.local.get}
  	                --pheno-col ${pheno.id}
                    --iid-col ${array.phenoFileId}
                    --diff-miss-min-expected-cell-count ${projectConfig.diffMissMinExpectedCellCount}
                    --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
                    --variants-stats-out ${schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).cohorts(cohort).local.get}
                    --variants-stats-ht-out ${schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).cohorts(cohort).local.get}
                    --log ${schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).cohorts(cohort).local.get}"""
                      .in(arrayStores(array).refMt.local.get, arrayStores(array).phenoFile.local.get, schemaStores((configSchema, configCohorts)).cohortMap.local.get)
                      .out(schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).cohorts(cohort).local.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).cohorts(cohort).local.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHailLog(pheno).cohorts(cohort).local.get)
                      .tag(s"${schemaStores((configSchema, configCohorts)).phenoVariantsStats(pheno).cohorts(cohort).local.get}".split("/").last)
                
                }
              
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
  
    val baseFilters = filters.filter(e => ! e.contains("variant_qc.diff_miss"))
    val baseCohortFilters = cohortFilters.filter(e => ! e.contains("variant_qc.diff_miss"))
    val baseKnockoutFilters = knockoutFilters.filter(e => ! e.contains("variant_qc.diff_miss"))
    val baseMasks = masks.filter(e => ! e.contains("variant_qc.diff_miss"))
  
    val fString = baseFilters.size match {
    
      case n if n > 0 => s"""echo "${baseFilters.mkString("\n")}" > """
      case _ => "touch "
    
    }
    
    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""${fString} ${schemaStores((configSchema, configCohorts)).filters.base.local.get}"""
        .out(schemaStores((configSchema, configCohorts)).filters.base.local.get)
        .tag(s"${schemaStores((configSchema, configCohorts)).filters.base.local.get}".split("/").last)
    
    }
    
    val cfString = baseCohortFilters.size match {
    
      case n if n > 0 => s"""echo "${baseCohortFilters.mkString("\n")}" > """
      case _ => "touch "
    
    }
    
    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""${cfString} ${schemaStores((configSchema, configCohorts)).cohortFilters.base.local.get}"""
        .out(schemaStores((configSchema, configCohorts)).cohortFilters.base.local.get)
        .tag(s"${schemaStores((configSchema, configCohorts)).cohortFilters.base.local.get}".split("/").last)
    
    }
    
    val kfString = baseKnockoutFilters.size match {
    
      case n if n > 0 => s"""echo "${baseKnockoutFilters.mkString("\n")}" > """
      case _ => "touch "
    
    }
    
    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""${kfString} ${schemaStores((configSchema, configCohorts)).knockoutFilters.base.local.get}"""
        .out(schemaStores((configSchema, configCohorts)).knockoutFilters.base.local.get)
        .tag(s"${schemaStores((configSchema, configCohorts)).knockoutFilters.base.local.get}".split("/").last)
    
    }
  
    val mString = baseMasks.size match {
    
      case n if n > 0 => s"""echo "${baseMasks.mkString("\n")}" > """
      case _ => "touch "
    
    }
    
    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""${mString} ${schemaStores((configSchema, configCohorts)).masks.base.local.get}"""
        .out(schemaStores((configSchema, configCohorts)).masks.base.local.get)
        .tag(s"${schemaStores((configSchema, configCohorts)).masks.base.local.get}".split("/").last)
    
    }

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
              s"""${a.id},${projectStores.annotationStores(a).local.get.toString.split("@")(1)}"""
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
    
          googleCopy(schemaStores((configSchema, configCohorts)).filters.base.local.get, schemaStores((configSchema, configCohorts)).filters.base.google.get)
          googleCopy(schemaStores((configSchema, configCohorts)).masks.base.local.get, schemaStores((configSchema, configCohorts)).masks.base.google.get)
          googleCopy(schemaStores((configSchema, configCohorts)).cohortFilters.base.local.get, schemaStores((configSchema, configCohorts)).cohortFilters.base.google.get)
          googleCopy(schemaStores((configSchema, configCohorts)).knockoutFilters.base.local.get, schemaStores((configSchema, configCohorts)).knockoutFilters.base.google.get)
        
        }
    
        val cohortStatsInString = {
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
    
        var cohortStatsIn = Seq(projectStores.hailUtils.google.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get, arrayStores(array).refAnnotationsHt.google.get, schemaStores((configSchema, configCohorts)).filters.base.google.get, schemaStores((configSchema, configCohorts)).masks.base.google.get, arrayStores(array).variantsExclude.google.get, schemaStores((configSchema, configCohorts)).cohortFilters.base.google.get, schemaStores((configSchema, configCohorts)).knockoutFilters.base.google.get)
        
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
  
        googleWith(projectConfig.cloudResources.variantHtCluster) {
        
          hail"""${utils.python.pyHailFilterSchemaVariants} --
            --cloud
            --hail-utils ${projectStores.hailUtils.google.get}
            --reference-genome ${projectConfig.referenceGenome}
            --full-stats-in ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get}
            ${cohortStatsInString}
            --annotation ${arrayStores(array).refAnnotationsHt.google.get}
            ${userAnnotationsInString}
            --filters ${schemaStores((configSchema, configCohorts)).filters.base.google.get}
            --cohort-filters ${schemaStores((configSchema, configCohorts)).cohortFilters.base.google.get}
            --knockout-filters ${schemaStores((configSchema, configCohorts)).knockoutFilters.base.google.get}
            --masks ${schemaStores((configSchema, configCohorts)).masks.base.google.get}
            --variants-remove ${arrayStores(array).variantsExclude.google.get}
            --variant-filters-out ${schemaStores((configSchema, configCohorts)).variantFilterTable.base.google.get}
            --variant-filters-ht-out ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.google.get}
            --log ${schemaStores((configSchema, configCohorts)).variantFilterHailLog.base.google.get}"""
              .in(cohortStatsIn ++ userAnnotationsIn)
              .out(schemaStores((configSchema, configCohorts)).variantFilterTable.base.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailLog.base.google.get)
              .tag(s"${schemaStores((configSchema, configCohorts)).variantFilterTable.base.local.get}.google".split("/").last)
        
        }
        
        local {
        
          googleCopy(schemaStores((configSchema, configCohorts)).variantFilterTable.base.google.get, schemaStores((configSchema, configCohorts)).variantFilterTable.base.local.get)
          googleCopy(schemaStores((configSchema, configCohorts)).variantFilterHailLog.base.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailLog.base.local.get)
        
        }
    
      case false =>
    
        val cohortStatsInString = {
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
        
        var cohortStatsIn = Seq(schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get, arrayStores(array).refAnnotationsHt.local.get, schemaStores((configSchema, configCohorts)).filters.base.local.get, schemaStores((configSchema, configCohorts)).masks.base.local.get, arrayStores(array).variantsExclude.local.get, schemaStores((configSchema, configCohorts)).cohortFilters.base.local.get, schemaStores((configSchema, configCohorts)).knockoutFilters.base.local.get)
        
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
  
        drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.tableHail.cpus, mem = projectConfig.resources.tableHail.mem, maxRunTime = projectConfig.resources.tableHail.maxRunTime) {
        
          cmd"""${utils.binary.binPython} ${utils.python.pyHailFilterSchemaVariants}
            --tmpdir ${dirTree.analysisSchemaMap(configSchema).local.get}
            --reference-genome ${projectConfig.referenceGenome}
            --full-stats-in ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get}
            ${cohortStatsInString}
            ${userAnnotationsInString}
            --annotation ${arrayStores(array).refAnnotationsHt.local.get}
            --filters ${schemaStores((configSchema, configCohorts)).filters.base.local.get}
            --cohort-filters ${schemaStores((configSchema, configCohorts)).cohortFilters.base.local.get}
            --knockout-filters ${schemaStores((configSchema, configCohorts)).knockoutFilters.base.local.get}
            --masks ${schemaStores((configSchema, configCohorts)).masks.base.local.get}
            --variants-remove ${arrayStores(array).variantsExclude.local.get}
            --variant-filters-out ${schemaStores((configSchema, configCohorts)).variantFilterTable.base.local.get}
            --variant-filters-ht-out ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.local.get}
            --log ${schemaStores((configSchema, configCohorts)).variantFilterHailLog.base.local.get}"""
              .in(cohortStatsIn ++ userAnnotationsIn)
              .out(schemaStores((configSchema, configCohorts)).variantFilterTable.base.local.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.local.get, schemaStores((configSchema, configCohorts)).variantFilterHailLog.base.local.get)
              .tag(s"${schemaStores((configSchema, configCohorts)).variantFilterTable.base.local.get}".split("/").last)
        
        }
    
    }
  
    val phenoFilters = filters.filter(e => e.contains("variant_qc.diff_miss"))
    val phenoCohortFilters = cohortFilters.filter(e => e.contains("variant_qc.diff_miss"))
    val phenoKnockoutFilters = knockoutFilters.filter(e => e.contains("variant_qc.diff_miss"))
    val phenoMasks = masks.filter(e => e.contains("variant_qc.diff_miss"))
  
    for {
    
      pheno <- binaryFilterPhenos
    
    } yield {
  
      val fString = phenoFilters.size match {
      
        case n if n > 0 => s"""echo "${phenoFilters.mkString("\n")}" > """
        case _ => "touch "
      
      }
      
      drmWith(imageName = s"${utils.image.imgTools}") {
      
        cmd"""${fString} ${schemaStores((configSchema, configCohorts)).filters.phenos(pheno).local.get}"""
          .out(schemaStores((configSchema, configCohorts)).filters.phenos(pheno).local.get)
          .tag(s"${schemaStores((configSchema, configCohorts)).filters.phenos(pheno).local.get}".split("/").last)
      
      }
      
      val cfString = phenoCohortFilters.size match {
      
        case n if n > 0 => s"""echo "${phenoCohortFilters.mkString("\n")}" > """
        case _ => "touch "
      
      }
      
      drmWith(imageName = s"${utils.image.imgTools}") {
      
        cmd"""${cfString} ${schemaStores((configSchema, configCohorts)).cohortFilters.phenos(pheno).local.get}"""
          .out(schemaStores((configSchema, configCohorts)).cohortFilters.phenos(pheno).local.get)
          .tag(s"${schemaStores((configSchema, configCohorts)).cohortFilters.phenos(pheno).local.get}".split("/").last)
      
      }
      
      val kfString = phenoKnockoutFilters.size match {
      
        case n if n > 0 => s"""echo "${phenoKnockoutFilters.mkString("\n")}" > """
        case _ => "touch "
      
      }
      
      drmWith(imageName = s"${utils.image.imgTools}") {
      
        cmd"""${kfString} ${schemaStores((configSchema, configCohorts)).knockoutFilters.phenos(pheno).local.get}"""
          .out(schemaStores((configSchema, configCohorts)).knockoutFilters.phenos(pheno).local.get)
          .tag(s"${schemaStores((configSchema, configCohorts)).knockoutFilters.phenos(pheno).local.get}".split("/").last)
      
      }
  
      val mString = phenoMasks.size match {
      
        case n if n > 0 => s"""echo "${phenoMasks.mkString("\n")}" > """
        case _ => "touch "
      
      }
      
      drmWith(imageName = s"${utils.image.imgTools}") {
      
        cmd"""${mString} ${schemaStores((configSchema, configCohorts)).masks.phenos(pheno).local.get}"""
          .out(schemaStores((configSchema, configCohorts)).masks.phenos(pheno).local.get)
          .tag(s"${schemaStores((configSchema, configCohorts)).masks.phenos(pheno).local.get}".split("/").last)
      
      }
  
      projectConfig.hailCloud match {
      
        case true =>
      
          local {
      
            googleCopy(schemaStores((configSchema, configCohorts)).filters.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).filters.phenos(pheno).google.get)
            googleCopy(schemaStores((configSchema, configCohorts)).masks.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).masks.phenos(pheno).google.get)
            googleCopy(schemaStores((configSchema, configCohorts)).cohortFilters.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).cohortFilters.phenos(pheno).google.get)
            googleCopy(schemaStores((configSchema, configCohorts)).knockoutFilters.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).knockoutFilters.phenos(pheno).google.get)
  
          }
      
          val cohortStatsInString = {
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
  
          var cohortStatsIn = Seq(projectStores.hailUtils.google.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).base.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.google.get, arrayStores(array).refAnnotationsHt.google.get, schemaStores((configSchema, configCohorts)).filters.phenos(pheno).google.get, schemaStores((configSchema, configCohorts)).masks.phenos(pheno).google.get, arrayStores(array).variantsExclude.google.get, schemaStores((configSchema, configCohorts)).cohortFilters.phenos(pheno).google.get, schemaStores((configSchema, configCohorts)).knockoutFilters.phenos(pheno).google.get)
          
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
      
          googleWith(projectConfig.cloudResources.variantHtCluster) {
          
            hail"""${utils.python.pyHailFilterSchemaPhenoVariants} --
              --cloud
              --hail-utils ${projectStores.hailUtils.google.get}
              --reference-genome ${projectConfig.referenceGenome}
              --full-stats-in ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get}
              --pheno-stats-in ${schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).base.google.get}
              --schema-filters-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.google.get}
              ${cohortStatsInString}
              ${userAnnotationsInString}
              --annotation ${arrayStores(array).refAnnotationsHt.google.get}
              --filters ${schemaStores((configSchema, configCohorts)).filters.phenos(pheno).google.get}
              --cohort-filters ${schemaStores((configSchema, configCohorts)).cohortFilters.phenos(pheno).google.get}
              --knockout-filters ${schemaStores((configSchema, configCohorts)).knockoutFilters.phenos(pheno).google.get}
              --masks ${schemaStores((configSchema, configCohorts)).masks.phenos(pheno).google.get}
              --variant-filters-out ${schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).google.get}
              --variant-filters-ht-out ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).google.get}
              --log ${schemaStores((configSchema, configCohorts)).variantFilterHailLog.phenos(pheno).google.get}"""
                .in(cohortStatsIn ++ userAnnotationsIn)
                .out(schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).google.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).google.get, schemaStores((configSchema, configCohorts)).variantFilterHailLog.phenos(pheno).google.get)
                .tag(s"${schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).local.get}.google".split("/").last)
          
          }
          
          local {
          
            googleCopy(schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).google.get, schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).local.get)
            googleCopy(schemaStores((configSchema, configCohorts)).variantFilterHailLog.phenos(pheno).google.get, schemaStores((configSchema, configCohorts)).variantFilterHailLog.phenos(pheno).local.get)
          
          }
      
        case false =>
      
          val cohortStatsInString = {
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
          
          var cohortStatsIn = Seq(schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get, schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).base.local.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.local.get, arrayStores(array).refAnnotationsHt.local.get, schemaStores((configSchema, configCohorts)).filters.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).masks.phenos(pheno).local.get, arrayStores(array).variantsExclude.local.get, schemaStores((configSchema, configCohorts)).cohortFilters.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).knockoutFilters.phenos(pheno).local.get)
          
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
          
          drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.tableHail.cpus, mem = projectConfig.resources.tableHail.mem, maxRunTime = projectConfig.resources.tableHail.maxRunTime) {
          
            cmd"""${utils.binary.binPython} ${utils.python.pyHailFilterSchemaPhenoVariants}
              --tmpdir ${dirTree.analysisSchemaMap(configSchema).local.get}
              --reference-genome ${projectConfig.referenceGenome}
              --full-stats-in ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get}
              --pheno-stats-in ${schemaStores((configSchema, configCohorts)).phenoVariantsStatsHt(pheno).base.local.get}
              --schema-filters-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.local.get}
              ${cohortStatsInString}
              ${userAnnotationsInString}
              --annotation ${arrayStores(array).refAnnotationsHt.local.get}
              --filters ${schemaStores((configSchema, configCohorts)).filters.phenos(pheno).local.get}
              --cohort-filters ${schemaStores((configSchema, configCohorts)).cohortFilters.phenos(pheno).local.get}
              --knockout-filters ${schemaStores((configSchema, configCohorts)).knockoutFilters.phenos(pheno).local.get}
              --masks ${schemaStores((configSchema, configCohorts)).masks.phenos(pheno).local.get}
              --variant-filters-out ${schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).local.get}
              --variant-filters-ht-out ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).local.get}
              --log ${schemaStores((configSchema, configCohorts)).variantFilterHailLog.phenos(pheno).local.get}"""
                .in(cohortStatsIn ++ userAnnotationsIn)
                .out(schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.phenos(pheno).local.get, schemaStores((configSchema, configCohorts)).variantFilterHailLog.phenos(pheno).local.get)
                .tag(s"${schemaStores((configSchema, configCohorts)).variantFilterTable.phenos(pheno).local.get}".split("/").last)
          
          }
      
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
                --mt-in ${arrayStores(array).refMt.google.get}
                --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.google.get}
                --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.google.get}
                --vcf-out ${schemaStores((configSchema, configCohorts)).vcf.get.data.google.get}
                --log ${schemaStores((configSchema, configCohorts)).vcfHailLog.google.get}"""
                .in(projectStores.hailUtils.google.get, arrayStores(array).refMt.google.get, schemaStores((configSchema, configCohorts)).cohortMap.google.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.google.get)
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
                --mt-in ${arrayStores(array).refMt.local.get}
                --cohorts-map-in ${schemaStores((configSchema, configCohorts)).cohortMap.local.get}
                --filter-table-in ${schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.local.get}
                --vcf-out ${schemaStores((configSchema, configCohorts)).vcf.get.data.local.get}
                --log ${schemaStores((configSchema, configCohorts)).vcfHailLog.local.get}"""
                .in(arrayStores(array).refMt.local.get, schemaStores((configSchema, configCohorts)).cohortMap.local.get, schemaStores((configSchema, configCohorts)).variantFilterHailTable.base.local.get)
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

    schemaStores((configSchema, configCohorts)).epacts match {

      case Some(_) =>

        import PrepareEpacts._

        PrepareEpacts(configSchema, configCohorts)

      case None => ()

    }

    schemaStores((configSchema, configCohorts)).regenie match {

      case Some(_) =>

        import PrepareRegenie._

        PrepareRegenie(configSchema, configCohorts)

      case None => ()

    }
  
  }

}
