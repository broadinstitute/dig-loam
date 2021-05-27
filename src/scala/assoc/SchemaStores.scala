object SchemaStores extends loamstream.LoamFile {

  import ProjectConfig._
  import StoreHelpers._
  import Stores._
  import Fxns._
  import DirTree._
  import Collections._
  
  final case class SchemaBaseCohortStore(
    base: MultiStore,
    cohorts: Map[ConfigCohort, MultiStore])
  
  final case class SchemaBasePhenoStore(
    base: MultiStore,
    phenos: Map[ConfigPheno, MultiStore])
  
  final case class SchemaBaseMaskStore(
    base: MultiStore,
    masks: Map[MaskFilter, MultiStore])
  
  final case class SchemaBasePhenoMaskStore(
    base: SchemaBaseMaskStore,
    phenos: Map[ConfigPheno, SchemaBaseMaskStore])

  final case class Schema(
    sampleMap: Store,
    cohortMap: MultiStore,
    samplesAvailable: Store,
    samplesAvailableLog: Store,
    filters: SchemaBasePhenoStore,
    cohortFilters: SchemaBasePhenoStore,
    knockoutFilters: SchemaBasePhenoStore,
    masks: SchemaBasePhenoStore,
    variantsStats: SchemaBaseCohortStore,
    variantsStatsHt: SchemaBaseCohortStore,
    variantsStatsHailLog: SchemaBaseCohortStore,
    phenoVariantsStats: Map[ConfigPheno, SchemaBaseCohortStore],
    phenoVariantsStatsHt: Map[ConfigPheno, SchemaBaseCohortStore],
    phenoVariantsStatsHailLog: Map[ConfigPheno, SchemaBaseCohortStore],
    variantFilterTable: SchemaBasePhenoStore,
    variantFilterHailTable: SchemaBasePhenoStore,
    variantFilterHailLog: SchemaBasePhenoStore,
    groupFile: SchemaBasePhenoMaskStore,
    groupFileHailLog: SchemaBasePhenoStore,
    regenieSetlist: SchemaBasePhenoStore,
    regenieAnnotations: SchemaBasePhenoStore,
    regenieMasks: SchemaBasePhenoStore,
    regenieHailLog: SchemaBasePhenoStore,
    vcf: Option[MultiPathVcf],
    vcfHailLog: MultiStore
  )
  
  val schemaStores = {
    for {
      x <- schemaCohorts
    } yield {
      (x.schema, x.cohorts)
    }
  }.distinct.map { sm =>
  
    val schema = sm._1
    val cohorts = sm._2
  
    val array = projectConfig.Arrays.filter(e => e.id == cohorts.head.array).head
  
    val nullString = s"${projectConfig.projectId}.${schema.id}"
  
    val baseString = schema.design match {
      case "full" => s"${nullString}"
      case "strat" => s"${nullString}.${cohorts.head.id}"
    }
  
    val local_dir = dirTree.analysisSchemaMap(schema).local.get
  
    val cloud_dir = projectConfig.hailCloud match {
        case true => Some(dirTree.analysisSchemaMap(schema).google.get)
        case false => None
    }
  
    val groupFile = SchemaBasePhenoMaskStore(
      base = SchemaBaseMaskStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.groupfile.tsv")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.groupfile.tsv")); case false => None }
        ),
        masks = schema.masks match {
          case Some(_) =>
            schema.masks.get.size match {
              case n if n > 0 =>
                schema.masks.get.map { mask =>
                  mask -> MultiStore(
                    local = Some(store(local_dir / s"${baseString}.variant.groupfile.${mask.id}.tsv")),
                    google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.groupfile.${mask.id}.tsv")); case false => None }
                  )
                }.toMap
              case _ => Map[MaskFilter, MultiStore]()
            }
          case None => Map[MaskFilter, MultiStore]()
        }
      ),
      phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
        case n if n > 0 =>
          projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
            pheno -> SchemaBaseMaskStore(
              base = MultiStore(
                local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.groupfile.tsv")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.groupfile.tsv")); case false => None }
              ),
              masks = schema.masks match {
                case Some(_) =>
                  schema.masks.get.size match {
                    case n if n > 0 =>
                      schema.masks.get.map { mask =>
                        mask -> MultiStore(
                          local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.groupfile.${mask.id}.tsv")),
                          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.groupfile.${mask.id}.tsv")); case false => None }
                        )
                      }.toMap
                    case _ => Map[MaskFilter, MultiStore]()
                  }
                case None => Map[MaskFilter, MultiStore]()
              }
            )
          }.toMap
        case _ =>  Map[ConfigPheno, SchemaBaseMaskStore]()
      }
    )
  
    try {
      val gFile = checkPath(s"""${groupFile.base.base.local.get.toString.split("@")(1)}""")
      val l = fileToList(gFile).map(e => e.split("\t")(0))
      for {
        group <- l
      } yield {
          dirTree.analysisModelGroupsMap(group) = appendSubDir(dirTree.analysisModelGroups, group)
      }
    }
    catch {
      case x: CfgException =>
        println(s"""skipping split assoc test by group due to missing group file: ${groupFile.base.base.local.get.toString.split("@")(1)}""")
    }
                  
    sm -> Schema(
      sampleMap = store(local_dir / s"${baseString}.sample.map.tsv"),
      cohortMap = MultiStore(
        local = Some(store(local_dir / s"${baseString}.cohort.map.tsv")),
        google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.cohort.map.tsv")); case false => None }
      ),
      samplesAvailable = store(local_dir / s"${baseString}.samples.available.txt"),
      samplesAvailableLog = store(local_dir / s"${baseString}.samples.available.log"),
      filters = SchemaBasePhenoStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.filters.txt")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.filters.txt")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                  local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.filters.txt")),
                  google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.filters.txt")); case false => None }
                )
              }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      cohortFilters = SchemaBasePhenoStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.cohortfilters.txt")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.cohortfilters.txt")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                  local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.cohortfilters.txt")),
                  google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.cohortfilters.txt")); case false => None }
                )
              }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      knockoutFilters = SchemaBasePhenoStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.knockoutfilters.txt")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.knockoutfilters.txt")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                  local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.knockoutfilters.txt")),
                  google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.knockoutfilters.txt")); case false => None }
                )
              }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      masks = SchemaBasePhenoStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.masks.txt")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.masks.txt")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                  local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.masks.txt")),
                  google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.masks.txt")); case false => None }
                )
              }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      variantsStats = SchemaBaseCohortStore(
        base = schema.design match {
          case "full" => 
            MultiStore(
              local = Some(store(local_dir / s"${baseString}.variant.stats.tsv.bgz")),
              google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.stats.tsv.bgz")); case false => None }
            )
          case "strat" =>
            MultiStore(
              local = Some(store(local_dir / s"${nullString}.${cohorts.head.id}.variant.stats.tsv.bgz")),
              google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohorts.head.id}.variant.stats.tsv.bgz")); case false => None }
            )
        },
        cohorts = (schema.design, schema.filterCohorts.size) match {
          case ("full", n) if n > 0 =>
            cohorts.filter(e => schema.filterCohorts.contains(e.id)).map { cohort =>
              cohort -> MultiStore(
                local = Some(store(local_dir / s"${nullString}.${cohort.id}.variant.stats.tsv.bgz")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohort.id}.variant.stats.tsv.bgz")); case false => None }
              )
            }.toMap
          case _ => Map[ConfigCohort, MultiStore]()
        }
      ),
      variantsStatsHt = SchemaBaseCohortStore(
        base = schema.design match {
          case "full" => 
            MultiStore(
              local = Some(store(local_dir / s"${baseString}.variant.stats.ht")),
              google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.stats.ht")); case false => None }
            )
          case "strat" => 
            MultiStore(
              local = Some(store(local_dir / s"${nullString}.${cohorts.head.id}.variant.stats.ht")),
              google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohorts.head.id}.variant.stats.ht")); case false => None }
            )
        },
        cohorts = (schema.design, schema.filterCohorts.size) match {
          case ("full", n) if n > 0 =>
            cohorts.filter(e => schema.filterCohorts.contains(e.id)).map { cohort =>
              cohort -> MultiStore(
                local = Some(store(local_dir / s"${nullString}.${cohort.id}.variant.stats.ht")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohort.id}.variant.stats.ht")); case false => None }
              )
            }.toMap
          case _ => Map[ConfigCohort, MultiStore]()
        }
      ),
      variantsStatsHailLog = SchemaBaseCohortStore(
        base = schema.design match {
          case "full" => 
            MultiStore(
              local = Some(store(local_dir / s"${baseString}.variant.stats.hail.log")),
              google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.stats.hail.log")); case false => None }
            )
          case "strat" => 
            MultiStore(
              local = Some(store(local_dir / s"${nullString}.${cohorts.head.id}.variant.stats.hail.log")),
              google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohorts.head.id}.variant.stats.hail.log")); case false => None }
            )
        },
        cohorts = (schema.design, schema.filterCohorts.size) match {
          case ("full", n) if n > 0 =>
            cohorts.filter(e => schema.filterCohorts.contains(e.id)).map { cohort =>
              cohort -> MultiStore(
                local = Some(store(local_dir / s"${nullString}.${cohort.id}.variant.stats.hail.log")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohort.id}.variant.stats.hail.log")); case false => None }
              )
            }.toMap
          case _ => Map[ConfigCohort, MultiStore]()
        }
      ),
      phenoVariantsStats = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
        case n if n > 0 =>
          projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
            pheno -> SchemaBaseCohortStore(
              base = schema.design match {
                case "full" => 
                  MultiStore(
                    local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.stats.tsv.bgz")),
                    google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.stats.tsv.bgz")); case false => None }
                  )
                case "strat" => 
                  MultiStore(
                    local = Some(store(local_dir / s"${nullString}.${cohorts.head.id}.${pheno.id}.variant.stats.tsv.bgz")),
                    google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohorts.head.id}.${pheno.id}.variant.stats.tsv.bgz")); case false => None }
                  )
              },
              cohorts = (schema.design, schema.filterCohorts.size) match {
                case ("full", n) if n > 0 =>
                  cohorts.filter(e => schema.filterCohorts.contains(e.id)).map { cohort =>
                    cohort -> MultiStore(
                      local = Some(store(local_dir / s"${nullString}.${cohort.id}.${pheno.id}.variant.stats.tsv.bgz")),
                      google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohort.id}.${pheno.id}.variant.stats.tsv.bgz")); case false => None }
                    )
                  }.toMap
                case _ => Map[ConfigCohort, MultiStore]()
              }
            )
          }.toMap
        case _ => Map[ConfigPheno, SchemaBaseCohortStore]()
      },
      phenoVariantsStatsHt = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
        case n if n > 0 =>
          projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
            pheno -> SchemaBaseCohortStore(
              base = schema.design match {
                case "full" => 
                  MultiStore(
                    local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.stats.ht")),
                    google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.stats.ht")); case false => None }
                  )
                case "strat" => 
                  MultiStore(
                    local = Some(store(local_dir / s"${nullString}.${cohorts.head.id}.${pheno.id}.variant.stats.ht")),
                    google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohorts.head.id}.${pheno.id}.variant.stats.ht")); case false => None }
                  )
              },
              cohorts = (schema.design, schema.filterCohorts.size) match {
                case ("full", n) if n > 0 =>
                  cohorts.filter(e => schema.filterCohorts.contains(e.id)).map { cohort =>
                    cohort -> MultiStore(
                      local = Some(store(local_dir / s"${nullString}.${cohort.id}.${pheno.id}.variant.stats.ht")),
                      google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohort.id}.${pheno.id}.variant.stats.ht")); case false => None }
                    )
                  }.toMap
                case _ => Map[ConfigCohort, MultiStore]()
              }
            )
          }.toMap
        case _ => Map[ConfigPheno, SchemaBaseCohortStore]()
      },
      phenoVariantsStatsHailLog = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
        case n if n > 0 =>
          projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
            pheno -> SchemaBaseCohortStore(
              base = schema.design match {
                case "full" => 
                  MultiStore(
                    local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.stats.hail.log")),
                    google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.stats.hail.log")); case false => None }
                  )
                case "strat" => 
                  MultiStore(
                    local = Some(store(local_dir / s"${nullString}.${cohorts.head.id}.${pheno.id}.variant.stats.hail.log")),
                    google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohorts.head.id}.${pheno.id}.variant.stats.hail.log")); case false => None }
                  )
              },
              cohorts = (schema.design, schema.filterCohorts.size) match {
                case ("full", n) if n > 0 =>
                  cohorts.filter(e => schema.filterCohorts.contains(e.id)).map { cohort =>
                    cohort -> MultiStore(
                      local = Some(store(local_dir / s"${nullString}.${cohort.id}.${pheno.id}.variant.stats.hail.log")),
                      google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${nullString}.${cohort.id}.${pheno.id}.variant.stats.hail.log")); case false => None }
                    )
                  }.toMap
                case _ => Map[ConfigCohort, MultiStore]()
              }
            )
          }.toMap
        case _ => Map[ConfigPheno, SchemaBaseCohortStore]()
      },
      variantFilterTable = SchemaBasePhenoStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.filters.tsv.bgz")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.filters.tsv.bgz")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                  local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.filters.tsv.bgz")),
                  google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.filters.tsv.bgz")); case false => None }
                )
              }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      variantFilterHailTable = SchemaBasePhenoStore(
        base = MultiStore(
          local = projectConfig.hailCloud match { case false => Some(store(local_dir / s"${baseString}.variant.filters.ht")); case true => None },
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.filters.ht")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                  local = projectConfig.hailCloud match { case false => Some(store(local_dir / s"${baseString}.${pheno.id}.variant.filters.ht")); case true => None },
                  google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.filters.ht")); case false => None }
                )
              }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      variantFilterHailLog = SchemaBasePhenoStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.filters.hail.log")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.filters.hail.log")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.filters.hail.log")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.filters.hail.log")); case false => None }
              )
            }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      groupFile = groupFile,
      groupFileHailLog = SchemaBasePhenoStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.groupfile.hail.log")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.groupfile.hail.log")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.groupfile.hail.log")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.groupfile.hail.log")); case false => None }
              )
            }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      regenieSetlist = SchemaBasePhenoStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.regenie.setlist.tsv")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.regenie.setlist.tsv")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.regenie.setlist.tsv")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.regenie.setlist.tsv")); case false => None }
              )
            }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      regenieAnnotations = SchemaBasePhenoStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.regenie.annotations.tsv")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.regenie.annotations.tsv")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.regenie.annotations.tsv")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.regenie.annotations.tsv")); case false => None }
              )
            }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      regenieMasks = SchemaBasePhenoStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.regenie.masks.tsv")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.regenie.masks.tsv")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.regenie.masks.tsv")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.regenie.masks.tsv")); case false => None }
              )
            }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      regenieHailLog = SchemaBasePhenoStore(
        base = MultiStore(
          local = Some(store(local_dir / s"${baseString}.variant.regenie.hail.log")),
          google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.variant.regenie.hail.log")); case false => None }
        ),
        phenos = schemaFilterFields.filter(e => e.schema.id == schema.id).head.fields.filter(e => e.startsWith("variant_qc.diff_miss")).size match {
          case n if n > 0 =>
            projectConfig.Phenos.filter(e => e.binary && projectConfig.Models.filter(f => f.schema == schema.id).map(g => g.pheno).contains(e.id)).map { pheno =>
              pheno -> MultiStore(
                local = Some(store(local_dir / s"${baseString}.${pheno.id}.variant.regenie.hail.log")),
                google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.${pheno.id}.variant.regenie.hail.log")); case false => None }
              )
            }.toMap
          case _ => Map[ConfigPheno, MultiStore]()
        }
      ),
      vcf = nonHailTests.intersect(projectConfig.Models.filter(e => e.schema == schema.id).map(e => e.tests).flatten).size match {
        case n if n > 0 =>
          (schema.knockoutFilters, array.exportCleanVcf) match {
            case (Some(_), _) | (_, false) =>
              Some(MultiPathVcf(
                base = MultiPath(
                  local = Some(local_dir / baseString),
                  google = projectConfig.hailCloud match { case true => Some(cloud_dir.get / baseString); case false => None }
                ),
                data = MultiStore(
                  local = Some(store(local_dir / s"${baseString}.vcf.bgz")),
                  google = projectConfig.hailCloud match { case true => Some(store(cloud_dir.get / s"${baseString}.vcf.bgz")); case false => None }
                ),
                tbi = MultiStore(local = Some(store(local_dir / s"${baseString}.vcf.bgz.tbi")), google = None)
              ))
            case _ => None
          }
        case _ => None
      },
      vcfHailLog = MultiStore(
        local = nonHailTests.intersect(projectConfig.Models.filter(e => e.schema == schema.id).map(e => e.tests).flatten).size match {
          case n if n > 0 =>
            (schema.knockoutFilters, array.exportCleanVcf, projectConfig.hailCloud) match {
              case (Some(_), _, false) | (_, false, false) => Some(store(local_dir / s"${baseString}.vcf.hail.log"))
              case _ => None
            }
          case _ => None
        },
        google = nonHailTests.intersect(projectConfig.Models.filter(e => e.schema == schema.id).map(e => e.tests).flatten).size match {
          case n if n > 0 =>
            (schema.knockoutFilters, array.exportCleanVcf, projectConfig.hailCloud) match {
              case (Some(_), _, true) | (_, false, true) => Some(store(cloud_dir.get / s"${baseString}.vcf.hail.log"))
              case _ => None
            }
          case _ => None
        }
      )
    )
  }.toMap
  
  //  //val groupResultsSplit = model.design match {
  //  //  case "full" =>
  //  //    model.groupFile match {
  //  //      case Some(_) =>
  //  //        val groups = fileToList(model.groupFile).map(e => e.split("\t")(0))
  //  //        groups.map { group =>
  //  //          group -> store(dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).base_tests(test).jobs(group).local.get / s"${group}.${model.test}.results.tsv")
  //  //        }.toMap
  //  //      case _ => Map[String, Store]()
  //  //    }
  //  //  case "strat" => Map[String, Store]()
  //  //}
  //  //
  //  //val groupFilesSplit = model.design match {
  //  //  case "full" =>
  //  //    model.groupFile match {
  //  //      case Some(_) =>
  //  //        val groups = fileToList(model.groupFile).map(e => e.split("\t")(0))
  //  //        groups.map { group =>
  //  //          group -> store(dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).base_tests.get.jobs(group).local.get / s"${group}.${model.test}.groupfile.tsv")
  //  //        }.toMap
  //  //      case _ => Map[String, Store]()
  //  //    }
  //  //  case "strat" => Map[String, Store]()
  //  //}
  //
  //  //val maskedGroupResultsSplit = model.design match {
  //  //  case "full" =>
  //  //    model.masks match {
  //  //      case Some(_) =>
  //  //        model.masks.get.map { mask =>
  //  //          val x = mask.groupFile match {
  //  //            case Some(_) =>
  //  //              val groups = fileToList(mask.groupFile).map(e => e.split("\t")(0))
  //  //              groups.map { group =>
  //  //                group -> 
  //  //                  println(s"${group}")
  //  //                  //store(dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).base_tests.get.jobs(group).local.get / s"${group}.${model.test}.${mask.id}.results.tsv")
  //  //              }.toMap
  //  //            case None => Map[MaskFilter, Map[String, Store]]()
  //  //          }
  //  //          mask -> x
  //  //        }
  //  //      case None => Map[MaskFilter, Map[String, Store]]()
  //  //    }
  //  //  case "strat" => Map[MaskFilter, Map[String, Store]]()
  //  //}
  //
  //  //val maskedGroupFilesSplit = model.design match {
  //  //  case "full" =>
  //  //    (model.groupFile, model.masks) match {
  //  //      case (Some(_), Some(_)) =>
  //  //        val groups = fileToList(model.groupFile).map(e => e.split("\t")(0))
  //  //        groups.map { group =>
  //  //          group -> model.masks.get.map { mask =>
  //  //            mask -> store(dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).base_tests.get.jobs(group).local.get / s"${group}.${model.test}.${mask.id}.groupfile.tsv")
  //  //          }.toMap
  //  //        }.toMap
  //  //      case _ => Map[String, Map[MaskFilter, Store]]()
  //  //    }
  //  //  case "strat" => Map[String, Map[MaskFilter, Store]]()
  //  //}
  //
  //  //val maskedResults = model.masks match {
  //  //  case Some(s) =>
  //  //    model.masks.get.size match {
  //  //      case n if n > 0 => model.masks.get.map { mask => mask -> store(local_dir / s"${baseString}.${model.test}.${mask.id}.results.tsv") }.toMap
  //  //      case _ => Map[MaskFilter, Store]()
  //  //    }
  //  //  case None => Map[MaskFilter, Store]()
  //  //}
  //  //
  //  //val maskedResultsMergeList = model.masks match {
  //  //  case Some(s) =>
  //  //    model.masks.get.size match {
  //  //      case n if n > 0 => model.masks.get.map { mask => mask -> store(local_dir / s"${baseString}.${model.test}.${mask.id}.mergelist.txt") }.toMap
  //  //      case _ => Map[MaskFilter, Store]()
  //  //    }
  //  //  case None => Map[MaskFilter, Store]()
  //  //}
  //  
  //  //val cohortGroupResultsSplit = model.design match {
  //  //  case "full" => Map[ConfigCohort, Map[String, Store]]()
  //  //  case "strat" =>
  //  //    model.cohortGroupFiles match {
  //  //      case Some(_) =>
  //  //          projectConfig.Cohorts.filter(e => model.cohorts.contains(e.id)).map { cohort =>
  //  //            model.cohortGroupFiles.get.contains(cohort.id) match {
  //  //              case true =>
  //  //                val groups = fileToList(model.cohortGroupFiles.get.filter(e => e.cohort == cohort.id).head.groupFile).map(e => e.split("\t")(0))
  //  //                cohort -> groups.map { group =>
  //  //                  group -> store(dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).cohort_tests(cohort).jobs(group).local.get / s"${group}.${model.test}.gz")
  //  //                }.toMap
  //  //              case false => Map[String, Store]()
  //  //            }
  //  //         }.toMap
  //  //      case _ => Map[ConfigCohort, Map[String, Store]]()
  //  //    }
  //  //}
  //  //
  //  //val cohortMaskedGroupResultsSplit = model.design match {
  //  //  case "full" => Map[ConfigCohort, Map[String, Map[MaskFilter, Store]]]()
  //  //  case "strat" =>
  //  //    (model.cohortGroupFiles, model.masks) match {
  //  //      case (Some(_), Some(_)) =>
  //  //        projectConfig.Cohorts.filter(e => model.cohorts.contains(e.id)).map { cohort =>
  //  //          model.cohortGroupFiles.get.contains(cohort.id) match {
  //  //            case true =>
  //  //              val groups = fileToList(model.cohortGroupFiles.get.filter(e => e.cohort == cohort.id).head.groupFile).map(e => e.split("\t")(0))
  //  //              cohort -> groups.map { group =>
  //  //                group -> model.masks.get.map { mask =>
  //  //                  mask -> store(dirTree.analysisPhenoMap(projectConfig.Phenos.filter(e => e.id == model.pheno).head).models(model).cohort_tests(cohort).jobs(group).local.get / s"${group}.${model.test}.${mask.id}.gz")
  //  //                }.toMap
  //  //              }.toMap
  //  //            case false => Map[String, Map[MaskFilter, Store]]()
  //  //          }
  //  //        }.toMap
  //  //      case _ => Map[ConfigCohort, Map[String, Map[MaskFilter, Store]]]()
  //  //    }
  //  //}

}
