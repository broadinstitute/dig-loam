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
  
  def AssocTest(configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {
  
    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head
  
    val pheno = projectConfig.Phenos.filter(e => e.id == configModel.pheno).head
  
    val maxGroupSizeString = configSchema.maxGroupSize match {
      case Some(_) => s"--max-group-size ${configSchema.maxGroupSize.get}"
      case _ => ""
    }

    val transString = configModel.trans match {
      case Some(_) => s"--trans ${configModel.trans.get}"
      case None => ""
    }

    configModel.assocPlatforms.contains("epacts") match {
    
      case true =>
    
        drmWith(imageName = s"${utils.image.imgR}") {
        
          cmd"""${utils.binary.binRscript} --vanilla --verbose
            ${utils.r.rConvertPhenoToPed}
            --pheno ${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get}
            --pcs ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get}
            --pheno-col ${configModel.pheno}
            --iid-col ${array.phenoFileId}
            --sex-col ${array.qcSampleFileSrSex}
            --male-code ${array.qcSampleFileMaleCode}
            --female-code ${array.qcSampleFileFemaleCode}
            ${transString}
            --covars "${configModel.covars}"
            --model-vars ${modelStores((configModel, configSchema, configCohorts, configMeta)).modelVarsEpacts.get}
            --ped ${modelStores((configModel, configSchema, configCohorts, configMeta)).pedEpacts.get}"""
            .in(modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get)
            .out(modelStores((configModel, configSchema, configCohorts, configMeta)).pedEpacts.get, modelStores((configModel, configSchema, configCohorts, configMeta)).modelVarsEpacts.get)
            .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).pedEpacts.get}".split("/").last)
        
        }
    
      case false => ()
    
    }

    projectConfig.hailCloud match {
  
      case true =>
  
        local {
        
          googleCopy(modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.google.get)
          googleCopy(modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.google.get)
        
        }
  
      case false => ()
  
    }
  
    for {
    
      test <- modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle.keys
    
    } yield {
  
      configModel.runAssoc match {
  
        case true =>
    
          test.split("\\.")(0) match {
          
            case "hail" =>
          
              projectConfig.hailCloud match {
          
                case true =>
                  
                  googleWith(projectConfig.cloudResources.mtCluster) {
                  
                    hail"""${utils.python.pyHailAssoc} --
                      --hail-utils ${projectStores.hailUtils.google.get}
                      --mt-in ${arrayStores(array).refMt.google.get}
                      --pheno-in ${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.google.get}
                      --iid-col ${array.phenoFileId}
                      --pheno-col ${configModel.pheno}
                      --pcs-include ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.google.get}
                      --variant-stats-in ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get}
                      --test ${test}
                      ${transString}
                      --covars "${configModel.covars}"
                      --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.google.get}
                      --cloud
                      --log ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).resultsHailLog.get.google.get}"""
                        .in(projectStores.hailUtils.google.get, arrayStores(array).refMt.google.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.google.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.google.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.base.google.get)
                        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.google.get, modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).resultsHailLog.get.google.get)
                        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get}.google".split("/").last)
                  
                  }
                  
                  local {
                  
                    googleCopy(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.google.get, modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get)
                    googleCopy(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).resultsHailLog.get.google.get, modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).resultsHailLog.get.local.get)
                  
                  }
                
                case false =>
                
                  drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.matrixTableHail.cpus, mem = projectConfig.resources.matrixTableHail.mem, maxRunTime = projectConfig.resources.matrixTableHail.maxRunTime) {
                  
                    cmd"""${utils.binary.binPython} ${utils.python.pyHailAssoc}
                      --mt-in ${arrayStores(array).refMt.local.get}
                      --pheno-in ${modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get}
                      --iid-col ${array.phenoFileId}
                      --pheno-col ${configModel.pheno}
                      --pcs-include ${modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get}
                      --variant-stats-in ${schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get}
                      --test ${test}
                      ${transString}
                      --covars "${configModel.covars}"
                      --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get}
                      --log ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).resultsHailLog.get.local.get}"""
                        .in(arrayStores(array).refMt.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pheno.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).pcsInclude.local.get, schemaStores((configSchema, configCohorts)).variantsStatsHt.base.local.get)
                        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).resultsHailLog.get.local.get)
                        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get}".split("/").last)
                  
                  }
          
              }
  
              drmWith(imageName = s"${utils.image.imgTools}") {
  
                cmd"""${utils.binary.binTabix} -f -b 2 -e 2 ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get}"""
                  .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get)
                  .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).resultsTbi)
                  .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).resultsTbi}".split("/").last)
              
              }
          
            case _ => ()
          
          }
  
        case false => ()
  
      }
  
      drmWith(imageName = s"${utils.image.imgPython2}") {
      
        cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get}
          --p pval
          --maf maf
          --mac mac
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).qqPlot}
          --out-low-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).qqPlotLowMaf}
          --out-mid-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).qqPlotMidMaf}
          --out-high-maf ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).qqPlotHighMaf}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).qqPlot, modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).qqPlotLowMaf, modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).qqPlotMidMaf, modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).qqPlotHighMaf)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).qqPlot}".split("/").last)
        
        cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get}
          --chr "#chr"
          --pos pos
          --p pval
          --mac mac
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).mhtPlot}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).mhtPlot)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).mhtPlot}".split("/").last)
      
      }
  
      drmWith(imageName = s"${utils.image.imgPython2}") {
    
        cmd"""${utils.binary.binPython} ${utils.python.pyTopResults}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get}
          --n 1000
          --p pval
          --mac mac 
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top1000Results}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top1000Results)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top1000Results}".split("/").last)
      
      }
  
      drmWith(imageName = s"${utils.image.imgEnsemblVep}", cores = projectConfig.resources.vep.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.vep.maxRunTime) {
  
        cmd"""${utils.bash.shAnnotateResults}
          ${arrayStores(array).refSitesVcf.local.get}
          ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top1000Results}
          ${projectConfig.resources.vep.cpus}
          ${projectStores.fasta}
          ${projectStores.vepCacheDir}
          ${projectStores.vepPluginsDir}
          ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top1000ResultsAnnot}"""
        .in(arrayStores(array).refSitesVcf.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top1000Results, projectStores.fasta, projectStores.vepCacheDir, projectStores.vepPluginsDir)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top1000ResultsAnnot)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top1000ResultsAnnot}".split("/").last)
  
      }
  
      var top20In = Seq(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top1000ResultsAnnot)
      var hiLdStrings = Seq[String]()
  
      configModel.knowns match {
        case Some(_) =>
          for {
            k <- configModel.knowns.get
          } yield {
            top20In = top20In :+ projectStores.knownStores(projectConfig.Knowns.filter(e => e.id == k).head).hiLd.local.get
            hiLdStrings = hiLdStrings ++ Seq(s"${projectStores.knownStores(projectConfig.Knowns.filter(e => e.id == k).head).hiLd.toString.split("@")(1)}")
          }
        case None => ()
      }
  
      drmWith(imageName = s"${utils.image.imgR}", cores = projectConfig.resources.standardR.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.standardR.maxRunTime) {
      
        cmd"""${utils.binary.binRscript} --vanilla --verbose
          ${utils.r.rTop20}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top1000ResultsAnnot}
          --chr "#chr"
          --pos pos
          --known-loci "${hiLdStrings.mkString(",")}"
          --p pval
          --test ${test}
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top20AnnotAlignedRisk}"""
          .in(top20In)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top20AnnotAlignedRisk)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).top20AnnotAlignedRisk}".split("/").last)
      
      }
  
      projectConfig.maxSigRegions match {
  
        case Some(s) =>
        
          drmWith(imageName = s"${utils.image.imgPython2}", cores = projectConfig.resources.standardPython.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.standardPython.maxRunTime) {
            
            cmd"""${utils.binary.binPython} ${utils.python.pyExtractTopRegions}
              --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get}
              --chr "#chr"
              --pos pos
              --p pval
              --max-regions ${s}
              --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).sigRegions}"""
              .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get)
              .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).sigRegions)
              .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).sigRegions}".split("/").last)
          
          }
        
        case None =>
        
          drmWith(imageName = s"${utils.image.imgPython2}", cores = projectConfig.resources.standardPython.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.standardPython.maxRunTime) {
            
            cmd"""${utils.binary.binPython} ${utils.python.pyExtractTopRegions}
              --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get}
              --chr "#chr"
              --pos pos
              --p pval
              --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).sigRegions}"""
              .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get)
              .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).sigRegions)
              .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).sigRegions}".split("/").last)
          
          }
  
      }
      
      drmWith(imageName = s"${utils.image.imgLocuszoom}", cores = projectConfig.resources.locuszoom.cpus, mem = projectConfig.resources.vep.mem, maxRunTime = projectConfig.resources.locuszoom.maxRunTime) {
      
        cmd"""${utils.bash.shRegPlot} 
          ${utils.binary.binTabix}
          ${utils.binary.binLocuszoom}
          ${utils.binary.binGhostscript}
          ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).sigRegions}
          ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get}
          EUR
          hg19
          1000G_Nov2014
          ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).regPlotsBase}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).results.local.get, modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).sigRegions)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).regPlotsPdf)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocSingle(test).regPlotsPdf}".split("/").last)
      
      }
  
    }
  
    val groupCountMap = scala.collection.mutable.Map[String, Int]()
  
    configModel.runAssoc match {
  
      case true =>
  
        val gFile = schemaStores((configSchema, configCohorts)).groupFile.phenos.keys.toList.contains(pheno) match {
          case true => s"""${schemaStores((configSchema, configCohorts)).groupFile.phenos(pheno).base.local.get.toString.split("@")(1)}"""
          case false => s"""${schemaStores((configSchema, configCohorts)).groupFile.base.base.local.get.toString.split("@")(1)}"""
        }
        try {
          val gFileList = fileToList(checkPath(gFile))
          println(s"""calculating group variant counts for group file: ${gFile}""")
          for {
            group <- gFileList
          } yield {
            val geneName = group.split("\t")(0)
            val N = group.split("\t").tail.size
            groupCountMap(geneName) = N
          }
        }
        catch {
          case x: CfgException =>
            println(s"""skipping group variant count calculation due to missing group file: ${gFile}""")
        }
  
      case false => ()
  
    }
  
    for {
    
      test <- modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup.keys.toList
    
    } yield {
    
      test.split("\\.")(0) match {
    
        case "epacts" =>
  
          configModel.runAssoc match {
  
            case true =>
  
              val modelTestGroupsKeys = modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).groups.keys
  
              val vcfString = schemaStores((configSchema, configCohorts)).vcf match {
                case Some(s) => s"""--vcf ${schemaStores((configSchema, configCohorts)).vcf.get.data.local.get.toString.split("@")(1)}"""
                case None => s"""--vcf ${arrayStores(array).cleanVcf.get.data.local.get.toString.split("@")(1)}"""
              }
  
              val groupFileIn = schemaStores((configSchema, configCohorts)).groupFile.phenos.keys.toList.contains(pheno) match {
                case true => s"""--groupfin ${schemaStores((configSchema, configCohorts)).groupFile.phenos(pheno).base.local.get.toString.split("@")(1)}"""
                case false => s"""--groupfin ${schemaStores((configSchema, configCohorts)).groupFile.base.base.local.get.toString.split("@")(1)}"""
              }
  
              var epactsIn = Seq(modelStores((configModel, configSchema, configCohorts, configMeta)).pedEpacts.get, modelStores((configModel, configSchema, configCohorts, configMeta)).modelVarsEpacts.get)
  
              schemaStores((configSchema, configCohorts)).groupFile.phenos.keys.toList.contains(pheno) match {
                case true => epactsIn = epactsIn ++ Seq(schemaStores((configSchema, configCohorts)).groupFile.phenos(pheno).base.local.get)
                case false => epactsIn = epactsIn ++ Seq(schemaStores((configSchema, configCohorts)).groupFile.base.base.local.get)
              }
  
              schemaStores((configSchema, configCohorts)).vcf match {
                case Some(s) => epactsIn = epactsIn ++ Seq(schemaStores((configSchema, configCohorts)).vcf.get.data.local.get)
                case None => epactsIn = epactsIn ++ Seq(arrayStores(array).cleanVcf.get.data.local.get)
              }
  
              modelTestGroupsKeys.toList.toSet.intersect(groupCountMap.keys.toList.toSet).size match {
                case n if n == modelTestGroupsKeys.toList.size => println(s"""extracting group sizes for group file ${groupFileIn} under model ${configModel.id} and test ${test} (${n}/${groupCountMap.keys.toList.size})""")
                case m => throw new CfgException(s"""unable to find some group sizes for group file ${groupFileIn} under model ${configModel.id} and test ${test} (${m}/${groupCountMap.keys.toList.size}) - ${modelTestGroupsKeys.toList.toSet.diff(modelTestGroupsKeys.toList.toSet.intersect(groupCountMap.keys.toList.toSet))}""")
              }
              
              for {
              
                group <- modelTestGroupsKeys.toList
              
              } yield {
              
                val groupid = group.split("\t")(0)
  
                val groupCores = groupCountMap(group) match {
                  case n if n >= 2000 => projectConfig.resources.highMemEpacts.cpus
                  case m if m >= 1000 => projectConfig.resources.midMemEpacts.cpus
                  case _ => projectConfig.resources.lowMemEpacts.cpus
                }
  
                val groupMem = groupCountMap(group) match {
                  case n if n >= 2000 => projectConfig.resources.highMemEpacts.mem
                  case m if m >= 1000 => projectConfig.resources.midMemEpacts.mem
                  case _ => projectConfig.resources.lowMemEpacts.mem
                }
  
                val groupTime = groupCountMap(group) match {
                  case n if n >= 2000 => projectConfig.resources.highMemEpacts.maxRunTime
                  case m if m >= 1000 => projectConfig.resources.midMemEpacts.maxRunTime
                  case _ => projectConfig.resources.lowMemEpacts.maxRunTime
                }
  
                drmWith(imageName = s"${utils.image.imgUmichStatgen}", cores = groupCores, mem = groupMem, maxRunTime = groupTime) {
                
                  cmd"""${utils.bash.shEpacts}
                    --bin ${utils.binary.binEpacts}
                    --type group
                    ${vcfString}
                    ${groupFileIn}
                    --groupfout ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).groups(groupid).groupFile}
                    --groupid "${groupid}"
                    --ped ${modelStores((configModel, configSchema, configCohorts, configMeta)).pedEpacts.get}
                    --vars ${modelStores((configModel, configSchema, configCohorts, configMeta)).modelVarsEpacts.get}
                    --test ${test.replace("epacts.","")}
                    --field "DS"
                    ${maxGroupSizeString}
                    --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).groups(groupid).results}
                    --run 1"""
                    .in(epactsIn)
                    .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).groups(groupid).groupFile, modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).groups(groupid).results)
                    .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).groups(groupid).results}".split("/").last)
                
                }
              
              }
              
              modelTestGroupsKeys.size match {
              
                case n if n > 0 =>
              
                  val resultsFile = s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).groups(modelTestGroupsKeys.head.split("\t")(0)).results.toString.split("@")(1).replace(modelTestGroupsKeys.head.split("\t")(0), "___GROUP___")}"""
              
                  val resultsFiles = for {
                    group <- modelTestGroupsKeys.toList
                  } yield {
                    modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).groups(group.split("\t")(0)).results
                  }
                  
                  drmWith(imageName = s"${utils.image.imgTools}") {
                  
                    cmd"""${utils.bash.shMergeResults}
                       --results ${resultsFile}
                       --groupf ${schemaStores((configSchema, configCohorts)).groupFile.base.base.local.get}
                       --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).results}"""
                      .in(resultsFiles :+ schemaStores((configSchema, configCohorts)).groupFile.base.base.local.get)
                      .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).results)
                      .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).results}".split("/").last)
                  
                  }
              
                case _ => ()
              
              }
  
            case false => ()
  
          }
  
          drmWith(imageName = s"${utils.image.imgPython2}") {
      
            cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
              --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).results}
              --p PVALUE
              --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).qqPlot}"""
              .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).results)
              .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).qqPlot)
              .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).qqPlot}".split("/").last)
            
            cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
              --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).results}
              --chr "#CHROM"
              --pos "BEGIN,END"
              --p PVALUE
              --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).mhtPlot}"""
              .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).results)
              .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).mhtPlot)
              .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).mhtPlot}".split("/").last)
          
          }
  
          drmWith(imageName = s"${utils.image.imgPython2}") {
    
            cmd"""${utils.binary.binPython} ${utils.python.pyTopGroupResults}
              --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).results}
              --group-id-map ${projectStores.geneIdMap.local.get}
              --n 20
              --p PVALUE 
              --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).top20Results}"""
              .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).results, projectStores.geneIdMap.local.get)
              .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).top20Results)
              .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroup(test).top20Results}".split("/").last)
          
          }
  
        case _ => ()
  
      }
  
    }
  
    val maskGroupCountMap = scala.collection.mutable.Map[String, Int]()
  
    configModel.runAssoc match {
  
      case true =>
  
        schemaStores((configSchema, configCohorts)).groupFile.phenos.keys.toList.contains(pheno) match {
          case true => 
            for {
              gm <- schemaStores((configSchema, configCohorts)).groupFile.phenos(pheno).masks.keys.toList
            } yield {
              val gFile = s"""${schemaStores((configSchema, configCohorts)).groupFile.phenos(pheno).masks(gm).local.get.toString.split("@")(1)}"""
              try {
                val gFileList = fileToList(checkPath(gFile))
                println(s"""calculating group variant counts for group file: ${gFile}""")
                for {
                  group <- gFileList
                } yield {
                  val geneName = group.split("\t")(0)
                  val N = group.split("\t").tail.size
  	        	maskGroupCountMap.keys.toList.contains(geneName) match {
                    case true =>
                      maskGroupCountMap(geneName) < N match {
                        case true => maskGroupCountMap(geneName) = N
                        case false => ()
                      }
                    case false => maskGroupCountMap(geneName) = N
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
              gm <- schemaStores((configSchema, configCohorts)).groupFile.base.masks.keys.toList
            } yield {
              val gFile = s"""${schemaStores((configSchema, configCohorts)).groupFile.base.masks(gm).local.get.toString.split("@")(1)}"""
              try {
                val gFileList = fileToList(checkPath(gFile))
                println(s"""calculating group variant counts for group file: ${gFile}""")
                for {
                  group <- gFileList
                } yield {
                  val geneName = group.split("\t")(0)
                  val N = group.split("\t").tail.size
  	        	maskGroupCountMap.keys.toList.contains(geneName) match {
                    case true =>
                      maskGroupCountMap(geneName) < N match {
                        case true => maskGroupCountMap(geneName) = N
                        case false => ()
                      }
                    case false => maskGroupCountMap(geneName) = N
                  }
                }
              }
              catch {
                case x: CfgException =>
                  println(s"""skipping group variant count calculation due to missing group file: ${gFile}""")
              }
            }
        }
  
      case false => ()
  
    }
  
    for {
  
      test <- modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup.keys.toList
    
    } yield {
  
      test.split("\\.")(0) match {
          
        case "epacts" =>
  
          configModel.runAssoc match {
  
            case true =>
  
              schemaStores((configSchema, configCohorts)).groupFile.phenos.keys.toList.contains(pheno) match {
                case true => 
                  for {
                    gm <- schemaStores((configSchema, configCohorts)).groupFile.phenos(pheno).masks.keys.toList
                  } yield {
                    val modelTestGroupsKeys = modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(gm).groups.keys
                  
                    modelTestGroupsKeys.toList.toSet.intersect(maskGroupCountMap.keys.toList.toSet).size match {
                      case n if n == modelTestGroupsKeys.size => println(s"""extracting group sizes for masked group files ${schemaStores((configSchema, configCohorts)).groupFile.phenos(pheno).masks(gm).local.get.toString.split("@")(1)} under model ${configModel.id} and test ${test} (${n}/${maskGroupCountMap.keys.toList.size})""")
                      case m => throw new CfgException(s"""unable to find some group sizes for masked group files ${schemaStores((configSchema, configCohorts)).groupFile.phenos(pheno).masks(gm).local.get.toString.split("@")(1)} under model ${configModel.id} and test ${test} (${m}/${maskGroupCountMap.keys.toList.size}) - ${modelTestGroupsKeys.toList.toSet.diff(modelTestGroupsKeys.toList.toSet.intersect(maskGroupCountMap.keys.toList.toSet))}""")
                    }
                  }
                case false =>
                  for {
                    gm <- schemaStores((configSchema, configCohorts)).groupFile.base.masks.keys.toList
                  } yield {
                    val modelTestGroupsKeys = modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(gm).groups.keys
                  
                    modelTestGroupsKeys.toList.toSet.intersect(maskGroupCountMap.keys.toList.toSet).size match {
                      case n if n == modelTestGroupsKeys.size => println(s"""extracting group sizes for masked group files ${schemaStores((configSchema, configCohorts)).groupFile.base.masks(gm).local.get.toString.split("@")(1)} under model ${configModel.id} and test ${test} (${n}/${maskGroupCountMap.keys.toList.size})""")
                      case m => throw new CfgException(s"""unable to find some group sizes for masked group files ${schemaStores((configSchema, configCohorts)).groupFile.base.masks(gm).local.get.toString.split("@")(1)} under model ${configModel.id} and test ${test} (${m}/${maskGroupCountMap.keys.toList.size}) - ${modelTestGroupsKeys.toList.toSet.diff(modelTestGroupsKeys.toList.toSet.intersect(maskGroupCountMap.keys.toList.toSet))}""")
                    }
                  }
              }
  
              for {
              
                group <- maskGroupCountMap.keys.toList
              
              } yield {
              
                //val groupid = group.split("\t")(0)
              
                val groupMasks = modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test).keys.toList.filter(e => modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(e).groups.keys.toList.contains(group))
              
                ! groupMasks.isEmpty match {
              
                  case true =>
              
                    val vcfString = schemaStores((configSchema, configCohorts)).vcf match {
                      case Some(s) => s"""--vcf ${schemaStores((configSchema, configCohorts)).vcf.get.data.local.get.toString.split("@")(1)}"""
                      case None => s"""--vcf ${arrayStores(array).cleanVcf.get.data.local.get.toString.split("@")(1)}"""
                    }
  
                    val groupFileIn = schemaStores((configSchema, configCohorts)).groupFile.phenos.keys.toList.contains(pheno) match {
                      case true => s"""--groupfin ${schemaStores((configSchema, configCohorts)).groupFile.phenos(pheno).base.local.get.toString.split("@")(1).replace("groupfile","groupfile.___MASK___")}"""
                      case false => s"""--groupfin ${schemaStores((configSchema, configCohorts)).groupFile.base.base.local.get.toString.split("@")(1).replace("groupfile","groupfile.___MASK___")}"""
                    }
                    
                    var epactsIn = Seq(modelStores((configModel, configSchema, configCohorts, configMeta)).pedEpacts.get, modelStores((configModel, configSchema, configCohorts, configMeta)).modelVarsEpacts.get)
              
                    for {
                      gm <- groupMasks
                    } yield {
                      schemaStores((configSchema, configCohorts)).groupFile.phenos.keys.toList.contains(pheno) match {
                        case true => epactsIn = epactsIn ++ Seq(schemaStores((configSchema, configCohorts)).groupFile.phenos(pheno).masks(gm).local.get)
                        case false => epactsIn = epactsIn ++ Seq(schemaStores((configSchema, configCohorts)).groupFile.base.masks(gm).local.get)
                      }
                    }
              
                    schemaStores((configSchema, configCohorts)).vcf match {
                      case Some(s) => epactsIn = epactsIn ++ Seq(schemaStores((configSchema, configCohorts)).vcf.get.data.local.get)
                      case None => epactsIn = epactsIn ++ Seq(arrayStores(array).cleanVcf.get.data.local.get)
                    }
              
                    var epactsOut = Seq[Store]()
              
                    for {
                      gm <- groupMasks
                    } yield {
                      epactsOut = epactsOut ++ Seq(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(gm).groups(group).groupFile, modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(gm).groups(group).results)
                    }
              
                    val groupMasksString = groupMasks.map(e => e.id).mkString(",")
  
                    val groupCores = groupCountMap(group) match {
                      case n if n >= 2000 => projectConfig.resources.highMemEpacts.cpus
                      case m if m >= 1000 => projectConfig.resources.midMemEpacts.cpus
                      case _ => projectConfig.resources.lowMemEpacts.cpus
                    }
                    
                    val groupMem = groupCountMap(group) match {
                      case n if n >= 2000 => projectConfig.resources.highMemEpacts.mem
                      case m if m >= 1000 => projectConfig.resources.midMemEpacts.mem
                      case _ => projectConfig.resources.lowMemEpacts.mem
                    }
                    
                    val groupTime = groupCountMap(group) match {
                      case n if n >= 2000 => projectConfig.resources.highMemEpacts.maxRunTime
                      case m if m >= 1000 => projectConfig.resources.midMemEpacts.maxRunTime
                      case _ => projectConfig.resources.lowMemEpacts.maxRunTime
                    }
                    
                    drmWith(imageName = s"${utils.image.imgUmichStatgen}", cores = groupCores, mem = groupMem, maxRunTime = groupTime) {
                    
                      cmd"""${utils.bash.shEpacts}
                        --bin ${utils.binary.binEpacts}
                        --type group
                        ${vcfString}
                        ${groupFileIn}
                        --groupid "${group}"
                        --groupfout ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(groupMasks.head).groups(group).groupFile.toString.split("@")(1).replace(groupMasks.head.id,"___MASK___")}
                        --ped ${modelStores((configModel, configSchema, configCohorts, configMeta)).pedEpacts.get}
                        --vars ${modelStores((configModel, configSchema, configCohorts, configMeta)).modelVarsEpacts.get}
                        --test ${test.replace("epacts.","")}
                        --field "DS"
                        ${maxGroupSizeString}
                        --masks ${groupMasksString}
                        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(groupMasks.head).groups(group).results.toString.split("@")(1).replace(groupMasks.head.id,"___MASK___")}
                        --run 1"""
                        .in(epactsIn)
                        .out(epactsOut)
                        .tag(s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(groupMasks.head).groups(group).results.toString.split("@")(1).replace(groupMasks.head.id,"___MASK___")}""".split("/").last)
                    
                    }
                  
                  case false => ()
              
                }
              
              }
  
            case false => ()
  
          }
  
          for {
          
            mask <- modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test).keys.toList
          
          } yield {
  
            configModel.runAssoc match {
  
              case true =>
  
                val modelMaskTestGroupsKeys = modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).groups.keys
                
                modelMaskTestGroupsKeys.size match {
                
                  case n if n > 0 =>
                
                    val maskResultsFile = s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).groups(modelMaskTestGroupsKeys.head.split("\t")(0)).results.toString.split("@")(1).replace(modelMaskTestGroupsKeys.head.split("\t")(0), "___GROUP___")}"""
                    
                    val maskResultsFiles = for {
                      group <- modelMaskTestGroupsKeys.toList
                    } yield {
                      modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).groups(group.split("\t")(0)).results
                    }
                
                    drmWith(imageName = s"${utils.image.imgTools}") {
                    
                      cmd"""${utils.bash.shMergeResults}
                         --results ${maskResultsFile}
                         --groupf ${schemaStores((configSchema, configCohorts)).groupFile.base.masks(mask).local.get}
                         --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).results}"""
                        .in(maskResultsFiles :+ schemaStores((configSchema, configCohorts)).groupFile.base.masks(mask).local.get)
                        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).results)
                        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).results}".split("/").last)
                    
                    }
                
                  case _ => ()
                
                }
  
              case false => ()
  
            }
            
            drmWith(imageName = s"${utils.image.imgPython2}") {
            
              cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
                --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).results}
                --p PVALUE
                --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).qqPlot}"""
                .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).results)
                .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).qqPlot)
                .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).qqPlot}".split("/").last)
              
              cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
                --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).results}
                --chr "#CHROM"
                --pos "BEGIN,END"
                --p PVALUE
                --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).mhtPlot}"""
                .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).results)
                .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).mhtPlot)
                .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).mhtPlot}".split("/").last)
            
            }
  
            drmWith(imageName = s"${utils.image.imgPython2}") {
    
              cmd"""${utils.binary.binPython} ${utils.python.pyTopGroupResults}
                --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).results}
                --group-id-map ${projectStores.geneIdMap.local.get}
                --n 20
                --p PVALUE 
                --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).top20Results}"""
                .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).results, projectStores.geneIdMap.local.get)
                .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).top20Results)
                .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroup(test)(mask).top20Results}".split("/").last)
            
            }
  
          }
  
        case _ => ()
        
      }
  
    }
  
  }

}