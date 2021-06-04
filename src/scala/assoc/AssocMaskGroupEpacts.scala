object AssocMaskGroupEpacts extends loamstream.LoamFile {

  /**
   * Run Grouped Variant Epacts Assoc Analysis
   * 
   */
  import ProjectConfig._
  import ModelStores._
  import ArrayStores._
  import Fxns._
  import SchemaStores._
  import ProjectStores._
  import DirTree._
  
  def AssocMaskGroupEpacts(test: String, maskGroupCountMap: scala.collection.mutable.Map[String, Int], configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {
  
    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head
    
    val pheno = projectConfig.Phenos.filter(e => e.id == configModel.pheno).head
    
    val maxGroupSizeString = configSchema.maxGroupSize match {
      case Some(_) => s"--max-group-size ${configSchema.maxGroupSize.get}"
      case _ => ""
    }
    
    schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
      case true => 
        for {
          gm <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks.keys.toList
        } yield {
          val modelTestGroupsKeys = modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(gm).groups.keys
        
          modelTestGroupsKeys.toList.toSet.intersect(maskGroupCountMap.keys.toList.toSet).size match {
            case n if n == modelTestGroupsKeys.size => println(s"""extracting group sizes for masked group files ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks(gm).local.get.toString.split("@")(1)} under model ${configModel.id} and test ${test} (${n}/${maskGroupCountMap.keys.toList.size})""")
            case m => throw new CfgException(s"""unable to find some group sizes for masked group files ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks(gm).local.get.toString.split("@")(1)} under model ${configModel.id} and test ${test} (${m}/${maskGroupCountMap.keys.toList.size}) - ${modelTestGroupsKeys.toList.toSet.diff(modelTestGroupsKeys.toList.toSet.intersect(maskGroupCountMap.keys.toList.toSet))}""")
          }
        }
      case false =>
        for {
          gm <- schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks.keys.toList
        } yield {
          val modelTestGroupsKeys = modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(gm).groups.keys
        
          modelTestGroupsKeys.toList.toSet.intersect(maskGroupCountMap.keys.toList.toSet).size match {
            case n if n == modelTestGroupsKeys.size => println(s"""extracting group sizes for masked group files ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks(gm).local.get.toString.split("@")(1)} under model ${configModel.id} and test ${test} (${n}/${maskGroupCountMap.keys.toList.size})""")
            case m => throw new CfgException(s"""unable to find some group sizes for masked group files ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks(gm).local.get.toString.split("@")(1)} under model ${configModel.id} and test ${test} (${m}/${maskGroupCountMap.keys.toList.size}) - ${modelTestGroupsKeys.toList.toSet.diff(modelTestGroupsKeys.toList.toSet.intersect(maskGroupCountMap.keys.toList.toSet))}""")
          }
        }
    }
    
    for {
    
      group <- maskGroupCountMap.keys.toList
    
    } yield {
    
      val groupMasks = modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test).keys.toList.filter(e => modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(e).groups.keys.toList.contains(group))
    
      ! groupMasks.isEmpty match {
    
        case true =>
    
          val vcfString = schemaStores((configSchema, configCohorts)).vcf match {
            case Some(s) => s"""--vcf ${schemaStores((configSchema, configCohorts)).vcf.get.data.local.get.toString.split("@")(1)}"""
            case None => s"""--vcf ${arrayStores(array).cleanVcf.get.data.local.get.toString.split("@")(1)}"""
          }
    
          val groupFileIn = schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
            case true => s"""--groupfin ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.local.get.toString.split("@")(1).replace("groupfile","groupfile.___MASK___")}"""
            case false => s"""--groupfin ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.local.get.toString.split("@")(1).replace("groupfile","groupfile.___MASK___")}"""
          }
          
          var epactsIn = Seq(modelStores((configModel, configSchema, configCohorts, configMeta)).pedEpacts.get, modelStores((configModel, configSchema, configCohorts, configMeta)).modelVarsEpacts.get)
    
          for {
            gm <- groupMasks
          } yield {
            schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
              case true => epactsIn = epactsIn ++ Seq(schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).masks(gm).local.get)
              case false => epactsIn = epactsIn ++ Seq(schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks(gm).local.get)
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
            epactsOut = epactsOut ++ Seq(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(gm).groups(group).groupFile, modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(gm).groups(group).results)
          }
    
          val groupMasksString = groupMasks.map(e => e.id).mkString(",")
    
          val groupCores = maskGroupCountMap(group) match {
            case n if n >= 2000 => projectConfig.resources.highMemEpacts.cpus
            case m if m >= 1000 => projectConfig.resources.midMemEpacts.cpus
            case _ => projectConfig.resources.lowMemEpacts.cpus
          }
          
          val groupMem = maskGroupCountMap(group) match {
            case n if n >= 2000 => projectConfig.resources.highMemEpacts.mem
            case m if m >= 1000 => projectConfig.resources.midMemEpacts.mem
            case _ => projectConfig.resources.lowMemEpacts.mem
          }
          
          val groupTime = maskGroupCountMap(group) match {
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
              --groupfout ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(groupMasks.head).groups(group).groupFile.toString.split("@")(1).replace(groupMasks.head.id,"___MASK___")}
              --ped ${modelStores((configModel, configSchema, configCohorts, configMeta)).pedEpacts.get}
              --vars ${modelStores((configModel, configSchema, configCohorts, configMeta)).modelVarsEpacts.get}
              --test ${test.replace("group.epacts.","")}
              --field "DS"
              ${maxGroupSizeString}
              --masks ${groupMasksString}
              --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(groupMasks.head).groups(group).results.toString.split("@")(1).replace(groupMasks.head.id,"___MASK___")}
              --run 1"""
              .in(epactsIn)
              .out(epactsOut)
              .tag(s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(groupMasks.head).groups(group).results.toString.split("@")(1).replace(groupMasks.head.id,"___MASK___")}""".split("/").last)
          
          }
        
        case false => ()
    
      }
    
    }
    
    for {
    
      mask <- modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test).keys.toList
    
    } yield {
    
      val modelMaskTestGroupsKeys = modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).groups.keys
      
      modelMaskTestGroupsKeys.size match {
      
        case n if n > 0 =>
      
          val maskResultsFile = s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).groups(modelMaskTestGroupsKeys.head.split("\t")(0)).results.toString.split("@")(1).replace(modelMaskTestGroupsKeys.head.split("\t")(0), "___GROUP___")}"""
          
          val maskResultsFiles = for {
            group <- modelMaskTestGroupsKeys.toList
          } yield {
            modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).groups(group.split("\t")(0)).results
          }
      
          drmWith(imageName = s"${utils.image.imgTools}") {
          
            cmd"""${utils.bash.shMergeResults}
               --results ${maskResultsFile}
               --groupf ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks(mask).local.get}
               --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).results}"""
              .in(maskResultsFiles :+ schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.masks(mask).local.get)
              .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).results)
              .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).results}".split("/").last)
          
          }
      
        case _ => ()
      
      }
      
      drmWith(imageName = s"${utils.image.imgPython2}") {
      
        cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).results}
          --p PVALUE
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).qqPlot}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).results)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).qqPlot)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).qqPlot}".split("/").last)
        
        cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).results}
          --chr "#CHROM"
          --pos "BEGIN,END"
          --p PVALUE
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).mhtPlot}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).results)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).mhtPlot)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).mhtPlot}".split("/").last)
      
      }
    
      drmWith(imageName = s"${utils.image.imgPython2}") {
    
        cmd"""${utils.binary.binPython} ${utils.python.pyTopGroupResults}
          --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).results}
          --group-id-map ${projectStores.geneIdMap.local.get}
          --n 20
          --p PVALUE 
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).top20Results}"""
          .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).results, projectStores.geneIdMap.local.get)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).top20Results)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocMaskGroupEpacts(test)(mask).top20Results}".split("/").last)
      
      }
    
    }
  
  }

}
