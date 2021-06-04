object AssocGroupEpacts extends loamstream.LoamFile {

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
  
  def AssocGroupEpacts(test: String, groupCountMap: scala.collection.mutable.Map[String, Int], configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {
  
    val array = projectConfig.Arrays.filter(e => e.id == configCohorts.head.array).head
    
    val pheno = projectConfig.Phenos.filter(e => e.id == configModel.pheno).head
    
    val maxGroupSizeString = configSchema.maxGroupSize match {
      case Some(_) => s"--max-group-size ${configSchema.maxGroupSize.get}"
      case _ => ""
    }

    val modelTestGroupsKeys = modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).groups.keys
    
    val vcfString = schemaStores((configSchema, configCohorts)).vcf match {
      case Some(s) => s"""--vcf ${schemaStores((configSchema, configCohorts)).vcf.get.data.local.get.toString.split("@")(1)}"""
      case None => s"""--vcf ${arrayStores(array).cleanVcf.get.data.local.get.toString.split("@")(1)}"""
    }
    
    val groupFileIn = schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
      case true => s"""--groupfin ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.local.get.toString.split("@")(1)}"""
      case false => s"""--groupfin ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.local.get.toString.split("@")(1)}"""
    }
    
    var epactsIn = Seq(modelStores((configModel, configSchema, configCohorts, configMeta)).pedEpacts.get, modelStores((configModel, configSchema, configCohorts, configMeta)).modelVarsEpacts.get)
    
    schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos.keys.toList.contains(pheno) match {
      case true => epactsIn = epactsIn ++ Seq(schemaStores((configSchema, configCohorts)).epacts.get.groupFile.phenos(pheno).base.local.get)
      case false => epactsIn = epactsIn ++ Seq(schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.local.get)
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
          --groupfout ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).groups(groupid).groupFile}
          --groupid "${groupid}"
          --ped ${modelStores((configModel, configSchema, configCohorts, configMeta)).pedEpacts.get}
          --vars ${modelStores((configModel, configSchema, configCohorts, configMeta)).modelVarsEpacts.get}
          --test ${test.replace("group.epacts.","")}
          --field "DS"
          ${maxGroupSizeString}
          --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).groups(groupid).results}
          --run 1"""
          .in(epactsIn)
          .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).groups(groupid).groupFile, modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).groups(groupid).results)
          .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).groups(groupid).results}".split("/").last)
      
      }
    
    }
    
    modelTestGroupsKeys.size match {
    
      case n if n > 0 =>
    
        val resultsFile = s"""${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).groups(modelTestGroupsKeys.head.split("\t")(0)).results.toString.split("@")(1).replace(modelTestGroupsKeys.head.split("\t")(0), "___GROUP___")}"""
    
        val resultsFiles = for {
          group <- modelTestGroupsKeys.toList
        } yield {
          modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).groups(group.split("\t")(0)).results
        }
        
        drmWith(imageName = s"${utils.image.imgTools}") {
        
          cmd"""${utils.bash.shMergeResults}
             --results ${resultsFile}
             --groupf ${schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.local.get}
             --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).results}"""
            .in(resultsFiles :+ schemaStores((configSchema, configCohorts)).epacts.get.groupFile.base.base.local.get)
            .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).results)
            .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).results}".split("/").last)
        
        }
    
      case _ => ()
    
    }
    
    drmWith(imageName = s"${utils.image.imgPython2}") {
    
      cmd"""${utils.binary.binPython} ${utils.python.pyQqPlot}
        --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).results}
        --p PVALUE
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).qqPlot}"""
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).results)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).qqPlot)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).qqPlot}".split("/").last)
      
      cmd"""${utils.binary.binPython} ${utils.python.pyMhtPlot}
        --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).results}
        --chr "#CHROM"
        --pos "BEGIN,END"
        --p PVALUE
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).mhtPlot}"""
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).results)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).mhtPlot)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).mhtPlot}".split("/").last)
    
    }
    
    drmWith(imageName = s"${utils.image.imgPython2}") {
    
      cmd"""${utils.binary.binPython} ${utils.python.pyTopGroupResults}
        --results ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).results}
        --group-id-map ${projectStores.geneIdMap.local.get}
        --n 20
        --p PVALUE 
        --out ${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).top20Results}"""
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).results, projectStores.geneIdMap.local.get)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).top20Results)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).assocGroupEpacts(test).top20Results}".split("/").last)
    
    }
  
  }

}
