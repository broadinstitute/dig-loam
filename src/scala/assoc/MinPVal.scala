object MinPVal extends loamstream.LoamFile {

  /**
   * Run Masked Group Assoc Analysis via Regenie
   * 
   */
  import ProjectConfig._
  import ModelStores._
  import ArrayStores._
  import Fxns._
  import SchemaStores._
  import ProjectStores._
  import DirTree._
  


  def MinPValue(configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {

    drm {

      val input=modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results
      cmd"""${utils.bash.shMinPVal} $input"""

        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.minpval.log, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.minpval.loco, modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.minpval.predList)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.minpval.base}".split("/").last)

    }
  }

  