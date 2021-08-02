object MinPVal extends loamstream.LoamFile {

  import ProjectConfig._
  import ModelStores._
  import ArrayStores._
  import Fxns._
  import SchemaStores._
  import ProjectStores._
  import DirTree._
  


  def MinPVal(test: String, configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {

    drm() {

      cmd"""${utils.bash.shMinPVal} ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results}
      ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.minPVal.get} ${projectStores.geneIdMap.local.get}
       """
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results, projectStores.geneIdMap.local.get)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.minPVal.get)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).summary.minPVal.get}".split("/").last)

    }
  }

}
