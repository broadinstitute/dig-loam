object MinPVal extends loamstream.LoamFile {

  import ProjectConfig._
  import ModelStores._
  import ArrayStores._
  import Fxns._
  import SchemaStores._
  import ProjectStores._
  import DirTree._
  


  def MinPValue(configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {

    drm() {

      cmd"""${utils.bash.shMinPVal} ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results}
      ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results.minpval.tsv} ${ProjectStores.geneIdMap}
       """
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results, ProjectStores.geneIdMap)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results.minpval.tsv)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(test).results.minpval.tsv}".split("/").last)

    }
  }
