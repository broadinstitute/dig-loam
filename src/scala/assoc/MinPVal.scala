object MinPVal extends loamstream.LoamFile {

  import ProjectConfig._
  import ModelStores._
  import ArrayStores._
  import Fxns._
  import SchemaStores._
  import ProjectStores._
  import DirTree._

  def MinPVal(configTest: ConfigTest, mask: MaskFilter, configModel: ConfigModel, configSchema: ConfigSchema, configCohorts: Seq[ConfigCohort], configMeta: Option[ConfigMeta] = None): Unit = {

    drmWith(imageName = s"${utils.image.imgPython3}") {

      cmd"""${utils.bash.shMinPVal}
      ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results}
      ${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.minPVal.get}
      ${projectStores.geneIdMap.local.get}
      ${utils.binary.binPython}
      ${utils.python.pyMinPValTest}
       """
        .in(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).results, projectStores.geneIdMap.local.get)
        .out(modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.minPVal.get)
        .tag(s"${modelStores((configModel, configSchema, configCohorts, configMeta)).regenie.get.assocGroup(configTest)(mask).summary.minPVal.get}".split("/").last)

    }
  }

}
