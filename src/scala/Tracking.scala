object Tracking extends loamstream.LoamFile {

  import Fxns._
  import DirTree._
  import ProjectConfig._
  import Collections._
  import ProjectStores._
  import ArrayStores._
  import SchemaStores._
  import ModelStores._
  
  def trackObjects(): Unit = {
  
    writeObject(obj = projectConfig, filename = s"${dirTree.base.local.get}/cfg.projectConfig")
    writeObject(obj = utils, filename = s"${dirTree.base.local.get}/cfg.utils")
    writeObject(obj = dirTree, filename = s"${dirTree.base.local.get}/cfg.dirTree")
  
    writeObject(obj = schemaCohorts, filename = s"${dirTree.base.local.get}/cfg.schemaCohorts")
    writeObject(obj = schemaFilterFields, filename = s"${dirTree.base.local.get}/cfg.schemaFilterFields")
    writeObject(obj = modelCollections, filename = s"${dirTree.base.local.get}/cfg.modelCollections")
    writeObject(obj = modelMetaCollections, filename = s"${dirTree.base.local.get}/cfg.modelMetaCollections")
    
    writeObject(obj = projectStores, filename = s"${dirTree.base.local.get}/cfg.projectStores")
    writeObject(obj = arrayStores, filename = s"${dirTree.base.local.get}/cfg.arrayStores")
    writeObject(obj = schemaStores, filename = s"${dirTree.base.local.get}/cfg.schemaStores")
    writeObject(obj = modelStores, filename = s"${dirTree.base.local.get}/cfg.modelStores")
  
    for {
      ms <- modelStores.keys
    } yield {
      val model = ms._1.id
      val schema = ms._2.id
      val cohorts = ms._3.map(e => e.id).mkString("_")
      val meta = ms._4 match {
        case Some(s) => s.id
        case None => "none"
      }
      for {
        test <- modelStores(ms).assocSingle.keys
      } yield {
        writeObject(obj = modelStores(ms).assocSingle(test), filename = s"${dirTree.base.local.get}/cfg.modelStores.${model}.${schema}.${cohorts}.${meta}.assocSingle.${test}")
      }
      
      for {
        test <- modelStores(ms).assocGroup.keys
      } yield {
        writeObject(obj = modelStores(ms).assocGroup(test), filename = s"${dirTree.base.local.get}/cfg.modelStores.${model}.${schema}.${cohorts}.${meta}.assocGroup.${test}")
        writeObject(obj = modelStores(ms).assocGroup(test).groups.keys, filename = s"${dirTree.base.local.get}/cfg.modelStores.${model}.${schema}.${cohorts}.${meta}.assocGroup.${test}.groups.keys")
      }
      
      for {
        test <- modelStores(ms).assocMaskGroup.keys
      } yield {
        for {
          mask <- modelStores(ms).assocMaskGroup(test).keys
        } yield {
          writeObject(obj = modelStores(ms).assocMaskGroup(test)(mask), filename = s"${dirTree.base.local.get}/cfg.modelStores.${model}.${schema}.${cohorts}.${meta}.assocMaskGroup.${test}.${mask.id}")
          writeObject(obj = modelStores(ms).assocMaskGroup(test)(mask).groups.keys, filename = s"${dirTree.base.local.get}/cfg.modelStores.${model}.${schema}.${cohorts}.${meta}.assocMaskGroup.${test}.${mask.id}.groups.keys")
        }
      }
  
    }
  

}
