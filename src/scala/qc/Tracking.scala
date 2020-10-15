object Tracking extends loamstream.LoamFile {

  import Fxns._
  import DirTree._
  import ProjectConfig._
  import ProjectStores._
  import ArrayStores._
  
  def trackObjects(): Unit = {
  
    writeObject(obj = projectConfig, filename = s"${dirTree.base.local.get}/cfg.projectConfig")
    writeObject(obj = utils, filename = s"${dirTree.base.local.get}/cfg.utils")
    writeObject(obj = dirTree, filename = s"${dirTree.base.local.get}/cfg.dirTree")
    writeObject(obj = projectStores, filename = s"${dirTree.base.local.get}/cfg.projectStores")
    writeObject(obj = arrayStores, filename = s"${dirTree.base.local.get}/cfg.arrayStores")
  
  }

}
