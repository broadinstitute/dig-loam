object Upload extends loamstream.LoamFile {

  /**
    * Upload Step
    *  Description: Upload input files to Google Cloud as needed
    *  Requires: NA
    */
  import ProjectConfig._
  import ProjectStores._
  import Fxns._
  
  def Upload(): Unit = {
  
    projectConfig.hailCloud match {
  
      case true =>
      
        checkURI(s"${projectStores.regionsExclude.google.get.uri}") match {
          case 0 => ()
          case 1 => 
            local {
              googleCopy(projectStores.regionsExclude.local.get, projectStores.regionsExclude.google.get)
            }
        }
  
        checkURI(s"${projectStores.kgPurcellVcf.google.get.uri}") match {
          case 0 => ()
          case 1 => 
            local {
              googleCopy(projectStores.kgPurcellVcf.local.get, projectStores.kgPurcellVcf.google.get)
            }
        }

        checkURI(s"${projectStores.dbSNPht.google.get.uri}") match {
          case 0 => ()
          case 1 => 
            local {
              //googleCopy(projectStores.dbSNPht.local.get, projectStores.dbSNPht.google.get)
              cmd"""${gsutilBinaryOpt.get} -m cp -r ${projectStores.dbSNPht.local.get} ${projectStores.dbSNPht.google.get}"""
              .in(projectStores.dbSNPht.local.get)
              .out(projectStores.dbSNPht.google.get)
              .tag(s"${projectStores.dbSNPht.local.get}.googleCopy".split("/").last)
            }
        }
        
        checkURI(s"${projectStores.kgSample.google.get.uri}") match {
          case 0 => ()
          case 1 => 
            local {
              googleCopy(projectStores.kgSample.local.get, projectStores.kgSample.google.get)
            }
        }
        
        checkURI(s"${projectStores.sampleFile.google.get.uri}") match {
          case 0 => ()
          case 1 => 
            local {
              googleCopy(projectStores.sampleFile.local.get, projectStores.sampleFile.google.get)
            }
        }
  
        projectStores.hailUtils.local match {
          case Some(r) =>
            projectStores.hailUtils.google match {
              case Some(s) =>
                local {
                  googleCopy(r, s)
                }
              case _ => ()
            }
          case _ => ()
        }
  
      case false => ()
  
    }
  
  }

}
