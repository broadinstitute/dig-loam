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

        checkURI(s"${projectStores.sampleFile.google.get.uri}") match {
          case 0 => ()
          case 1 => 
            local {
              googleCopy(projectStores.sampleFile.local.get, projectStores.sampleFile.google.get)
            }
        }
  
        checkURI(s"${projectStores.geneIdMap.google.get.uri}") match {
          case 0 => ()
          case 1 => 
            local {
              googleCopy(projectStores.geneIdMap.local.get, projectStores.geneIdMap.google.get)
            }
        }
  
        projectStores.phenoFile.local match {
          case Some(r) =>
            projectStores.phenoFile.google match {
              case Some(s) =>
                checkURI(s"${s.uri}") match {
                  case 0 => ()
                  case 1 => 
                    local {
                      googleCopy(r, s)
                    }
                }
              case _ => ()
            }
          case _ => ()
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
