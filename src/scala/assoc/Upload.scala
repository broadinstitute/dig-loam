object Upload extends loamstream.LoamFile {

  /**
    * Upload Step
    *  Description: Upload input files to Google Cloud as needed
    *  Requires: NA
    */
  import ProjectConfig._
  import ProjectStores._
  import ArrayStores._
  import Fxns._
  import Collections._
  
  def Upload(): Unit = {

    val gsutilBinaryOpt: Option[Path] = projectContext.config.googleConfig.map(_.gsutilBinary)
    require(gsutilBinaryOpt.isDefined, "Couldn't find gsutil binary path; set loamstream.googlecloud.gsutilBinary in loamstream.conf")
  
    projectConfig.hailCloud match {
  
      case true =>

        ifURI(s"${projectStores.geneIdMap.google.get.uri}") match {
          case 0 => ()
          case 1 => 
            local {
              googleCopy(projectStores.geneIdMap.local.get, projectStores.geneIdMap.google.get)
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

        for {
           a <- projectConfig.annotationTables
        } yield {
          projectStores.annotationStores(a).local match {
            case Some(r) =>
              projectStores.annotationStores(a).google match {
                case Some(s) =>
                  ifURI(s"${s.uri}") match {
                    case 0 => ()
                    case 1 => 
                      local {
                        local {
                          //googleCopy(r, s)
                          cmd"""${gsutilBinaryOpt.get} -m cp -r ${r} ${s}"""
                          .in(r)
                          .out(s)
                          .tag(s"${r}.googleCopy".split("/").last)
                        }
                      }
                  }
                case _ => ()
              }
            case _ => ()
          }
        }
  
      case false => ()
  
    }

    for {
      array <- projectConfig.Arrays.filter(e => usedArrays.contains(e.id))
    } yield {

      projectConfig.hailCloud match {
        case true =>
          local {
            googleCopy(arrayStores(array).phenoFile.local.get, arrayStores(array).phenoFile.google.get)
          }
        case false => ()
      }

      (projectConfig.hailCloud, array.mt) match {

        case (true, Some(_)) =>

          val mtGoogleDir = s"${arrayStores(array).mt.get.google.get.toString.split("@")(1)}".split("/").dropRight(1).mkString("/")
          val annotationHtGoogleDir = s"${arrayStores(array).annotationsHt.google.get.toString.split("@")(1)}".split("/").dropRight(1).mkString("/")

          drm {
            cmd"""${gsutilBinaryOpt.get} -m cp -r ${arrayStores(array).mt.get.local.get} ${mtGoogleDir}"""
              .in(arrayStores(array).mt.get.local.get)
              .out(arrayStores(array).mt.get.google.get)
              .tag(s"mtLocal_to_mtGoogle.${array.id}")
          }
          
          drm {
            cmd"""${gsutilBinaryOpt.get} -m cp -r ${arrayStores(array).annotationsHt.local.get} ${annotationHtGoogleDir}"""
              .in(arrayStores(array).annotationsHt.local.get)
              .out(arrayStores(array).annotationsHt.google.get)
              .tag(s"annotationsHtLocal_to_annotationsHtGoogle.${array.id}")
          }

          googleCopy(arrayStores(array).variantsExclude.local.get, arrayStores(array).variantsExclude.google.get)
          googleCopy(arrayStores(array).samplesExclude.local.get, arrayStores(array).samplesExclude.google.get)

        case _ => ()

      }

    }
  
  }

}
