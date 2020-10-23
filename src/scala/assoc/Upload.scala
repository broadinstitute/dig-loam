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
  
      case false => ()
  
    }

    for {
      array <- projectConfig.Arrays.filter(e => usedArrays.contains(e.id))
    } yield {

        (projectConfig.hailCloud, array.qcHailCloud) match {

          case (true, false) =>

            val refMtGoogleDir = s"${arrayStores(array).refMt.google.get.toString.split("@")(1)}".split("/").dropRight(1).mkString("/")
            val refAnnotationHtGoogleDir = s"${arrayStores(array).refAnnotationsHt.google.get.toString.split("@")(1)}".split("/").dropRight(1).mkString("/")

            drm {
              cmd"""/humgen/diabetes/users/dig/loamstream/google-cloud-sdk/bin/gsutil -m cp -r ${arrayStores(array).refMtOrig.local.get} ${refMtGoogleDir}"""
                .in(arrayStores(array).refMtOrig.local.get)
                .out(arrayStores(array).refMt.google.get)
                .tag("refMtOrigLocal_to_refMtGoogle")
            }
            
            drm {
              cmd"""/humgen/diabetes/users/dig/loamstream/google-cloud-sdk/bin/gsutil -m cp -r ${arrayStores(array).refAnnotationsHtOrig.local.get} ${refAnnotationHtGoogleDir}"""
                .in(arrayStores(array).refAnnotationsHtOrig.local.get)
                .out(arrayStores(array).refAnnotationsHt.google.get)
                .tag("refAnnotationsHtOrigLocal_to_refAnnotationsHtGoogle")
            }

            // NOT WORKING googleCopy(arrayStores(array).refMtOrig.local.get, arrayStores(array).refMt.google.get, "-r")
            // NOT WORKING googleCopy(arrayStores(array).refAnnotationsHtOrig.local.get, arrayStores(array).refAnnotationsHt.google.get, "-r")

            googleCopy(arrayStores(array).variantsExclude.local.get, arrayStores(array).variantsExclude.google.get)

          case (false, true) => ()

            val refMtLocalDir = s"${arrayStores(array).refMt.local.get.toString.split("@")(1)}".split("/").dropRight(1).mkString("/")
            val refAnnotationHtLocalDir = s"${arrayStores(array).refAnnotationsHt.local.get.toString.split("@")(1)}".split("/").dropRight(1).mkString("/")

            drm {
              cmd"""/humgen/diabetes/users/dig/loamstream/google-cloud-sdk/bin/gsutil -m cp -r ${arrayStores(array).refMtOrig.google.get} ${refMtLocalDir}"""
                .in(arrayStores(array).refMtOrig.google.get)
                .out(arrayStores(array).refMt.local.get)
                .tag("refMtOrigGoogle_to_refMtLocal")
            }
            
            drm {
              cmd"""/humgen/diabetes/users/dig/loamstream/google-cloud-sdk/bin/gsutil -m cp -r ${arrayStores(array).refAnnotationsHtOrig.google.get} ${refAnnotationHtLocalDir}"""
                .in(arrayStores(array).refAnnotationsHtOrig.google.get)
                .out(arrayStores(array).refAnnotationsHt.local.get)
                .tag("refAnnotationsHtOrigGoogle_to_refAnnotationsHtLocal")
            }

            // NOT WORKING googleCopy(arrayStores(array).refMtOrig.google.get, arrayStores(array).refMt.local.get, "-r")
            // NOT WORKING googleCopy(arrayStores(array).refAnnotationsHtOrig.google.get, arrayStores(array).refAnnotationsHt.local.get, "-r")

          case _ => ()

        }

    }
  
  }

}
