object Main extends loamstream.LoamFile {

  import Ancestry._
  import Annotate._
  import ArrayStores._
  import ExportGenotypes._
  import ExportQcData._
  import FilterArray._
  import Harmonize._
  import Kinship._
  import Load._
  import Pca._
  import Prepare._
  import ProjectConfig._
  import ProjectStores._
  import SampleQc._
  import Tracking._
  import Upload._
  import QcReport._
  
  import loamstream.conf.DataConfig
  import loamstream.googlecloud.HailSupport._
  import loamstream.model.Store
  import loamstream.util.CanBeClosed.enclosed

  

  // write pipeline object tracking files
  trackObjects()
  
  // Upload input files to Google Cloud
  Upload()
  
  // Array specific QC steps up to ancestry inferrence
  for {
    array <- projectConfig.Arrays if (array.technology == "gwas") && (array.format != "mt")
  } yield {
      
    if (List("all","load").contains(projectConfig.step)) Prepare(array)
    if (List("all","load").contains(projectConfig.step)) Harmonize(array)
  
  }
  
  for {
    array <- projectConfig.Arrays
  } yield {

    if (List("all","load").contains(projectConfig.step)) Load(array)
    if (List("all","exportQc").contains(projectConfig.step)) ExportQcData(array)
    if (List("all","annotate").contains(projectConfig.step)) Annotate(array)
    if (List("all","kinship").contains(projectConfig.step)) Kinship(array)
    if (List("all","ancestry").contains(projectConfig.step)) AncestryPca(array)
    if (List("all","ancestry").contains(projectConfig.step)) AncestryGmm(array)
    if (List("all","ancestry").contains(projectConfig.step)) AncestryKnn(array)
  
  }
  
  // Reconcile inferred ancestry
  if (List("all","ancestry").contains(projectConfig.step)) MergeInferredAncestryGmm()
  if (List("all","ancestry").contains(projectConfig.step)) MergeInferredAncestryKnn()
  
  // Array specific QC steps post ancestry inference
  for {
    array <- projectConfig.Arrays
  } yield { 
  
    if (List("all","pca").contains(projectConfig.step)) Pca(array)
    if (List("all","sampleQc").contains(projectConfig.step)) SampleQc(array)
    if (List("all","filter").contains(projectConfig.step)) FilterArray(array)
    if (List("all","exportFinal").contains(projectConfig.step)) ExportGenotypes(array, filter=false, alignBgenMaf=array.exportBgenAlignedMinor)

	array.exportFiltered match {
      case true =>
        if (List("all","exportFinal").contains(projectConfig.step)) ExportGenotypes(array, filter=true, alignBgenMaf=array.exportBgenAlignedMinor)
      case _ => ()
    }

  }
  
  // QC Report
  if (List("all","report").contains(projectConfig.step)) QcReport()
  
  //// Generate imputation ready data files
  //for {
  //  array <- projectConfig.Arrays
  //} yield { 
  //
  //  FilterImpute(array.id)
  //
  //}

}
