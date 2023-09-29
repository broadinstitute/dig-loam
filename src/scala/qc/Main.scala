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
      
    Prepare(array)
    Harmonize(array)
  
  }
  
  for {
    array <- projectConfig.Arrays
  } yield {
  
    Load(array)
    ExportQcData(array)
    Annotate(array)
    Kinship(array)
    AncestryPca(array)
    AncestryCluster(array)
  
  }
  
  // Reconcile inferred ancestry
  MergeInferredAncestry()
  
  // Array specific QC steps post ancestry inference
  for {
    array <- projectConfig.Arrays
  } yield { 
  
    Pca(array)
    SampleQc(array)
    FilterArray(array)
    ExportGenotypes(array, filter=false)

	array.exportFiltered match {
      case true =>
        ExportGenotypes(array, filter=true)
      case _ => ()
    }

  }
  
  // QC Report
  QcReport()
  
  //// Generate imputation ready data files
  //for {
  //  array <- projectConfig.Arrays
  //} yield { 
  //
  //  FilterImpute(array.id)
  //
  //}

}
