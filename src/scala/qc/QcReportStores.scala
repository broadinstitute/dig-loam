object QcReportStores extends loamstream.LoamFile {

  import ProjectConfig._
  import StoreHelpers._
  import Stores._
  import Fxns._
  import DirTree._
  
  final case class TablesData(
    rawVariantsSummary: Store,
    seqVariantsSummary: Store,
    variantsExcludeSummary: Store,
    clustersGmm: Store,
    ancestryGmm: Store,
    ancestryKnn: Store,
    sampleQc: Store)
  
  final case class TexData(
    header: Store,
    intro: Store,
    data: Store,
    ancestry: Store,
    ibdSexcheck: Store,
    sampleQc: Store,
    variantQc: Store,
    bibliography: Store)
  
  final case class FigureData(
    samplesUpsetPlotPdf: Option[Store],
    variantsUpsetPlotPdf: Option[Store],
    metricDistUnadjPdf: Store,
    metricDistAdjPdf: Store,
    samplesRemainingUpsetPlotPdf: Store,
    variantsRemainingUpsetPlotPdf: Store)
  
  final case class QcReport(
    tablesData: TablesData,
    texData: TexData,
    figureData: FigureData,
    tex: Store,
    pdf: Store)
  
  val qcReportStores = QcReport(
  
    tablesData = TablesData(
  
      rawVariantsSummary = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.raw_variants.summary.tbl"),
      seqVariantsSummary = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.seq_variants.summary.tbl"),
      variantsExcludeSummary = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.variants_exclude.summary.tbl"),
      clustersGmm = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.clusters.gmm.tbl"),
      ancestryGmm = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.ancestry.gmm.tbl"),
      ancestryKnn = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.ancestry.knn.tbl"),
      sampleQc = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.sampleqc.tbl")),
  
    texData = TexData(
      header = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.header.tex"),
      intro = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.intro.tex"),
      data = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.data.tex"),
      ancestry = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.ancestry.tex"),
      ibdSexcheck = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.ibd_sexcheck.tex"),
      sampleQc = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.sampleqc.tex"),
      variantQc = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.variantqc.tex"),
      bibliography = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.bibliography.tex")),
  
    figureData = FigureData(
      samplesUpsetPlotPdf = if (projectConfig.nArrays > 1) { Some(store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.samples.upsetplot.pdf")) } else { None },
      variantsUpsetPlotPdf = if (projectConfig.nArrays > 1) { Some(store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.variants.upsetplot.pdf")) } else { None },
      metricDistUnadjPdf = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.metric_dist_unadj.pdf"),
      metricDistAdjPdf = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.metric_dist_adj.pdf"),
      samplesRemainingUpsetPlotPdf = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.samples_remaining.upsetplot.pdf"),
      variantsRemainingUpsetPlotPdf = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.variants_remaining.upsetplot.pdf")),
  
    tex = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.tex"),
    pdf = store(dirTree.reportQc.local.get / s"${projectConfig.projectId}.qc_report.pdf")
  
  )

}