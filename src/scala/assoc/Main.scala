object Main extends loamstream.LoamFile {

  import Collections._
  import CrossCohort._
  import PrepareModel._
  import PrepareSchema._
  import ProjectConfig._
  import ProjectStores._
  import Tracking._
  import Upload._
  import AssocTest._
  
  import loamstream.conf.DataConfig
  import loamstream.googlecloud.HailSupport._
  import loamstream.model.Store
  import loamstream.util.CanBeClosed.enclosed
  
  // write pipeline object tracking files
  trackObjects()
  
  // Upload input files to Google Cloud
  Upload()
  
  // Meta-analysis cross-cohort common variants search
  for {
    meta <- projectConfig.Metas
  } yield {
  
    if (List("all","prepareSchema").contains(projectConfig.step)) CrossCohortCommonVars(configMeta = meta)
  
  }
  
  // Meta-analysis cross-cohort qc prep
  for {
    meta <- projectConfig.Metas
    cohort <- projectConfig.Cohorts.filter(e => meta.cohorts.contains(e.id))
  } yield {
  
    if (List("all","prepareSchema").contains(projectConfig.step)) CrossCohortPrep(configMeta = meta, configCohort = cohort)
  
  }
  
  // Meta-analysis cross-array kinship
  for {
    meta <- projectConfig.Metas
  } yield {
  
    if (List("all","prepareSchema").contains(projectConfig.step)) CrossCohortKinship(meta)
  
  }
  
  // Prepare schema cohorts
  for {
    x <- schemaCohorts
  } yield {
  
    if (List("all","prepareSchema").contains(projectConfig.step)) PrepareSchema(configSchema = x.schema, configCohorts = x.cohorts)
  
  }
  
  // Prepare model cohorts
  for {
    x <- modelCollections
  } yield {
  
    if (List("all","prepareModel").contains(projectConfig.step)) PrepareModel(configModel = x.model, configSchema = x.schema, configCohorts = x.cohorts, configMeta = None)
  
  }
  
  //Cohort variant association
  for {
    x <- modelCollections if ! x.model.tests.isEmpty
  } yield {
  
    if (List("all","assocTest").contains(projectConfig.step)) AssocTest(configModel = x.model, configSchema = x.schema, configCohorts = x.cohorts, configMeta = None)
  
  }
  
  //// Prepare meta cohort model
  //for {
  //  x <- modelMetaCollections 
  //} yield {
  //
  //  PrepareModel(configModel = x.model, configSchema = x.schema, configCohorts = x.cohorts, configMeta = Some(x.meta))
  //
  //}

  //// Cohort variant association for known loci
  //for {
  //  x <- modelCohortKnowns
  //} yield {
  //
  //  VariantAssoc(configModel = x.model, configCohorts = x.cohorts, configMeta = None, configKnown = Some(x.known))
  //
  //}
  //
  // Meta-analysis specific cohort variant association
  //for {
  //  x <- modelMetaCohorts
  //} yield { 
  //
  //  VariantAssoc(configCohorts = x.cohorts, configModel = x.model, configMeta = Some(x.meta), configKnown = None)
  //
  //}
  //
  //// Meta-analysis specific cohort variant association for known loci
  //for {
  //  x <- modelMetaCohortKnowns
  //} yield { 
  //
  //  VariantAssoc(configCohorts = x.cohorts, configModel = x.model, configMeta = Some(x.meta), configKnown = Some(x.known))
  //
  //}
  
  //// Copy known results to Google Cloud
  //for {
  //  x <- projectStores.knownStores.keys
  //} yield {
  //
  //  KnownLociToGoogle(configKnown = x)
  //
  //}
  //
  //// Meta-analysis specific cohort variant association for known loci
  //for {
  //  x <- modelCohortMetaKnowns
  //} yield { 
  //
  //  KnownLociAssoc(configModel = x.model, configCohorts = x.cohorts, configKnown = x.known, configMeta = Some(x.meta))
  //
  //}
  //
  //// Meta-analysis
  //for {
  //  x <- modelMetas
  //} yield {
  //
  //  MetaAnalysis(configModel = x.model, configMeta = x.meta)
  //
  //}
  //
  //// Meta-analysis for known loci
  //for {
  //  x <- modelMetaKnowns
  //} yield {
  //
  //  MetaAnalysisKnownLoci(configModel = x.model, configMeta = x.meta, configKnown = x.known)
  //
  //}
  //
  //// Merge results
  //for {
  //  x <- modelMerges
  //} yield {
  //
  //  MergeAssoc(configModel = x.model, configMerge = x.merge)
  //
  //}
  //
  //// Merge known assoc results
  //for {
  //  x <- modelMergeKnowns
  //} yield {
  //
  //  MergeKnownAssoc(configModel = x.model, configMerge = x.merge, configKnown = x.known)
  //
  //}
  //
  //// Cohort Results Summary
  //for {
  //  x <- modelCohortsReport
  //} yield {
  //
  //  ResultsSummary(configModel = x.model, configCohort = Some(x.cohort), configMeta = None, configMerge = None)
  //
  //}
  //
  //// Meta Results Summary
  //for {
  //  x <- modelMetasReport
  //} yield { 
  //
  //  ResultsSummary(configModel = x.model, configCohort = None, configMeta = Some(x.meta), configMerge = None)
  //
  //}
  //
  //// Merge Results Summary
  //for {
  //  x <- modelMerges
  //} yield { 
  //
  //  ResultsSummary(configModel = x.model, configCohort = None, configMeta = None, configMerge = Some(x.merge))
  //
  //}
  //
  //// Cohort Known Loci Results Summary
  //for {
  //  x <- modelCohortKnownsReport
  //} yield { 
  //
  //  ResultsKnownLociSummary(configModel = x.model, configCohort = Some(x.cohort), configMeta = None, configMerge = None, configKnown = x.known)
  //
  //}
  //
  //// Meta Known Loci Results Summary
  //for {
  //  x <- modelMetaKnownsReport
  //} yield { 
  //
  //  ResultsKnownLociSummary(configModel = x.model, configCohort = None, configMeta = Some(x.meta), configMerge = None, configKnown = x.known)
  //
  //}
  //
  //// Merge Known Loci Results Summary
  //for {
  //  x <- modelMergeKnowns
  //} yield { 
  //
  //  ResultsKnownLociSummary(configModel = x.model, configCohort = None, configMeta = None, configMerge = Some(x.merge), configKnown = x.known)
  //
  //}
  //
  //// Generate Phenotype Figures for Cohorts
  //for {
  //  x <- phenoCohorts
  //} yield { 
  //
  //  PhenotypeDistPlots(configPheno = x.pheno, configCohort = x.cohort, configMeta = None)
  //
  //}
  //
  //// Generate Phenotype Figures for Meta Cohorts
  //for {
  //  x <- phenoCohortMetas
  //} yield { 
  //
  //  PhenotypeDistPlots(configPheno = x.pheno, configCohort = x.cohort, configMeta = Some(x.meta))
  //
  //}
  //
  //// Analysis Report Global
  //for {
  //  report <- projectConfig.Reports
  //} yield {
  //
  //  AnalysisReportGlobal(configReport = report)
  //
  //}
  //
  //// Analysis Report Phenotypes
  //for {
  //  report <- projectConfig.Reports
  //  section <- report.sections
  //  pheno <- projectConfig.Models.filter(e => section.models contains e.id).map(e => projectConfig.Phenos.filter(f => f.id == e.pheno)).flatten.distinct
  //} yield {
  //
  //  AnalysisReportPheno(configReport = report, configSection = section, configPheno = pheno)
  //
  //}
  //
  //// Analysis Report Compile
  //for {
  //  report <- projectConfig.Reports
  //} yield {
  //
  //AnalysisReportCompile(configReport = report)
  //
  //}

}
