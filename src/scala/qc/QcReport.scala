object QcReport extends loamstream.LoamFile {

  /**
    * QC Report Step
    *  Description: Generate QC Report
    *  Requires: R-3.4, Python, convert, pdflatex
    */
  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  import QcReportStores._
  import DirTree._
  import Fxns._
  
  def QcReport(): Unit = {

    projectConfig.Arrays.filter(e => (e.technology == "gwas") && (e.format != "mt")).size match {

      case n if n > 0 =>

        val freqStrings = { for { a <- projectConfig.Arrays if (a.technology == "gwas") && (a.format != "mt") } yield { Seq(a.id, s"""${arrayStores(a).rawData.freq.get.path}""").mkString(",") } }
        val indelStrings = { for { a <- projectConfig.Arrays if (a.technology == "gwas") && (a.format != "mt") } yield { Seq(a.id, s"""${arrayStores(a).rawData.indel.get.path}""").mkString(",") } }
        val multiStrings = { for { a <- projectConfig.Arrays if (a.technology == "gwas") && (a.format != "mt") } yield { Seq(a.id, s"""${arrayStores(a).preparedData.get.multiallelic.path}""").mkString(",") } }
        val dupVarsRemoveStrings = { for { a <- projectConfig.Arrays if (a.technology == "gwas") && (a.format != "mt") } yield { Seq(a.id, s"""${arrayStores(a).rawData.dupVarsRemove.get.path}""").mkString(",") } }

        val freqInclude = { for { a <- projectConfig.Arrays if (a.technology == "gwas") && (a.format != "mt") } yield { arrayStores(a).rawData.freq.get } }
        val indelInclude = { for { a <- projectConfig.Arrays if (a.technology == "gwas") && (a.format != "mt") } yield { arrayStores(a).rawData.indel.get } }
        val multiInclude = { for { a <- projectConfig.Arrays if (a.technology == "gwas") && (a.format != "mt") } yield { arrayStores(a).preparedData.get.multiallelic } }
        val dupVarsRemoveInclude = { for { a <- projectConfig.Arrays if (a.technology == "gwas") && (a.format != "mt") } yield { arrayStores(a).rawData.dupVarsRemove.get } }
        
        drmWith(imageName = s"${utils.image.imgR}") {
        
          cmd"""${utils.binary.binRscript} --vanilla --verbose
            ${utils.r.rRawVariantsSummaryTable}
            --freq-in ${freqStrings.mkString(" ")}
            --indel-in ${indelStrings.mkString(" ")}
            --multi-in ${multiStrings.mkString(" ")}
            --dupl-in ${dupVarsRemoveStrings.mkString(" ")}
            --out ${qcReportStores.tablesData.rawVariantsSummary}"""
            .in(freqInclude ++ indelInclude ++ multiInclude ++ dupVarsRemoveInclude)
            .out(qcReportStores.tablesData.rawVariantsSummary)
            .tag(s"${qcReportStores.tablesData.rawVariantsSummary}.r".split("/").last)

        }

      case _ =>

        drmWith(imageName = s"${utils.image.imgTools}") {
        
          cmd"""touch ${qcReportStores.tablesData.rawVariantsSummary}"""
            .out(qcReportStores.tablesData.rawVariantsSummary)
            .tag(s"${qcReportStores.tablesData.rawVariantsSummary}.touch".split("/").last)

        }

    }

    projectConfig.Arrays.filter(e => (e.technology != "gwas") || (e.format == "mt")).size match {

      case n if n > 0 =>

        val seqStatsStrings = { for { a <- projectConfig.Arrays if (a.technology != "gwas") || (a.format == "mt") } yield { Seq(a.id, s"""${arrayStores(a).refData.variantMetrics.local.get.path}""").mkString(",") } }

        drmWith(imageName = s"${utils.image.imgR}") {
        
          cmd"""${utils.binary.binRscript} --vanilla --verbose
            ${utils.r.rSeqVariantsSummaryTable}
            --stats-in ${seqStatsStrings.mkString(" ")}
            --out ${qcReportStores.tablesData.seqVariantsSummary}"""
            .in(arrayStores.map(e => e._2).map(e => e.refData.variantMetrics.local).flatten.toSeq)
            .out(qcReportStores.tablesData.seqVariantsSummary)
            .tag(s"${qcReportStores.tablesData.seqVariantsSummary}.r".split("/").last)

        }

      case _ =>

        drmWith(imageName = s"${utils.image.imgTools}") {
        
          cmd"""touch ${qcReportStores.tablesData.seqVariantsSummary}"""
            .out(qcReportStores.tablesData.seqVariantsSummary)
            .tag(s"${qcReportStores.tablesData.seqVariantsSummary}.touch".split("/").last)

        }

    }

    val statsStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).filterPostQc.variantsStats.local.get.path}""").mkString(",") } }

    drmWith(imageName = s"${utils.image.imgR}") {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rVariantsExcludeSummaryTable}
        --stats ${statsStrings.mkString(" ")}
        --out ${qcReportStores.tablesData.variantsExcludeSummary}"""
        .in(arrayStores.map(e => e._2).map(e => e.filterPostQc.variantsStats.local).flatten.toSeq)
        .out(qcReportStores.tablesData.variantsExcludeSummary)
        .tag(s"${qcReportStores.tablesData.variantsExcludeSummary}.r".split("/").last)

    }

    val clusterGroupsStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).ancestryClusterData.groups.path}""").mkString(",") } }
    val ancestryInferredStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).ancestryData.inferred.path}""").mkString(",") } }
    
    drmWith(imageName = s"${utils.image.imgR}") {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rAncestryClusterTable}
        --cluster-in ${clusterGroupsStrings.mkString(" ")}
        --ancestry-in ${ancestryInferredStrings.mkString(" ")}
        --final-in ${projectStores.ancestryInferred.local.get}
        --cluster-out ${qcReportStores.tablesData.clusters}
        --final-out ${qcReportStores.tablesData.ancestry}"""
        .in(arrayStores.map(e => e._2).map(e => e.ancestryClusterData.groups).toSeq ++ arrayStores.map(e => e._2).map(e => e.ancestryData.inferred).toSeq :+ projectStores.ancestryInferred.local.get)
        .out(qcReportStores.tablesData.clusters, qcReportStores.tablesData.ancestry)
        .tag(s"${qcReportStores.tablesData.ancestry}".split("/").last)
    
    }
    
    val dataStrings = { 
      for {
        a <- projectConfig.Arrays
      } yield {
        Seq(a.technology, a.format, s"""${arrayStores(a).refData.sampleList.local.get.path}""").mkString(",")
      }
    }

    val dataIn = { 
      for {
        a <- projectConfig.Arrays
      } yield {
        arrayStores(a).refData.sampleList.local.get
      }
    }

    drmWith(imageName = s"${utils.image.imgPython2}") {
    
  	  cmd"""${utils.binary.binPython} ${utils.python.pyGenerateReportHeader}
        --out ${qcReportStores.texData.header}"""
        .out(qcReportStores.texData.header)
        .tag(s"${qcReportStores.texData.header}".split("/").last)
      
      cmd"""${utils.binary.binPython} ${utils.python.pyGenerateQcReportIntro}
        --id ${projectConfig.projectId}
        --authors "${projectConfig.authors.mkString(",")}"
        --organization "${projectConfig.organization}"
        --email "${projectConfig.email}"
        --out ${qcReportStores.texData.intro} 
        --array-data ${dataStrings.mkString(" ")}"""
        .in(dataIn)
        .out(qcReportStores.texData.intro)
        .tag(s"${qcReportStores.texData.intro}".split("/").last)
    
    }
    
    val sampleStrings = {
      for {
        a <- projectConfig.Arrays
      } yield {
        Seq(a.id, s"""${arrayStores(a).refData.sampleList.local.get.path}""").mkString(",")
      }
    }
    
    val varStrings = {
      for {
        a <- projectConfig.Arrays
      } yield {
        Seq(a.id, s"""${arrayStores(a).refData.varList.local.get.path}""").mkString(",")
      }
    }

    val imissRemoveStrings = { for { a <- projectConfig.Arrays if (a.technology == "gwas") && (a.format != "mt") } yield { Seq(a.id, s"""${arrayStores(a).rawData.imissRemove.get.path}""").mkString(",") } }
    val imissRemoveCmdStrings = imissRemoveStrings.size match {
      case n if n > 0 => s"""--imiss ${imissRemoveStrings.mkString(" ")}"""
      case _ => ""
    }
    val imissRemoveFiles = { for { a <- projectConfig.Arrays if (a.technology == "gwas") && (a.format != "mt") } yield { arrayStores(a).rawData.imissRemove.get } }.toSeq

    val sampleIn = for {
      a <- projectConfig.Arrays
    } yield {
      arrayStores(a).refData.sampleList.local.get
    }
    
    val variantIn = for {
      a <- projectConfig.Arrays
    } yield {
      arrayStores(a).refData.varList.local.get
    }
    
    if (projectConfig.nArrays > 1) {
    
      drmWith(imageName = s"${utils.image.imgR}") {
    
        cmd"""${utils.binary.binRscript} --vanilla --verbose
          ${utils.r.rUpsetplotVariantSample}
          --input ${sampleStrings.mkString(" ")}
          --type sample
          --out ${qcReportStores.figureData.samplesUpsetPlotPdf.get}"""
          .in(sampleIn)
          .out(qcReportStores.figureData.samplesUpsetPlotPdf.get)
          .tag(s"${qcReportStores.figureData.samplesUpsetPlotPdf.get}".split("/").last)
        
        cmd"""${utils.binary.binRscript} --vanilla --verbose
          ${utils.r.rUpsetplotVariantSample}
          --input ${varStrings.mkString(" ")}
          --type variant
          --out ${qcReportStores.figureData.variantsUpsetPlotPdf.get}"""
          .in(variantIn)
          .out(qcReportStores.figureData.variantsUpsetPlotPdf.get)
          .tag(s"${qcReportStores.figureData.variantsUpsetPlotPdf.get}".split("/").last)
    
      }
    
      drmWith(imageName = s"${utils.image.imgPython2}") {
        
        cmd"""${utils.binary.binPython} ${utils.python.pyGenerateQcReportData}
          --narrays ${projectConfig.nArrays}
          ${imissRemoveCmdStrings}
          --samples-upset-diagram ${qcReportStores.figureData.samplesUpsetPlotPdf.get.path.toAbsolutePath()}
          --geno-variants-summary-table ${qcReportStores.tablesData.rawVariantsSummary.path.toAbsolutePath()} 
          --seq-variants-summary-table ${qcReportStores.tablesData.seqVariantsSummary.path.toAbsolutePath()} 
          --variants-upset-diagram ${qcReportStores.figureData.variantsUpsetPlotPdf.get.path.toAbsolutePath()} 
          --out ${qcReportStores.texData.data}"""
          .in(imissRemoveFiles :+ qcReportStores.figureData.samplesUpsetPlotPdf.get :+ qcReportStores.tablesData.rawVariantsSummary :+ qcReportStores.tablesData.seqVariantsSummary :+ qcReportStores.figureData.variantsUpsetPlotPdf.get)
          .out(qcReportStores.texData.data)
          .tag(s"${qcReportStores.texData.data}".split("/").last)
      
      }
    
    } else {
    
      drmWith(imageName = s"${utils.image.imgPython2}") {
    
        cmd"""${utils.binary.binPython} ${utils.python.pyGenerateQcReportData}
        --narrays ${projectConfig.nArrays}
        ${imissRemoveCmdStrings}
        --fam ${sampleStrings(0)}
        --geno-variants-summary-table ${qcReportStores.tablesData.rawVariantsSummary.path.toAbsolutePath()} 
        --seq-variants-summary-table ${qcReportStores.tablesData.seqVariantsSummary.path.toAbsolutePath()} 
        --bim ${varStrings.mkString(" ")}
        --out ${qcReportStores.texData.data}"""
        .in(arrayStores.map(e => e._2).map(e => e.rawData.imissRemove).flatten.toSeq ++ sampleIn ++ variantIn :+ qcReportStores.tablesData.rawVariantsSummary :+ qcReportStores.tablesData.seqVariantsSummary)
        .out(qcReportStores.texData.data)
        .tag(s"${qcReportStores.texData.data}".split("/").last)
    
      }
    
    }

    for {
      a <- projectConfig.Arrays
    } yield {

      drmWith(imageName = s"${utils.image.imgImagemagick}") {

        cmd"""${utils.binary.binConvert} -density 300 ${arrayStores(a).ancestryPcaData.plots}[0] ${arrayStores(a).ancestryPcaData.plotsPc1Pc2Png}"""
          .in(arrayStores(a).ancestryPcaData.plots)
          .out(arrayStores(a).ancestryPcaData.plotsPc1Pc2Png)
          .tag(s"${arrayStores(a).ancestryPcaData.plotsPc1Pc2Png}".split("/").last)

        cmd"""${utils.binary.binConvert} -density 300 ${arrayStores(a).ancestryPcaData.plots}[1] ${arrayStores(a).ancestryPcaData.plotsPc2Pc3Png}"""
          .in(arrayStores(a).ancestryPcaData.plots)
          .out(arrayStores(a).ancestryPcaData.plotsPc2Pc3Png)
          .tag(s"${arrayStores(a).ancestryPcaData.plotsPc2Pc3Png}".split("/").last)

        cmd"""${utils.binary.binConvert} -density 300 ${arrayStores(a).ancestryClusterData.plots}[0] ${arrayStores(a).ancestryClusterData.plotsPc1Pc2Png}"""
          .in(arrayStores(a).ancestryClusterData.plots)
          .out(arrayStores(a).ancestryClusterData.plotsPc1Pc2Png)
          .tag(s"${arrayStores(a).ancestryClusterData.plotsPc1Pc2Png}".split("/").last)

        cmd"""${utils.binary.binConvert} -density 300 ${arrayStores(a).ancestryClusterData.plots}[1] ${arrayStores(a).ancestryClusterData.plotsPc2Pc3Png}"""
          .in(arrayStores(a).ancestryClusterData.plots)
          .out(arrayStores(a).ancestryClusterData.plotsPc2Pc3Png)
          .tag(s"${arrayStores(a).ancestryClusterData.plotsPc2Pc3Png}".split("/").last)

      }

    }
    
    val ref1kgBimStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"${arrayStores(a).ref1kgData.plink.base.local.get}.bim").mkString(",") } }
    val ancestryPcaPlotsStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).ancestryPcaData.plotsPc1Pc2Png.path.toAbsolutePath()}""", s"""${arrayStores(a).ancestryPcaData.plotsPc2Pc3Png.path.toAbsolutePath()}""").mkString(",") } }
    val ancestryClusterPlotsStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).ancestryClusterData.plotsPc1Pc2Png.path.toAbsolutePath()}""", s"""${arrayStores(a).ancestryClusterData.plotsPc2Pc3Png.path.toAbsolutePath()}""").mkString(",") } }
    val restoreStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).filterQc.samplesRestore.path}""").mkString(",") } }
    
    drmWith(imageName = s"${utils.image.imgPython2}") {
    
      cmd"""${utils.binary.binPython} ${utils.python.pyGenerateQcReportAncestry}
        --kg-merged-bim ${ref1kgBimStrings.mkString(" ")}
        --features ${projectConfig.nAncestryInferenceFeatures}
        --pca-plots ${ancestryPcaPlotsStrings.mkString(" ")}
        --cluster-plots ${ancestryClusterPlotsStrings.mkString(" ")}
        --cluster-table ${qcReportStores.tablesData.clusters.path.toAbsolutePath()}
        --final-table ${qcReportStores.tablesData.ancestry.path.toAbsolutePath()}
        --restore ${restoreStrings.mkString(" ")}
        --out ${qcReportStores.texData.ancestry}"""
        .in(arrayStores.map(e => e._2).map(e => e.ref1kgData.plink.data.local.get).flatten.toSeq ++ arrayStores.map(e => e._2).map(e => e.ancestryPcaData.plotsPc1Pc2Png).toSeq ++ arrayStores.map(e => e._2).map(e => e.ancestryPcaData.plotsPc2Pc3Png).toSeq ++ arrayStores.map(e => e._2).map(e => e.ancestryClusterData.plotsPc1Pc2Png).toSeq ++ arrayStores.map(e => e._2).map(e => e.ancestryClusterData.plotsPc2Pc3Png).toSeq ++ arrayStores.map(e => e._2).map(e => e.filterQc.samplesRestore).toSeq :+ qcReportStores.tablesData.clusters :+ qcReportStores.tablesData.ancestry)
        .out(qcReportStores.texData.ancestry)
        .tag(s"${qcReportStores.texData.ancestry}".split("/").last)
    
    }
    
    val refPrunedBimStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"${arrayStores(a).prunedData.plink.base}.bim").mkString(",") } }
    val kin0Strings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).kinshipData.kin0.path}""").mkString(",") } }
    val famSizesStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).kinshipData.famSizes.path}""").mkString(",") } }
    val sexcheckProblemsStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).sexcheckData.problems.local.get.path}""").mkString(",") } }
    
    drmWith(imageName = s"${utils.image.imgPython2}") {
      
      cmd"""${utils.binary.binPython} ${utils.python.pyGenerateQcReportIbdSexcheck}
        --filtered-bim ${refPrunedBimStrings.mkString(" ")}
        --kin0-related ${kin0Strings.mkString(" ")}
        --famsizes ${famSizesStrings.mkString(" ")}
        --sexcheck-problems ${sexcheckProblemsStrings.mkString(" ")}
        --restore ${restoreStrings.mkString(" ")}
        --out ${qcReportStores.texData.ibdSexcheck}"""
        .in(arrayStores.map(e => e._2).map(e => e.prunedData.plink.data).flatten.toSeq ++ arrayStores.map(e => e._2).map(e => e.kinshipData.kin0).toSeq ++ arrayStores.map(e => e._2).map(e => e.kinshipData.famSizes).toSeq ++ arrayStores.map(e => e._2).map(e => e.sexcheckData.problems.local.get).toSeq ++ arrayStores.map(e => e._2).map(e => e.filterQc.samplesRestore).toSeq)
        .out(qcReportStores.texData.ibdSexcheck)
        .tag(s"${qcReportStores.texData.ibdSexcheck}".split("/").last)
    
    }
    
    drmWith(imageName = s"${utils.image.imgR}") {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rMakeMetricDistPlot}
        --sampleqc ${arrayStores(projectConfig.Arrays.head).sampleQcData.stats.local.get}
        --metric ${projectConfig.Arrays.head.sampleQcMetrics.head}
        --out ${qcReportStores.figureData.metricDistUnadjPdf}
        """
        .in(arrayStores(projectConfig.Arrays.head).sampleQcData.stats.local.get)
        .out(qcReportStores.figureData.metricDistUnadjPdf)
        .tag(s"${qcReportStores.figureData.metricDistUnadjPdf}".split("/").last)
      
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rMakeMetricDistPlot}
        --sampleqc ${arrayStores(projectConfig.Arrays.head).sampleQcData.statsAdj}
        --metric ${projectConfig.Arrays.head.sampleQcMetrics.head}_res
        --out ${qcReportStores.figureData.metricDistAdjPdf}
        """
        .in(arrayStores(projectConfig.Arrays.head).sampleQcData.statsAdj)
        .out(qcReportStores.figureData.metricDistAdjPdf)
        .tag(s"${qcReportStores.figureData.metricDistAdjPdf}".split("/").last)
    
    }
    
    val sampleQcOutliersStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).sampleQcData.outliers.path}""").mkString(",") } }
    val finalSampleExclusionsStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).filterQc.samplesExclude.local.get.path}""").mkString(",") } }

    drmWith(imageName = s"${utils.image.imgR}") {
      
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rMakeOutlierTable}
        --ancestry-inferred-outliers ${ancestryInferredStrings.mkString(" ")}
        --kinship-related ${kin0Strings.mkString(" ")}
        --kinship-famsizes ${famSizesStrings.mkString(" ")}
        ${imissRemoveCmdStrings}
        --sampleqc-outliers ${sampleQcOutliersStrings.mkString(" ")}
        --sexcheck-problems ${sexcheckProblemsStrings.mkString(" ")}
        --final-exclusions ${finalSampleExclusionsStrings.mkString(" ")}
        --out ${qcReportStores.tablesData.sampleQc}"""
        .in(imissRemoveFiles ++ arrayStores.map(e => e._2).map(e => e.ancestryData.inferred).toSeq ++ arrayStores.map(e => e._2).map(e => e.kinshipData.kin0).toSeq ++ arrayStores.map(e => e._2).map(e => e.kinshipData.famSizes).toSeq ++ arrayStores.map(e => e._2).map(e => e.sampleQcData.outliers).toSeq ++ arrayStores.map(e => e._2).map(e => e.sexcheckData.problems.local.get).toSeq ++ arrayStores.map(e => e._2).map(e => e.filterQc.samplesExclude.local.get).toSeq :+ projectStores.ancestryOutliers)
        .out(qcReportStores.tablesData.sampleQc)
        .tag(s"${qcReportStores.tablesData.sampleQc}".split("/").last)
    
    }
    
    drmWith(imageName = s"${utils.image.imgR}") {
      
      cmd"""${utils.binary.binRscript} --vanilla --verbose
        ${utils.r.rUpsetplotVariantSample}
        --input ${sampleStrings.mkString(" ")}
        --exclusions ${finalSampleExclusionsStrings.mkString(" ")}
        --type sample
        --ancestry ${projectStores.ancestryInferred.local.get}
        --out ${qcReportStores.figureData.samplesRemainingUpsetPlotPdf}"""
        .in(sampleIn.toSeq ++ arrayStores.map(e => e._2).map(e => e.filterQc.samplesExclude.local.get).toSeq :+ projectStores.ancestryInferred.local.get)
        .out(qcReportStores.figureData.samplesRemainingUpsetPlotPdf)
        .tag(s"${qcReportStores.figureData.samplesRemainingUpsetPlotPdf}".split("/").last)
    
    }

    for {
      a <- projectConfig.Arrays
    } yield {

      drmWith(imageName = s"${utils.image.imgImagemagick}") {

        cmd"""${utils.binary.binConvert} -density 300 ${arrayStores(a).sampleQcData.metricPlots} ${arrayStores(a).sampleQcData.metricPlotsPng}"""
          .in(arrayStores(a).sampleQcData.metricPlots)
          .out(arrayStores(a).sampleQcData.metricPlotsPng)
          .tag(s"${arrayStores(a).sampleQcData.metricPlotsPng}".split("/").last)

      }

    }
    
    val metricOutlierPlotsStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).sampleQcData.metricPlotsPng.path.toAbsolutePath()}""").mkString(",") } }
    
    drmWith(imageName = s"${utils.image.imgPython2}") {
    
      cmd"""${utils.binary.binPython} ${utils.python.pyGenerateQcReportSampleqc}
        --compare-dist-unadj ${qcReportStores.figureData.metricDistUnadjPdf.path.toAbsolutePath()}
        --compare-dist-adj ${qcReportStores.figureData.metricDistAdjPdf.path.toAbsolutePath()}
        --compare-dist-label ${projectConfig.Arrays.map(e => e.id).head}
        --compare-dist-metric ${projectConfig.Arrays.head.sampleQcMetrics.head}
        --metric-outlier-plots ${metricOutlierPlotsStrings.mkString(" ")}
        --sampleqc-summary-table ${qcReportStores.tablesData.sampleQc.path.toAbsolutePath()}
        --samples-upset-diagram ${qcReportStores.figureData.samplesRemainingUpsetPlotPdf.path.toAbsolutePath()}
        --restore ${restoreStrings.mkString(" ")}
        --out ${qcReportStores.texData.sampleQc}"""
        .in(arrayStores.map(e => e._2).map(e => e.sampleQcData.metricPlotsPng).toSeq ++ arrayStores.map(e => e._2).map(e => e.filterQc.samplesRestore).toSeq :+ qcReportStores.figureData.metricDistUnadjPdf :+ qcReportStores.figureData.metricDistAdjPdf :+ qcReportStores.tablesData.sampleQc :+ qcReportStores.figureData.samplesRemainingUpsetPlotPdf)
        .out(qcReportStores.texData.sampleQc)
        .tag(s"${qcReportStores.texData.sampleQc}".split("/").last)
    
    }
    
    val finalVariantExclusionsStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).filterPostQc.variantsExclude.local.get.path}""").mkString(",") } }
    val postQcVariantFiltersStrings = { for { a <- projectConfig.Arrays } yield { Seq(a.id, s"""${arrayStores(a).filterPostQc.vFilters.local.get.path}""").mkString(",") } }
      
    if (projectConfig.nArrays > 1) {
    
      drmWith(imageName = s"${utils.image.imgR}") {
      
        cmd"""${utils.binary.binRscript} --vanilla --verbose
          ${utils.r.rUpsetplotVariantSample}
          --input ${varStrings.mkString(" ")}
          --exclusions ${finalVariantExclusionsStrings.mkString(" ")}
          --type variant
          --out ${qcReportStores.figureData.variantsRemainingUpsetPlotPdf}"""
          .in(variantIn.toSeq ++ arrayStores.map(e => e._2).map(e => e.filterPostQc.variantsExclude.local.get).toSeq)
          .out(qcReportStores.figureData.variantsRemainingUpsetPlotPdf)
          .tag(s"${qcReportStores.figureData.variantsRemainingUpsetPlotPdf}".split("/").last)
      
      }
    
      drmWith(imageName = s"${utils.image.imgPython2}") {
    	  
        cmd"""${utils.binary.binPython} ${utils.python.pyGenerateQcReportVariantqc}
          --variants-upset-diagram ${qcReportStores.figureData.variantsRemainingUpsetPlotPdf.path.toAbsolutePath()}
          --variant-exclusions ${finalVariantExclusionsStrings.mkString(" ")}
          --variants-exclude-table ${qcReportStores.tablesData.variantsExcludeSummary.path.toAbsolutePath()}
          --postqc-variant-filters ${postQcVariantFiltersStrings.mkString(" ")}
          --out ${qcReportStores.texData.variantQc}"""
          .in(arrayStores.map(e => e._2).map(e => e.filterPostQc.variantsExclude.local.get).toSeq ++ arrayStores.map(e => e._2).map(e => e.filterPostQc.vFilters.local.get).toSeq :+ qcReportStores.figureData.variantsRemainingUpsetPlotPdf :+ qcReportStores.tablesData.variantsExcludeSummary)
          .out(qcReportStores.texData.variantQc)
          .tag(s"${qcReportStores.texData.variantQc}".split("/").last)
    
      }
    
    } else {
    
      drmWith(imageName = s"${utils.image.imgPython2}") {
    
        cmd"""${utils.binary.binPython} ${utils.python.pyGenerateQcReportVariantqc}
          --bim ${varStrings.mkString(" ")}
          --variant-exclusions ${finalVariantExclusionsStrings.mkString(" ")}
          --variants-exclude-table ${qcReportStores.tablesData.variantsExcludeSummary.path.toAbsolutePath()}
          --postqc-variant-filters ${postQcVariantFiltersStrings.mkString(" ")}
          --out ${qcReportStores.texData.variantQc}"""
          .in(variantIn.toSeq ++ arrayStores.map(e => e._2).map(e => e.filterPostQc.variantsExclude.local.get).toSeq ++ arrayStores.map(e => e._2).map(e => e.filterPostQc.vFilters.local.get).toSeq :+ qcReportStores.tablesData.variantsExcludeSummary)
          .out(qcReportStores.texData.variantQc)
          .tag(s"${qcReportStores.texData.variantQc}".split("/").last)
    
      }
    
    }
    
    val acknowledgements = projectConfig.acknowledgements match {
    
      case Some(s) => s"--acknowledgements ${s.mkString(",")}"
      case None => ""
    
    }
    
    drmWith(imageName = s"${utils.image.imgPython2}") {
    
      cmd"""${utils.binary.binPython} ${utils.python.pyGenerateQcReportBibliography}
        ${acknowledgements}
        --loamstream-version "${projectConfig.loamstreamVersion}"
        --pipeline-version "${projectConfig.pipelineVersion}"
        --out ${qcReportStores.texData.bibliography}"""
        .out(qcReportStores.texData.bibliography)
        .tag(s"${qcReportStores.texData.bibliography}".split("/").last)
    
    }
    
    drmWith(imageName = s"${utils.image.imgTools}") {
      
      cmd"""cat ${qcReportStores.texData.header} ${qcReportStores.texData.intro} ${qcReportStores.texData.data} ${qcReportStores.texData.ancestry} ${qcReportStores.texData.ibdSexcheck} ${qcReportStores.texData.sampleQc} ${qcReportStores.texData.variantQc} ${qcReportStores.texData.bibliography} > ${qcReportStores.tex}"""
        .in(qcReportStores.texData.header, qcReportStores.texData.intro, qcReportStores.texData.data, qcReportStores.texData.ancestry, qcReportStores.texData.ibdSexcheck, qcReportStores.texData.sampleQc, qcReportStores.texData.variantQc, qcReportStores.texData.variantQc, qcReportStores.texData.bibliography)
        .out(qcReportStores.tex)
        .tag(s"${qcReportStores.tex}".split("/").last)
    
    }
    
    drmWith(imageName = s"${utils.image.imgTexLive}") {
      
      cmd"""bash -c "${utils.binary.binPdflatex} --output-directory=${dirTree.reportQc.local.get} ${qcReportStores.tex}; sleep 5; ${utils.binary.binPdflatex} --output-directory=${dirTree.reportQc.local.get} ${qcReportStores.tex}""""
        .in(qcReportStores.tex)
        .out(qcReportStores.pdf)
        .tag(s"${qcReportStores.pdf}".split("/").last)
    
    }
  
  }

}
