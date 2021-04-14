object Meta extends loamstream.LoamFile {

  /**
   * Run meta analysis
   *
   */
  
  import ProjectConfig._
  //import AssocStores._
  
  def MetaAnalysis(configModel: ConfigModel, configMeta: ConfigMeta): Unit = {
  //
  //val meta = metaStores((configModel, configMeta))
  //
  //val minPartitions =  projectConfig.Metas.filter(e => e.id == configMeta.id).head.minPartitions.getOrElse("") match { case "" => ""; case _ => s"--min-partitions ${projectConfig.Metas.filter(e => e.id == configMeta.id).head.minPartitions.get}" }
  //
  //projectConfig.hailCloud match {
  //
  //	case true =>
  //
  //	val resultsListStrings = {
  //		for {
  //		c <- configMeta.cohorts
  //		} yield {
  //		val l = for {
  //			f <- assocStores.filter(e => e._1._3.isDefined).filter(e => e._1._3.get == configMeta).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).filter(e => e._2.resultsGoogle.isDefined).map(e => e._2.resultsGoogle.get).toSeq
  //		} yield {
  //			c + "___" + configModel.test + "___" + s"${f.toString.split("@")(1)}"
  //		}
  //		l.size match {
  //			case 0 => None
  //			case _ => l.head
  //		}
  //		}
  //	}
  //
  //	val excludeListStrings = {
  //		val l = for {
  //		c <- configMeta.cohorts
  //		} yield {
  //		val m = for {
  //			f <- assocStores.filter(e => e._1._3.isDefined).filter(e => e._1._3.get == configMeta).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).filter(e => e._2.variantsExcludeGoogle.isDefined).map(e => e._2.variantsExcludeGoogle.get).toSeq
  //		} yield {
  //			c + "___" + s"${f.toString.split("@")(1)}"
  //		}
  //		m.size match {
  //			case 0 => None
  //			case _ => m.head
  //		}
  //		}
  //		l.size match {
  //		case 0 => ""
  //		case _ => "--exclusions " + l.mkString(",")
  //		}
  //	}
  //
  //	val resultsList = {
  //		for {
  //		c <- configMeta.cohorts
  //		} yield {
  //		assocStores.filter(e => e._1._3.isDefined).filter(e => e._1._3.get == configMeta).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).filter(e => e._2.resultsGoogle.isDefined).map(e => e._2.resultsGoogle.get).toSeq.head
  //		}
  //	}
  //
  //	val excludeList = {
  //		for {
  //		c <- configMeta.cohorts
  //		} yield {
  //		assocStores.filter(e => e._1._3.isDefined).filter(e => e._1._3.get == configMeta).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).filter(e => e._2.variantsExcludeGoogle.isDefined).map(e => e._2.variantsExcludeGoogle.get).toSeq.head
  //		}
  //	}
  //
  //	val inList = excludeList.size match {
  //		case 0 => resultsList
  //		case _ => resultsList ++ excludeList
  //	}
  //	
  //	google {
  //	
  //		hail"""${utils.python.pyHailMetaAnalysis} --
  //		${minPartitions}
  //		--results ${resultsListStrings.mkString(",")}
  //		${excludeListStrings}
  //		--out ${meta.resultsGoogle.get}
  //		--cloud
  //		--log ${meta.hailLogGoogle.get}"""
  //			.in(inList)
  //			.out(meta.resultsGoogle.get, meta.hailLogGoogle.get)
  //			.tag(s"${meta.results}.google".split("/").last)
  //	
  //	}
  //	
  //	local {
  //	
  //		googleCopy(meta.resultsGoogle.get, meta.results)
  //		googleCopy(meta.hailLogGoogle.get, meta.hailLog)
  //	
  //	}
  //
  //	case false =>
  //
  //	val resultsListStrings = {
  //		for {
  //		c <- configMeta.cohorts
  //		} yield {
  //		val l = for {
  //			f <- assocStores.filter(e => e._1._3.isDefined).filter(e => e._1._3.get == configMeta).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).map(e => e._2.results).toSeq
  //		} yield {
  //			c + "___" + configModel.test + "___" + s"${f.toString.split("@")(1)}"
  //		}
  //		l.size match {
  //			case 0 => None
  //			case _ => l.head
  //		}
  //		}
  //	}
  //	
  //	val excludeListStrings = {
  //		val l = for {
  //		c <- configMeta.cohorts
  //		} yield {
  //		val m = for {
  //			f <- assocStores.filter(e => e._1._3.isDefined).filter(e => e._1._3.get == configMeta).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).map(e => e._2.variantsExclude.get).toSeq
  //		} yield {
  //			c + "___" + s"${f.toString.split("@")(1)}"
  //		}
  //		m.size match {
  //			case 0 => None
  //			case _ => m.head
  //		}
  //		}
  //		l.size match {
  //		case 0 => ""
  //		case _ => "--exclusions " + l.mkString(",")
  //		}
  //	}
  //	
  //	val resultsList = {
  //		for {
  //		c <- configMeta.cohorts
  //		} yield {
  //		assocStores.filter(e => e._1._3.isDefined).filter(e => e._1._3.get == configMeta).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).map(e => e._2.results).toSeq.head
  //		}
  //	}
  //	
  //	val excludeList = {
  //		for {
  //		c <- configMeta.cohorts
  //		} yield {
  //		assocStores.filter(e => e._1._3.isDefined).filter(e => e._1._3.get == configMeta).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).map(e => e._2.variantsExclude.get).toSeq.head
  //		}
  //	}
  //	
  //	val inList = excludeList.size match {
  //		case 0 => resultsList
  //		case _ => resultsList ++ excludeList
  //	}
  //	
  //	drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.metaAnalysisHail.cpus, mem = projectConfig.resources.metaAnalysisHail.mem, maxRunTime = projectConfig.resources.metaAnalysisHail.maxRunTime) {
  //	
  //		cmd"""${utils.binary.binPython} ${utils.python.pyHailMetaAnalysis}
  //		${minPartitions}
  //		--results ${resultsListStrings.mkString(",")}
  //		${excludeListStrings}
  //		--out ${meta.results}
  //		--log ${meta.hailLog}"""
  //			.in(inList)
  //			.out(meta.results, meta.hailLog)
  //			.tag(s"${meta.results}".split("/").last)
  //	
  //	}
  //
  //}
  //
  //drmWith(imageName = s"${utils.image.imgTools}") {
  //
  //	cmd"""${utils.binary.binTabix} -f -b 2 -e 2 ${meta.results}"""
  //	.in(meta.results)
  //	.out(meta.tbi)
  //	.tag(s"${meta.tbi}".split("/").last)
  //
  //}
  //
  //}
  //
  //def MetaAnalysisKnownLoci(configModel: ConfigModel, configMeta: ConfigMeta, configKnown: ConfigKnown): Unit = {
  //
  //val known = knownMetaStores((configModel, configMeta, configKnown))
  //
  //val minPartitions =  projectConfig.Metas.filter(e => e.id == configMeta.id).head.minPartitions.getOrElse("") match { case "" => ""; case _ => s"--min-partitions ${projectConfig.Metas.filter(e => e.id == configMeta.id).head.minPartitions.get}" }
  //
  //projectConfig.hailCloud match {
  //
  //	case true =>
  //
  //	val resultsListStrings = {
  //	
  //		for {
  //		c <- configMeta.cohorts
  //		} yield {
  //	
  //		val result = knownLociStores.filter(e => e._1._4.isDefined).filter(e => e._1._4.get == configMeta).filter(e => e._1._3 == configKnown).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).filter(e => e._2.resultsGoogle.isDefined).map(e => e._2.resultsGoogle.get).toSeq.head
  //		c + "___" + configModel.test + "___" + s"${result.toString.split("@")(1)}"
  //	
  //		}
  //	
  //	}
  //	
  //	val resultsList = {
  //	
  //		for {
  //		c <- configMeta.cohorts
  //		} yield {
  //	
  //		knownLociStores.filter(e => e._1._4.isDefined).filter(e => e._1._4.get == configMeta).filter(e => e._1._3 == configKnown).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).filter(e => e._2.resultsGoogle.isDefined).map(e => e._2.resultsGoogle.get).toSeq.head
  //	
  //		}
  //	
  //	}
  //	
  //	google {
  //	
  //		hail"""${utils.python.pyHailMetaAnalysis} --
  //		${minPartitions}
  //		--results ${resultsListStrings.mkString(",")}
  //		--out ${known.resultsGoogle.get}
  //		--cloud
  //		--log ${known.hailLogGoogle.get}"""
  //			.in(resultsList)
  //			.out(known.resultsGoogle.get, known.hailLogGoogle.get)
  //			.tag(s"${known.results}.google".split("/").last)
  //	
  //	}
  //	
  //	local {
  //	
  //		googleCopy(known.resultsGoogle.get, known.results)
  //		googleCopy(known.hailLogGoogle.get, known.hailLog)
  //	
  //	}
  //
  //	case false =>
  //
  //	val resultsListStrings = {
  //	
  //		for {
  //		c <- configMeta.cohorts
  //		} yield {
  //	
  //		val result = knownLociStores.filter(e => e._1._4.isDefined).filter(e => e._1._4.get == configMeta).filter(e => e._1._3 == configKnown).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).map(e => e._2.results).toSeq.head
  //		c + "___" + configModel.test + "___" + s"${result.toString.split("@")(1)}"
  //	
  //		}
  //	
  //	}
  //	
  //	val resultsList = {
  //	
  //		for {
  //		c <- configMeta.cohorts
  //		} yield {
  //	
  //		knownLociStores.filter(e => e._1._4.isDefined).filter(e => e._1._4.get == configMeta).filter(e => e._1._3 == configKnown).filter(e => e._1._1 == configModel).filter(e => e._1._2.id == c).map(e => e._2.results).toSeq.head
  //	
  //		}
  //	
  //	}
  //
  //	drmWith(imageName = s"${utils.image.imgHail}", cores = projectConfig.resources.metaAnalysisHail.cpus, mem = projectConfig.resources.metaAnalysisHail.mem, maxRunTime = projectConfig.resources.metaAnalysisHail.maxRunTime) {
  //
  //		cmd"""${utils.binary.binPython} ${utils.python.pyHailMetaAnalysis}
  //		${minPartitions}
  //		--results ${resultsListStrings.mkString(",")}
  //		--out ${known.results}
  //		--log ${known.hailLog}"""
  //			.in(resultsList)
  //			.out(known.results, known.hailLog)
  //			.tag(s"${known.results}".split("/").last)
  //
  //	}
  //
  //}
  //
  //drmWith(imageName = s"${utils.image.imgTools}") {
  //
  //	cmd"""${utils.binary.binTabix} -f -b 2 -e 2 ${known.results}"""
  //	.in(known.results)
  //	.out(known.tbi)
  //	.tag(s"${known.tbi}".split("/").last)
  //
  //}
  //
  }

}
