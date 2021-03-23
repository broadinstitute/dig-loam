object Harmonize extends loamstream.LoamFile {

  /**
    * Harmonize array data for integration with prior projects
    *  Description:
    *    Extract only snps for harmonization with reference
    *    Split snps and indels into individual chromosomes
    *      Run GenotypeHarmonizer to align snps with reference data
    *      Align any non-reference snps and indels manually with human reference assembly fasta file
    *      Remove any variants that failed to align and force reference allele order
    *      Merge indels and snps together
    *      Force reference allele to A2 so Hail can read as reference downstream
    *      Recode Plink to VCF and index with Tabix
    *  Requires: Plink, GenotypeHarmonizer, Python, Bash, Tabix
    */

  import ProjectConfig._
  import ArrayStores._
  import ProjectStores._
  
  import scala.collection.immutable.ListMap
  
  def Harmonize(arrayCfg: ConfigArray): Unit = {
  
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
  
      cmd"""${utils.binary.binPlink} --bfile ${arrayStores(arrayCfg).annotatedData.get.plink.base} --allow-no-sex --snps-only --write-snplist --out ${arrayStores(arrayCfg).annotatedData.get.plink.base}.variants"""
        .in(arrayStores(arrayCfg).annotatedData.get.plink.data)
        .out(arrayStores(arrayCfg).annotatedData.get.snplist)
        .tag(s"${arrayStores(arrayCfg).annotatedData.get.snplist}".split("/").last)
    }
  
    for {
  
      (chr, chrData) <- arrayStores(arrayCfg).annotatedChrData.get
  
    } yield {
  
      drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
  
  	  cmd"""${utils.binary.binPlink} --bfile ${arrayStores(arrayCfg).annotatedData.get.plink.base} --allow-no-sex --snps-only --chr $chr --keep-allele-order --make-bed --out ${chrData.snpsPlink.base} --output-chr MT --memory ${(projectConfig.resources.standardPlink.mem * 0.9 * 1000)} --seed 1"""
          .in(arrayStores(arrayCfg).annotatedData.get.plink.data)
          .out(chrData.snpsPlink.data)
          .tag(s"${chrData.snpsPlink.base}".split("/").last)
  
      }
  
      arrayCfg.keepIndels match {
  
        case true =>
  
          drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
  
            cmd"""${utils.binary.binPlink} --bfile ${arrayStores(arrayCfg).annotatedData.get.plink.base} --allow-no-sex --exclude ${arrayStores(arrayCfg).annotatedData.get.snplist} --chr $chr --keep-allele-order --make-bed --out ${chrData.otherPlink.get.base} --output-chr MT --memory ${(projectConfig.resources.standardPlink.mem * 0.9 * 1000)} --seed 1"""
              .in(arrayStores(arrayCfg).annotatedData.get.plink.data)
              .out(chrData.otherPlink.get.data)
              .tag(s"${chrData.otherPlink.get.base}".split("/").last)
  
          }
  
        case false => ()
  
      }
  
      val harmMem = projectConfig.resources.genotypeHarmonizer.mem * 0.75 * 1000
  
      drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.genotypeHarmonizer.cpus, mem = projectConfig.resources.genotypeHarmonizer.mem, maxRunTime = projectConfig.resources.genotypeHarmonizer.maxRunTime) {
      
        cmd"""java -Xms${harmMem.toInt}m -Xmx${harmMem.toInt}m -jar ${utils.binary.binGenotypeHarmonizer}
        --input ${chrData.snpsPlink.base}
        --inputType PLINK_BED
        --output ${chrData.mergedKgPlink.base}
        --outputType PLINK_BED
        --ref ${projectStores.kgVcf(chr)}
        --refType VCF
        --keep
        --update-id
        --variants 1000
        --mafAlign 0.1
        --update-id
        --update-reference-allele
        --debug"""
          .in(chrData.snpsPlink.data :+ projectStores.kgVcf(chr))
          .out(chrData.mergedKgPlink.data :+ chrData.mergedKgVarIdUpdate :+ chrData.mergedKgVarSnpLog)
          .tag(s"${chrData.mergedKgPlink.base}".split("/").last)
      
      }
  
      drmWith(imageName = s"${utils.image.imgPython2}", cores = projectConfig.resources.standardPython.cpus, mem = projectConfig.resources.standardPython.mem, maxRunTime = projectConfig.resources.standardPython.maxRunTime) {
        
        cmd"""${utils.binary.binPython} ${utils.python.pyAlignNon1kgVariants}
        --kg-ids ${projectStores.kgIds(chr)}
        --bim ${chrData.mergedKgPlink.base}.bim
        --ref ${projectStores.humanReference(chr)}
        --out-remove ${chrData.nonKgRemove}
        --out-ignore ${chrData.nonKgIgnore}
        --out-mono ${chrData.nonKgMono}
        --out-nomatch ${chrData.nonKgNomatch}
        --out-flip ${chrData.nonKgFlip}
        --out-force-a1 ${chrData.nonKgForceA1}"""
          .in(chrData.mergedKgPlink.data :+ projectStores.kgIds(chr) :+ chrData.mergedKgVarIdUpdate :+ chrData.mergedKgVarSnpLog)
          .out(chrData.nonKgRemove, chrData.nonKgIgnore, chrData.nonKgMono, chrData.nonKgNomatch, chrData.nonKgFlip, chrData.nonKgForceA1)
          .tag(s"${chrData.mergedKgNonKgBase}".split("/").last)
  
      }
  
      arrayCfg.keepIndels match {
  
        case true =>
  
          drmWith(imageName = s"${utils.image.imgPython2}", cores = projectConfig.resources.standardPython.cpus, mem = projectConfig.resources.standardPython.mem, maxRunTime = projectConfig.resources.standardPython.maxRunTime) {
  
            cmd"""${utils.binary.binPython} ${utils.python.pyAlignNon1kgVariants}
            --bim ${chrData.otherPlink.get.base}.bim
            --ref ${projectStores.humanReference(chr)}
            --out-remove ${chrData.otherRemove.get}
            --out-ignore ${chrData.otherIgnore.get}
            --out-mono ${chrData.otherMono.get}
            --out-nomatch ${chrData.otherNomatch.get}
            --out-flip ${chrData.otherFlip.get}
            --out-force-a1 ${chrData.otherForceA1.get}"""
              .in(chrData.otherPlink.get.data)
              .out(chrData.otherRemove.get, chrData.otherIgnore.get, chrData.otherMono.get, chrData.otherNomatch.get, chrData.otherFlip.get, chrData.otherForceA1.get)
              .tag(s"${chrData.otherNonKgBase.get}".split("/").last)
  
          }
  
        case false => ()
  
      }
  
      drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
        
        cmd"""${utils.binary.binPlink} --bfile ${chrData.mergedKgPlink.base} --allow-no-sex --exclude ${chrData.nonKgRemove} --flip ${chrData.nonKgFlip} --a1-allele ${chrData.nonKgForceA1} --output-chr MT --make-bed --out ${chrData.mergedKgHuRefPlink.base} --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1"""
          .in(chrData.mergedKgPlink.data :+ chrData.nonKgRemove :+ chrData.nonKgFlip :+ chrData.nonKgForceA1)
          .out(chrData.mergedKgHuRefPlink.data)
          .tag(s"${chrData.mergedKgHuRefPlink.base}".split("/").last)
      
      }
  
      arrayCfg.keepIndels match {
  
        case true =>
  
          drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
        
            cmd"""${utils.binary.binPlink} --bfile ${chrData.otherPlink.get.base} --allow-no-sex --exclude ${chrData.otherRemove.get} --flip ${chrData.otherFlip.get} --a1-allele ${chrData.otherForceA1.get} --output-chr MT --make-bed --out ${chrData.otherHuRefPlink.get.base} --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1"""
              .in(chrData.otherPlink.get.data :+ chrData.otherRemove.get :+ chrData.otherFlip.get :+ chrData.otherForceA1.get)
              .out(chrData.otherHuRefPlink.get.data)
              .tag(s"${chrData.otherHuRefPlink.get.base}".split("/").last)
          
            cmd"""${utils.binary.binPlink} --bfile ${chrData.mergedKgHuRefPlink.base} --allow-no-sex --bmerge ${chrData.otherHuRefPlink.get.base} --output-chr MT --make-bed --keep-allele-order --out ${chrData.refPlink.base} --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1"""
              .in(chrData.mergedKgHuRefPlink.data ++ chrData.otherHuRefPlink.get.data)
              .out(chrData.refPlink.data)
              .tag(s"${chrData.refPlink.base}".split("/").last)

          }
  
        case false => ()
      
      }

      drmWith(imageName = s"${utils.image.imgTools}") {

        cmd"""awk '{print $$2,$$5}' ${chrData.refPlink.base}.bim > ${chrData.forceA2}"""
          .in(chrData.refPlink.data)
          .out(chrData.forceA2)
          .tag(s"${chrData.forceA2}".split("/").last)
      
      }
      
      drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
      
        cmd"""${utils.binary.binPlink} --bfile ${chrData.refPlink.base} --allow-no-sex --real-ref-alleles --a2-allele ${chrData.forceA2} --output-chr MT --recode vcf-iid bgz --out ${chrData.harmonizedVcf.base.local.get} --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1; if [ $$? -eq 0 ]; then mv ${chrData.harmonizedVcf.base.local.get}.vcf.gz ${chrData.harmonizedVcf.base.local.get}.vcf.bgz; fi"""
          .in(chrData.refPlink.data :+ chrData.forceA2)
          .out(chrData.harmonizedVcf.data.local.get)
          .tag(s"${chrData.harmonizedVcf.base.local.get}.vcf.bgz".split("/").last)

      }
      
      drmWith(imageName = s"${utils.image.imgTools}") {
        
        cmd"""${utils.binary.binTabix} -f -p vcf ${chrData.harmonizedVcf.data.local.get}"""
          .in(chrData.harmonizedVcf.data.local.get)
          .out(chrData.harmonizedVcf.tbi.local.get)
          .tag(s"${chrData.harmonizedVcf.tbi.local.get}".split("/").last)
      
      }
    
    }
  
    //val mergedKgHuRefLines = {
    //
    //  for {
    //    chr <- arrayStores(arrayCfg).annotatedChrData.get.map(e => e._1).toSeq.sortWith(_ < _)
    //  } yield {
    //
    //    s"${arrayStores(arrayCfg).annotatedChrData.get(chr).mergedKgHuRefPlink.base}"
    //
    //  }
    //
    //}
  
    //val otherHuRefLines = {
    //
    //  for {
    //    chr <- arrayStores(arrayCfg).annotatedChrData.get.map(e => e._1).toSeq.sortWith(_ < _) if arrayCfg.keepIndels
    //  } yield {
    //
    //    s"${arrayStores(arrayCfg).annotatedChrData.get(chr).otherHuRefPlink.get.base}"
    //
    //  }
    //
    //}
  
    //drmWith(imageName = s"${utils.image.imgTools}") {
    //
    //  cmd"""echo "${(mergedKgHuRefLines ++ otherHuRefLines).drop(1).mkString("\n")}" > ${arrayStores(arrayCfg).harmonizedData.get.mergeList}"""
    //    .out(arrayStores(arrayCfg).harmonizedData.get.mergeList)
    //    .tag(s"${arrayStores(arrayCfg).harmonizedData.get.mergeList}".split("/").last)
    //
    //}
  
    //val chrInputData = arrayCfg.keepIndels match {
    //
    //  case true => arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).flatMap(e => e.mergedKgHuRefPlink.data).toSeq ++ arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).flatMap(e => e.otherHuRefPlink.get.data).toSeq
    //  case false => arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).flatMap(e => e.mergedKgHuRefPlink.data).toSeq
    //
    //}
        
  
    //drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
    //
    //  cmd"""${utils.binary.binPlink} --bfile ${mergedKgHuRefLines.head} --allow-no-sex --merge-list ${arrayStores(arrayCfg).harmonizedData.get.mergeList} --output-chr MT --make-bed --keep-allele-order --out ${arrayStores(arrayCfg).harmonizedData.get.plink.base} --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1"""
    //    .in(chrInputData :+ arrayStores(arrayCfg).harmonizedData.get.mergeList)
    //    .out(arrayStores(arrayCfg).harmonizedData.get.plink.data)
    //    .tag(s"${arrayStores(arrayCfg).harmonizedData.get.plink.base}".split("/").last)
    //
    //}
  
    //drmWith(imageName = s"${utils.image.imgTools}") {
    //
    //  cmd"""awk '{print $$2,$$5}' ${arrayStores(arrayCfg).harmonizedData.get.plink.base}.bim > ${arrayStores(arrayCfg).harmonizedData.get.forceA2}"""
    //    .in(arrayStores(arrayCfg).harmonizedData.get.plink.data)
    //    .out(arrayStores(arrayCfg).harmonizedData.get.forceA2)
    //    .tag(s"${arrayStores(arrayCfg).harmonizedData.get.forceA2}".split("/").last)
    //
    //}
    //
    //drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
    //
    //  //cmd"""${utils.binary.binPlink} --bfile ${arrayStores(arrayCfg).harmonizedData.get.plink.base} --allow-no-sex --real-ref-alleles --a2-allele ${arrayStores(arrayCfg).harmonizedData.get.forceA2} --output-chr MT --make-bed --out ${arrayStores(arrayCfg).refData.plink.get.base.local.get} --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1"""
    //  //  .in(arrayStores(arrayCfg).harmonizedData.get.plink.data :+ arrayStores(arrayCfg).harmonizedData.get.forceA2)
    //  //  .out(arrayStores(arrayCfg).refData.plink.get.data.local.get)
    //  //  .tag(s"${arrayStores(arrayCfg).refData.plink.get.base.local.get}".split("/").last)
    //
    //  cmd"""${utils.binary.binPlink} --bfile ${arrayStores(arrayCfg).harmonizedData.get.plink.base} --allow-no-sex --real-ref-alleles --a2-allele ${arrayStores(arrayCfg).harmonizedData.get.forceA2} --output-chr MT --recode vcf-iid bgz --out ${arrayStores(arrayCfg).refData.vcf.base.local.get} --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1; if [ $$? -eq 0 ]; then mv ${arrayStores(arrayCfg).refData.vcf.base.local.get}.vcf.gz ${arrayStores(arrayCfg).refData.vcf.base.local.get}.vcf.bgz; fi"""
    //    .in(arrayStores(arrayCfg).harmonizedData.get.plink.data :+ arrayStores(arrayCfg).harmonizedData.get.forceA2)
    //    .out(arrayStores(arrayCfg).refData.vcf.data.local.get)
    //    .tag(s"${arrayStores(arrayCfg).refData.vcf.base.local.get}".split("/").last)
    //
    //  //cmd"""${utils.bash.shPlinkToVcfNoHalfCalls} ${utils.binary.binPlink} ${arrayStores(arrayCfg).harmonizedData.get.plink.base} ${arrayStores(arrayCfg).harmonizedData.get.forceA2} ${arrayStores(arrayCfg).refData.vcf.get.base.local.get} ${projectConfig.resources.standardPlink.mem * 0.75 * 1000} ${arrayStores(arrayCfg).refData.vcf.get.data.local.get}"""
    //  //  .in(arrayStores(arrayCfg).harmonizedData.get.plink.data :+ arrayStores(arrayCfg).harmonizedData.get.forceA2)
    //  //  .out(arrayStores(arrayCfg).refData.vcf.get.data.local.get)
    //  //  .tag(s"${arrayStores(arrayCfg).refData.vcf.get.data.local.get}".split("/").last)
    //
    //}
    //
    //drmWith(imageName = s"${utils.image.imgTools}") {
    //  
    //  cmd"""${utils.binary.binTabix} -f -p vcf ${arrayStores(arrayCfg).refData.vcf.data.local.get}"""
    //    .in(arrayStores(arrayCfg).refData.vcf.data.local.get)
    //    .out(arrayStores(arrayCfg).refData.vcf.tbi.local.get)
    //    .tag(s"${arrayStores(arrayCfg).refData.vcf.tbi.local.get}".split("/").last)
    //
    //}
  
    //val nonKgRemoveString = ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.nonKgRemove.toString.split("@")(1)}"""}.mkString(",")
    //val nonKgMonoString = ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.nonKgMono.toString.split("@")(1)}"""}.mkString(",")
    //val nonKgNomatchString = ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.nonKgNomatch.toString.split("@")(1)}"""}.mkString(",")
    //val nonKgIgnoreString = ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.nonKgIgnore.toString.split("@")(1)}"""}.mkString(",")
    //val nonKgFlipString = ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.nonKgFlip.toString.split("@")(1)}"""}.mkString(",")
    //val nonKgForceA1String = ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.nonKgForceA1.toString.split("@")(1)}"""}.mkString(",")
    //val mergedKgVarIdUpdateString = ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.mergedKgVarIdUpdate.toString.split("@")(1)}"""}.mkString(",")
    //val mergedKgVarSnpLogString = ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.mergedKgVarSnpLog.toString.split("@")(1)}"""}.mkString(",")
    //
    //val nonKgRemove = arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.nonKgRemove).toSeq
    //val nonKgMono = arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.nonKgMono).toSeq
    //val nonKgNomatch = arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.nonKgNomatch).toSeq
    //val nonKgIgnore = arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.nonKgIgnore).toSeq
    //val nonKgFlip = arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.nonKgFlip).toSeq
    //val nonKgForceA1 = arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.nonKgForceA1).toSeq
    //val mergedKgVarIdUpdate = arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.mergedKgVarIdUpdate).toSeq
    //val mergedKgVarSnpLog = arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.mergedKgVarSnpLog).toSeq
    //
    //val otherRemoveString = arrayCfg.keepIndels match { case true => Some(ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.otherRemove.get.toString.split("@")(1)}"""}.mkString(",")); case false => None }
    //val otherMonoString = arrayCfg.keepIndels match { case true => Some(ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.otherMono.get.toString.split("@")(1)}"""}.mkString(",")); case false => None }
    //val otherNomatchString = arrayCfg.keepIndels match { case true => Some(ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.otherNomatch.get.toString.split("@")(1)}"""}.mkString(",")); case false => None }
    //val otherIgnoreString = arrayCfg.keepIndels match { case true => Some(ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.otherIgnore.get.toString.split("@")(1)}"""}.mkString(",")); case false => None }
    //val otherFlipString = arrayCfg.keepIndels match { case true => Some(ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.otherFlip.get.toString.split("@")(1)}"""}.mkString(",")); case false => None }
    //val otherForceA1String = arrayCfg.keepIndels match { case true => Some(ListMap(arrayStores(arrayCfg).annotatedChrData.get.toSeq.sortBy(_._1):_*).map(e => e._2).map{ e => s"""${e.otherForceA1.get.toString.split("@")(1)}"""}.mkString(",")); case false => None }
    //
    //val xRemove = arrayCfg.keepIndels match { case true => arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.otherRemove.get).toSeq ++ nonKgRemove; case false => nonKgRemove }
    //val xMono = arrayCfg.keepIndels match { case true => arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.otherMono.get).toSeq ++ nonKgMono; case false => nonKgMono }
    //val xNomatch = arrayCfg.keepIndels match { case true => arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.otherNomatch.get).toSeq ++ nonKgNomatch; case false => nonKgNomatch }
    //val xIgnore = arrayCfg.keepIndels match { case true => arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.otherIgnore.get).toSeq ++ nonKgIgnore; case false => nonKgIgnore }
    //val xFlip = arrayCfg.keepIndels match { case true => arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.otherFlip.get).toSeq ++ nonKgFlip; case false => nonKgFlip }
    //val xForceA1 = arrayCfg.keepIndels match { case true => arrayStores(arrayCfg).annotatedChrData.get.map(e => e._2).map(e => e.otherForceA1.get).toSeq ++ nonKgForceA1; case false => nonKgForceA1 }
    //
    //val xRemoveString = otherRemoveString match { case Some(s) => Seq(nonKgRemoveString,s).mkString(","); case None => nonKgRemoveString }
    //val xMonoString = otherMonoString match { case Some(s) => Seq(nonKgMonoString, s).mkString(","); case None => nonKgMonoString }
    //val xNomatchString = otherNomatchString match { case Some(s) => Seq(nonKgNomatchString, s).mkString(","); case None => nonKgNomatchString }
    //val xIgnoreString = otherIgnoreString match { case Some(s) => Seq(nonKgIgnoreString, s).mkString(","); case None => nonKgIgnoreString }
    //val xFlipString = otherFlipString match { case Some(s) => Seq(nonKgFlipString, s).mkString(","); case None => nonKgFlipString }
    //val xForceA1String = otherForceA1String match { case Some(s) => Seq(nonKgForceA1String, s).mkString(","); case None => nonKgForceA1String }
    //
    //drmWith(imageName = s"${utils.image.imgPython2}") {
    //
    //  cmd"""${utils.binary.binPython} ${utils.python.pyMergeVariantLists}
    //    --remove-in "$xRemoveString"
    //    --remove-mono-in "$xMonoString"
    //    --remove-nomatch-in "$xNomatchString"
    //    --ignore-in "$xIgnoreString"
    //    --flip-in "$xFlipString"
    //    --force-a1-in "$xForceA1String"
    //    --id-updates-in "$mergedKgVarIdUpdateString"
    //    --snp-log-in "$mergedKgVarSnpLogString"
    //    --remove-out ${arrayStores(arrayCfg).harmonizedData.get.nonKgRemove}
    //    --remove-mono-out ${arrayStores(arrayCfg).harmonizedData.get.nonKgMono}
    //    --remove-nomatch-out ${arrayStores(arrayCfg).harmonizedData.get.nonKgNomatch}
    //    --ignore-out ${arrayStores(arrayCfg).harmonizedData.get.nonKgIgnore}
    //    --flip-out ${arrayStores(arrayCfg).harmonizedData.get.nonKgFlip}
    //    --force-a1-out ${arrayStores(arrayCfg).harmonizedData.get.nonKgForceA1}
    //    --id-updates-out ${arrayStores(arrayCfg).harmonizedData.get.mergedKgVarIdUpdate}
    //    --snp-log-out ${arrayStores(arrayCfg).harmonizedData.get.mergedKgVarSnpLog}
    //    """
    //    .in(xRemove ++ xMono ++ xNomatch ++ xIgnore ++ xFlip ++ xForceA1 ++ mergedKgVarIdUpdate ++ mergedKgVarSnpLog)
    //    .out(arrayStores(arrayCfg).harmonizedData.get.nonKgRemove, arrayStores(arrayCfg).harmonizedData.get.nonKgNomatch, arrayStores(arrayCfg).harmonizedData.get.nonKgMono, arrayStores(arrayCfg).harmonizedData.get.nonKgIgnore, arrayStores(arrayCfg).harmonizedData.get.nonKgFlip, arrayStores(arrayCfg).harmonizedData.get.nonKgForceA1, arrayStores(arrayCfg).harmonizedData.get.mergedKgVarIdUpdate, arrayStores(arrayCfg).harmonizedData.get.mergedKgVarSnpLog)
    //    .tag(s"${arrayStores(arrayCfg).harmonizedData.get.plink.base}.pyMergeVariantLists".split("/").last)
    //
    //}
  
  }

}
