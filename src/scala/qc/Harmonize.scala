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
  
            cmd"""${utils.bash.shExtractIndels} ${utils.binary.binPlink} ${arrayStores(arrayCfg).annotatedData.get.plink.base} ${arrayStores(arrayCfg).annotatedData.get.snplist} $chr ${chrData.otherPlink.get.base} ${(projectConfig.resources.standardPlink.mem * 0.9 * 1000)}"""
              .in(arrayStores(arrayCfg).annotatedData.get.plink.data :+ arrayStores(arrayCfg).annotatedData.get.snplist)
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

            cmd"""${utils.bash.shMergeSnpsOther}
              --plink-path ${utils.binary.binPlink}
              --other-bim ${chrData.otherPlink.get.base}.bim
              --other-nonkg-remove ${chrData.otherRemove.get}
              --other-nonkg-flip ${chrData.otherFlip.get}
              --other-nonkg-force-a1 ${chrData.otherForceA1.get}
              --snps-huref-bfile ${chrData.mergedKgHuRefPlink.base}
              --other-huref-bfile ${chrData.otherHuRefPlink.get.base}
              --other-bfile ${chrData.otherPlink.get.base}
              --ref-bfile ${chrData.refPlink.base}
              """
              .in((chrData.mergedKgHuRefPlink.data ++ chrData.otherPlink.get.data) :+ chrData.otherRemove.get :+ chrData.otherFlip.get :+ chrData.otherForceA1.get)
              .out(chrData.otherHuRefPlink.get.data ++ chrData.refPlink.data)
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

      drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {

        cmd"""${utils.binary.binPlink} 
          --fam ${chrData.refPlink.base}.fam
          --allow-no-sex
          --make-just-fam
          --out ${chrData.harmonizedFam.toString.split("@")(1).replace(".fam","")}
          --memory 14400.0
          --seed 1"""
          .in(chrData.refPlink.data :+ chrData.forceA2)
          .out(chrData.harmonizedFam)
          .tag(s"${chrData.harmonizedFam}".split("/").last)

        cmd"""${utils.binary.binPlink} 
          --bim ${chrData.refPlink.base}.bim
          --allow-no-sex
          --real-ref-alleles
          --a2-allele ${chrData.forceA2}
          --output-chr MT
          --make-just-bim
          --out ${chrData.harmonizedBim.toString.split("@")(1).replace(".bim","")}
          --memory 14400.0
          --seed 1"""
          .in(chrData.refPlink.data :+ chrData.forceA2)
          .out(chrData.harmonizedBim)
          .tag(s"${chrData.harmonizedBim}".split("/").last)

      }
    
    }
  
  }

}
