object Prepare extends loamstream.LoamFile {

  /**
    * Prepare array data for QC analysis
    *  Description:
    *    Convert if needed to Plink format
    *    List unplaced, unique and indels
    *    Calculate missingness. If sample missingness > 0.5, flag for removal
    *    Calculate allele frequency. If variant is monomorphic, flag for removal
    *    Convert all variant IDs to universal identifier by adding row index to end of variant ID
    *    Find all possible duplicate variants and remove lowest quality duplicates
    *    List all multiallelic variantsfor downstream use
    *    LiftOver variants if needed to GRCh37
    *  Requires: Plink, R, bash, liftOver?
    */

  import ProjectConfig._
  import ArrayStores._
  
  def Prepare(arrayCfg: ConfigArray): Unit = {
  
    val inputType = arrayCfg.technology + "_" + arrayCfg.format
  
    (arrayCfg.technology, arrayCfg.format) match {
  
      case (m,n) if inputTypesGwasVcf.contains((m,n)) =>
  
        drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {

          cmd"""${utils.binary.binPlink} --vcf ${arrayStores(arrayCfg).rawData.vcf.get.data.local.get} --allow-no-sex --keep-allele-order --output-chr ${projectConfig.plinkOutputChr} --double-id --make-bed --out ${arrayStores(arrayCfg).rawData.plink.get.base} --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1"""
            .in(arrayStores(arrayCfg).rawData.vcf.get.data.local.get)
            .out(arrayStores(arrayCfg).rawData.plink.get.data)
            .tag(s"${arrayStores(arrayCfg).rawData.vcf.get.base.local.get}.convert_to_plink".split("/").last)
        
        }
  
      case _ => ()
  
    }
  
    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""awk '$$1 == 0 {print $$2}' ${arrayStores(arrayCfg).rawData.plink.get.base}.bim > ${arrayStores(arrayCfg).rawData.unplaced.get}"""
        .in(arrayStores(arrayCfg).rawData.plink.get.data)
        .out(arrayStores(arrayCfg).rawData.unplaced.get)
        .tag(s"${arrayStores(arrayCfg).rawData.unplaced.get}".split("/").last)
    
      cmd"""awk '{k=$$1":"$$4":"$$5":"$$6; if(!m[k]) {print $$2; m[k]=1}}' ${arrayStores(arrayCfg).rawData.plink.get.base}.bim > ${arrayStores(arrayCfg).rawData.unique.get}"""
        .in(arrayStores(arrayCfg).rawData.plink.get.data)
        .out(arrayStores(arrayCfg).rawData.unique.get)
        .tag(s"${arrayStores(arrayCfg).rawData.unique.get}".split("/").last)
    
      cmd"""awk '{if($$5$$6 == "ID" || $$5$$6 == "DI") print $$2}' ${arrayStores(arrayCfg).rawData.plink.get.base}.bim > ${arrayStores(arrayCfg).rawData.indel.get}"""
        .in(arrayStores(arrayCfg).rawData.plink.get.data)
        .out(arrayStores(arrayCfg).rawData.indel.get)
        .tag(s"${arrayStores(arrayCfg).rawData.indel.get}".split("/").last)
    
    }
    
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
    
      cmd"""${utils.binary.binPlink} --bfile ${arrayStores(arrayCfg).rawData.plink.get.base} --allow-no-sex --missing --out ${arrayStores(arrayCfg).rawData.rawBase.get}.missing --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1"""
        .in(arrayStores(arrayCfg).rawData.plink.get.data)
        .out(arrayStores(arrayCfg).rawData.lmiss.get, arrayStores(arrayCfg).rawData.imiss.get)
        .tag(s"${arrayStores(arrayCfg).rawData.plink.get.base}.missing".split("/").last)
    
    }
    
    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""sed '1d' ${arrayStores(arrayCfg).rawData.imiss.get} | awk '{if($$6 > 0.5) print $$2" "$$2}' > ${arrayStores(arrayCfg).rawData.imissRemove.get}"""
        .in(arrayStores(arrayCfg).rawData.imiss.get)
        .out(arrayStores(arrayCfg).rawData.imissRemove.get)
        .tag(s"${arrayStores(arrayCfg).rawData.imissRemove.get}".split("/").last)
    
    }
    
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
    
      cmd"""${utils.binary.binPlink} --bfile ${arrayStores(arrayCfg).rawData.plink.get.base} --allow-no-sex --freq --out ${arrayStores(arrayCfg).rawData.rawBase.get}.freq --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000} --seed 1"""
        .in(arrayStores(arrayCfg).rawData.plink.get.data)
        .out(arrayStores(arrayCfg).rawData.freq.get)
        .tag(s"${arrayStores(arrayCfg).rawData.plink.get.base}.freq".split("/").last)
    
    }
    
    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""sed '1d' ${arrayStores(arrayCfg).rawData.freq.get} | awk '{if($$5 == 0) print $$2}' > ${arrayStores(arrayCfg).rawData.mono.get}"""
        .in(arrayStores(arrayCfg).rawData.freq.get)
        .out(arrayStores(arrayCfg).rawData.mono.get)
        .tag(s"${arrayStores(arrayCfg).rawData.mono.get}".split("/").last)
    
    }
  
    drmWith(imageName = s"${utils.image.imgPython2}") {
  
      cmd"""${utils.binary.binPython} ${utils.python.pyBimToUid} --bim ${arrayStores(arrayCfg).rawData.plink.get.base}.bim --out ${arrayStores(arrayCfg).rawData.uids.get}"""
        .in(arrayStores(arrayCfg).rawData.plink.get.data)
        .out(arrayStores(arrayCfg).rawData.uids.get)
        .tag(s"${arrayStores(arrayCfg).rawData.uids.get}".split("/").last)
  
    }
  
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
  
      cmd"""${utils.bash.shFindPossibleDuplicateVariants}
        ${utils.binary.binPlink}
        ${arrayStores(arrayCfg).rawData.uids.get}
        ${arrayStores(arrayCfg).rawData.possDupVars.get}
        ${arrayStores(arrayCfg).rawData.plink.get.base}
        ${arrayStores(arrayCfg).rawData.possDupPlink.get.base}
        ${arrayStores(arrayCfg).rawData.possDupBase.get}.freq
        ${arrayStores(arrayCfg).rawData.possDupBase.get}.missing
        ${projectConfig.resources.standardPlink.mem * 0.9 * 1000}
        """
        .in(arrayStores(arrayCfg).rawData.plink.get.data)
        .out(arrayStores(arrayCfg).rawData.possDupPlink.get.data :+ arrayStores(arrayCfg).rawData.possDupVars.get :+ arrayStores(arrayCfg).rawData.possDupFreq.get :+ arrayStores(arrayCfg).rawData.possDupLmiss.get)
        .tag(s"${arrayStores(arrayCfg).rawData.possDupVars.get}".split("/").last)
  
    }
  
    drmWith(imageName = s"${utils.image.imgR}", cores = projectConfig.resources.standardR.cpus, mem = projectConfig.resources.standardR.mem, maxRunTime = projectConfig.resources.standardR.maxRunTime) {
    
      cmd"""${utils.binary.binRscript} --vanilla --verbose 
        ${utils.r.rFindBestDuplicateVariants}
        --bim-in ${arrayStores(arrayCfg).rawData.possDupPlink.get.base}.bim
        --freq-in ${arrayStores(arrayCfg).rawData.possDupFreq.get}
        --miss-in ${arrayStores(arrayCfg).rawData.possDupLmiss.get}
        --out ${arrayStores(arrayCfg).rawData.dupVarsRemove.get}"""
        .in(arrayStores(arrayCfg).rawData.possDupPlink.get.data :+ arrayStores(arrayCfg).rawData.possDupFreq.get :+ arrayStores(arrayCfg).rawData.possDupLmiss.get)
        .out(arrayStores(arrayCfg).rawData.dupVarsRemove.get)
        .tag(s"${arrayStores(arrayCfg).rawData.dupVarsRemove.get}".split("/").last)
  
    }
    
    drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
  
      cmd"""${utils.bash.shPlinkPrepare} ${utils.binary.binPlink} ${arrayStores(arrayCfg).rawData.plink.get.base} ${arrayStores(arrayCfg).preparedData.get.plink.base} ${arrayStores(arrayCfg).rawData.imissRemove.get} ${arrayStores(arrayCfg).rawData.dupVarsRemove.get} ${projectConfig.resources.standardPlink.mem * 0.9 * 1000}"""
        .in(arrayStores(arrayCfg).rawData.plink.get.data :+ arrayStores(arrayCfg).rawData.imissRemove.get :+ arrayStores(arrayCfg).rawData.dupVarsRemove.get)
        .out(arrayStores(arrayCfg).preparedData.get.plink.data)
        .tag(s"${arrayStores(arrayCfg).preparedData.get.plink.base}".split("/").last)
    
    }
    
    drmWith(imageName = s"${utils.image.imgTools}") {
    
      cmd"""awk '{if(x[$$1":"$$4]) {x_count[$$1":"$$4]++; print $$2; if(x_count[$$1":"$$4] == 1) {print x[$$1":"$$4]}} x[$$1":"$$4] = $$2}' ${arrayStores(arrayCfg).preparedData.get.plink.base}.bim > ${arrayStores(arrayCfg).preparedData.get.multiallelic}"""
        .in(arrayStores(arrayCfg).preparedData.get.plink.data)
        .out(arrayStores(arrayCfg).preparedData.get.multiallelic)
        .tag(s"${arrayStores(arrayCfg).preparedData.get.multiallelic}".split("/").last)
    
    }
    
    arrayCfg.liftOver match {
    
      case Some(s) =>
    
        drmWith(imageName = s"${utils.image.imgTools}") {
        
          cmd"""awk '{print "chr"$$1"\t"$$4"\t"$$4+1"\t"$$2}' ${arrayStores(arrayCfg).preparedData.get.plink.base}.bim | sed 's/^chrMT/chrM/g' > ${arrayStores(arrayCfg).preparedData.get.bed.get}"""
            .in(arrayStores(arrayCfg).preparedData.get.plink.data)
            .out(arrayStores(arrayCfg).preparedData.get.bed.get)
        
        }
        
        drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.liftOver.cpus, mem = projectConfig.resources.liftOver.mem, maxRunTime = projectConfig.resources.liftOver.maxRunTime) {
        
          cmd"""${utils.binary.binLiftOver} ${arrayStores(arrayCfg).preparedData.get.bed.get} ${arrayStores(arrayCfg).preparedData.get.chain.get} ${arrayStores(arrayCfg).preparedData.get.lifted.get} ${arrayStores(arrayCfg).preparedData.get.unlifted.get}"""
            .in(arrayStores(arrayCfg).preparedData.get.bed.get, arrayStores(arrayCfg).preparedData.get.chain.get)
            .out(arrayStores(arrayCfg).preparedData.get.lifted.get, arrayStores(arrayCfg).preparedData.get.unlifted.get)
        
        }
        
        drmWith(imageName = s"${utils.image.imgTools}") {
    
          cmd"""sed 's/^chrM/chrMT/g' ${arrayStores(arrayCfg).preparedData.get.lifted.get} | sed 's/^chr//g' | awk '{print $$1"\t"$$2"\t"$$4}' > ${arrayStores(arrayCfg).preparedData.get.liftedUpdate.get}"""
            .in(arrayStores(arrayCfg).preparedData.get.lifted.get)
            .out(arrayStores(arrayCfg).preparedData.get.liftedUpdate.get)
        
          cmd"""awk '{print $$4}' ${arrayStores(arrayCfg).preparedData.get.lifted.get} > ${arrayStores(arrayCfg).preparedData.get.liftedExtract.get}"""
            .in(arrayStores(arrayCfg).preparedData.get.lifted.get)
            .out(arrayStores(arrayCfg).preparedData.get.liftedExtract.get)
    
        }
    
        drmWith(imageName = s"${utils.image.imgTools}", cores = projectConfig.resources.standardPlink.cpus, mem = projectConfig.resources.standardPlink.mem, maxRunTime = projectConfig.resources.standardPlink.maxRunTime) {
        
          cmd"""${utils.binary.binPlink}
            --bfile ${arrayStores(arrayCfg).preparedData.get.plink.base}
            --allow-no-sex
            --extract ${arrayStores(arrayCfg).preparedData.get.liftedExtract.get}
            --update-chr ${arrayStores(arrayCfg).preparedData.get.liftedUpdate.get} 1 3
            --update-map ${arrayStores(arrayCfg).preparedData.get.liftedUpdate.get} 2 3
            --output-chr ${projectConfig.plinkOutputChr}
            --make-bed
            --out ${arrayStores(arrayCfg).annotatedData.get.plink.base}
            --memory ${projectConfig.resources.standardPlink.mem * 0.9 * 1000}
            --seed 1"""
            .in(arrayStores(arrayCfg).preparedData.get.plink.data :+ arrayStores(arrayCfg).preparedData.get.liftedExtract.get :+ arrayStores(arrayCfg).preparedData.get.liftedUpdate.get)
            .out(arrayStores(arrayCfg).annotatedData.get.plink.data)
        
        }
    
      case None => ()
      
    }
  
  }
}
