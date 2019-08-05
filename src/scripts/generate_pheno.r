library(GENESIS)
library(SNPRelate)
library(GWASTools)
library(gdsfmt)
library(pryr)
library(argparse)

set.seed(1)

parser <- ArgumentParser()
parser$add_argument("--cpus", dest="cpus", type="character", help="number of cpus to use for King")
parser$add_argument("--gds-in", dest="gds_in", type="character", help="a plink binary file path")
parser$add_argument("--pheno-in", dest="pheno_in", type="character", help="a phenotype file")
parser$add_argument("--ancestry-in", dest="ancestry_in", type="character", help="an ancestry file")
parser$add_argument("--ancestry-keep", dest="ancestry_keep", type="character", help="a comma separated list of population groups to keep (ie. EUR,AFR)")
parser$add_argument("--pheno-col", dest="pheno_col", type="character", help="a column name for phenotype")
parser$add_argument("--iid-col", dest="iid_col", help='a column name for sample ID in phenotype file')
parser$add_argument("--sampleqc-in", dest="sampleqc_in", type="character", help="a sampleqc file")
parser$add_argument("--kinship-in", dest="kinship_in", type="character", help="a kinship file containing related pairs")
parser$add_argument("--samples-exclude-qc", dest="samples_exclude_qc", type="character", help="a list of sample IDs to exclude based on sample qc")
parser$add_argument("--samples-exclude-postqc", dest="samples_exclude_postqc", type="character", help="a list of sample IDs to exclude based on postqc filters")
parser$add_argument("--samples-exclude-cohort", dest="samples_exclude_cohort", type="character", help="a list of sample IDs to exclude based on cohort-level filters")
parser$add_argument("--samples-exclude-cross-array", dest="samples_exclude_cross_array", type="character", help="a list of sample IDs to exclude based on cross array kinship")
parser$add_argument("--variants-exclude-postqc", dest="variants_exclude_postqc", default=NULL, type="character", help="a list of variant IDs to exclude based on post-qc filtering")
parser$add_argument("--test", dest="test", type="character", help="a test code")
parser$add_argument("--trans", dest="trans", type="character", help="a comma separated list of transformation codes")
parser$add_argument("--covars", dest="covars", type="character", help="a '+' separated list of covariates")
parser$add_argument("--min-pcs", dest="min_pcs", type="integer", help="minimum number of pcs to include in analysis")
parser$add_argument("--max-pcs", dest="max_pcs", type="integer", help="maximum number of pcs to include in analysis")
parser$add_argument("--n-stddevs", dest="n_stddevs", type="integer", help="outlier detection threshold in number of standard deviations from the mean")
parser$add_argument("--out-id-map", dest="out_id_map", type="character", help="an output filename for the id removal map")
parser$add_argument("--out-pheno", dest="out_pheno", type="character", help="a phenotype output filename")
parser$add_argument("--out-pcs", dest="out_pcs", type="character", help="an output filename for PCs to include in analysis")
args<-parser$parse_args()

print(args)

calc_kinship <- function(gds, sam, vin, t) {
	gf <- snpgdsOpen(gds)
	k<-snpgdsIBDKING(gf, sample.id=sam, snp.id=vin, autosome.only=TRUE, remove.monosnp=TRUE, maf=NaN, missing.rate=NaN, type="KING-robust", family.id=NULL, num.thread=t, verbose=TRUE)
	kinship <- k$kinship
	rownames(kinship)<-k$sample.id
	colnames(kinship)<-k$sample.id
	snpgdsClose(gf)
	return(kinship)
}

INVN <- function(x){
	return(qnorm((rank(x,na.last="keep") - 0.5)/sum(!is.na(x))))
}

pcs_include <- function(d, y, cv, n) {
	if(cv != "") {
		m <- summary(lm(as.formula(paste(y,"~",cv,"+",paste(paste("PC",seq(1,n,1),sep=""),collapse="+"),sep="")),data=d))
	} else {
		m <- summary(lm(as.formula(paste(y,"~",paste(paste("PC",seq(1,n,1),sep=""),collapse="+"),sep="")),data=d))
	}
	print(m)
	mc <- as.data.frame(m$coefficients)
	s <- rownames(mc[mc[,"Pr(>|t|)"] <= 0.05,])
	spcs <- s[grep("^PC",s)]
	if(length(spcs) > 0) {
		mpc <- max(as.integer(gsub("PC","",spcs)))
		inpcs <- paste("PC",seq(1,mpc,1),sep="")
	} else {
		inpcs<-c()
	}
	return(inpcs)
}

cat("removing factor indicators from covariates\n")
covars <- gsub("\\]","",gsub("\\[","",unlist(strsplit(args$covars,split="\\+"))))

cat("extracting model specific columns from phenotype file\n")
pheno<-read.table(args$pheno_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
cat(paste0("extracting model specific columns: ", paste(c(args$iid_col, args$pheno_col, covars), collapse=",")),"\n")
pheno<-pheno[,c(args$iid_col, args$pheno_col, covars)]
out_cols<-colnames(pheno)

cat("reading inferred ancestry from file\n")
ancestry<-read.table(args$ancestry_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
names(ancestry)[1]<-args$iid_col
names(ancestry)[2]<-"ANCESTRY_INFERRED"
pheno<-merge(pheno,ancestry,all.x=T)
if(! is.null(args$ancestry_keep)) {
	anc_keep = unlist(strsplit(args$ancestry_keep,","))
	cat(paste("keeping populations group/s",paste(anc_keep,collapse="+"),"for analysis",sep=" "),"\n")
	pheno <- pheno[pheno$ANCESTRY_INFERRED %in% anc_keep,]
}

cat("reading sample and variant IDs from gds file\n")
geno <- GdsGenotypeReader(filename = args$gds_in)
geno_snps <- data.frame(idx = getSnpID(geno), name = paste(getChromosome(geno), pos = getPosition(geno), sep=":"))
iids <- getScanID(geno)
close(geno)

id_map <- data.frame(ID = pheno[,args$iid_col])
id_map$removed_nogeno <- 0
id_map$removed_sampleqc <- 0
id_map$removed_postqc_filters <- 0
id_map$removed_cohort_filters <- 0
id_map$removed_incomplete_obs <- 0
id_map$removed_kinship_cross_array <- 0
id_map$removed_kinship <- 0
id_map$removed_pc_outlier <- 0
id_map$removed_nogeno[which(! id_map$ID %in% iids)] <- 1
pheno <- pheno[which(pheno[,args$iid_col] %in% iids),]

cat("read in sample IDs excluded during sample qc\n")
if(args$samples_exclude_qc != "") {
	samples_excl<-scan(file=args$samples_exclude_qc,what="character")
	pheno <- pheno[which(! pheno[,args$iid_col] %in% samples_excl),]
	id_map$removed_sampleqc[which((id_map$removed_nogeno == 0) & (id_map$ID %in% samples_excl))] <- 1
	cat(paste0("removed ",as.character(length(id_map$removed_sampleqc[which(id_map$removed_sampleqc == 1)]))," samples that did not pass sample qc"),"\n")
}

cat("read in sample IDs excluded during post-qc filtering\n")
if(args$samples_exclude_postqc != "") {
	samples_excl<-scan(file=args$samples_exclude_postqc,what="character")
	pheno <- pheno[which(! pheno[,args$iid_col] %in% samples_excl),]
	id_map$removed_postqc_filters[which((id_map$removed_nogeno == 0) & (id_map$removed_sampleqc == 0) & (id_map$ID %in% samples_excl))] <- 1
	cat(paste0("removed ",as.character(length(id_map$removed_postqc_filters[which(id_map$removed_postqc_filters == 1)]))," samples that did not pass post-qc filters"),"\n")
}

cat("read in sample IDs excluded during cohort-level filtering\n")
if(args$samples_exclude_cohort != "") {
	samples_excl<-scan(file=args$samples_exclude_cohort,what="character")
	pheno <- pheno[which(! pheno[,args$iid_col] %in% samples_excl),]
	id_map$removed_cohort_filters[which((id_map$removed_nogeno == 0) & (id_map$removed_sampleqc == 0) & (id_map$removed_postqc_filters == 0) & (id_map$ID %in% samples_excl))] <- 1
	cat(paste0("removed ",as.character(length(id_map$removed_cohort_filters[which(id_map$removed_cohort_filters == 1)]))," samples that did not pass cohort-level filters"),"\n")
}

cat("read in sample IDs excluded during cross array kinship checks\n")
if(args$samples_exclude_cross_array != "") {
	samples_excl<-scan(file=args$samples_exclude_cross_array,what="character")
	pheno <- pheno[which(! pheno[,args$iid_col] %in% samples_excl),]
	id_map$removed_kinship_cross_array[which((id_map$removed_nogeno == 0) & (id_map$removed_sampleqc == 0) & (id_map$removed_postqc_filters == 0) & (id_map$removed_cohort_filters == 0) & (id_map$ID %in% samples_excl))] <- 1
	cat(paste0("removed ",as.character(length(id_map$removed_kinship_cross_array[which(id_map$removed_kinship_cross_array == 1)]))," samples due to cross-array kinship"),"\n")
}

cat("reading in kinship values for related pairs\n")
kinship_in <- read.table(args$kinship_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
kinship_in <- kinship_in[which((kinship_in$ID1 %in% pheno[,args$iid_col]) & (kinship_in$ID2 %in% pheno[,args$iid_col])),]
kinship_in <- kinship_in[which((! kinship_in$ID1 %in% id_map$ID[which((id_map$removed_nogeno == 1) | (id_map$removed_sampleqc == 1) | (id_map$removed_postqc_filters == 1) | (id_map$removed_cohort_filters == 1) | (id_map$removed_kinship_cross_array == 1))]) & (! kinship_in$ID2 %in% id_map$ID[which((id_map$removed_nogeno == 1) | (id_map$removed_sampleqc == 1) | (id_map$removed_postqc_filters == 1) | (id_map$removed_cohort_filters == 1) | (id_map$removed_kinship_cross_array == 1))])),]
if(nrow(kinship_in) > 0) {
	kinship_in$pair_idx <- row.names(kinship_in)
	kinship_in$rand_choice <- sample.int(2, replace = TRUE, size = nrow(kinship_in))
	sampleqc_in <- read.table(args$sampleqc_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
	sampleqc_in <- sampleqc_in[,c("IID","call_rate")]
	names(sampleqc_in)[1] <- args$iid_col
	sampleqc_in <- merge(sampleqc_in, pheno[,c(args$iid_col, args$pheno_col)], all.x=TRUE)
	sampleqc_in <- sampleqc_in[,c(args$iid_col, "call_rate", args$pheno_col)]
	names(sampleqc_in)[1] <- "ID1"
	names(sampleqc_in)[2] <- "ID1_call_rate"
	names(sampleqc_in)[3] <- "ID1_pheno"
	kinship_in <- merge(kinship_in, sampleqc_in, all.x=T)
	names(sampleqc_in)[1] <- "ID2"
	names(sampleqc_in)[2] <- "ID2_call_rate"
	names(sampleqc_in)[3] <- "ID2_pheno"
	kinship_in <- merge(kinship_in, sampleqc_in, all.x=T)
	kinship_in <- kinship_in[order(-kinship_in$Kinship),]
	
	cat("removing lower quality sample for each related pair starting with the highest kinship value pair, until no more pairs remain\n")
	kinship_in$ID1_remove <- 0
	kinship_in$ID2_remove <- 0
	samples_excl <- c()
	if(args$test != "lmm") {
		for(i in 1:nrow(kinship_in)) {
			if(kinship_in$ID1_remove[i] == 0 & kinship_in$ID2_remove[i] == 0) {
				if(length(unique(pheno[,args$pheno_col])) == 2 & kinship_in$ID1_pheno[i] != kinship_in$ID2_pheno[i]) {
					if(kinship_in$ID1_pheno[i] > kinship_in$ID2_pheno[i]) {
						kinship_in$ID1_remove[which(kinship_in$ID1 == kinship_in$ID2[i])] <- 1
						kinship_in$ID2_remove[which(kinship_in$ID2 == kinship_in$ID2[i])] <- 1
					} else {
						kinship_in$ID1_remove[which(kinship_in$ID1 == kinship_in$ID1[i])] <- 1
						kinship_in$ID2_remove[which(kinship_in$ID2 == kinship_in$ID1[i])] <- 1
					}
				} else {
					if(kinship_in$ID1_call_rate[i] == kinship_in$ID2_call_rate[i]) {
						kinship_in$ID1_remove[which(kinship_in$rand_choice == 1)] <- 1
						kinship_in$ID2_remove[which(kinship_in$rand_choice == 2)] <- 1
					} else if(kinship_in$ID1_call_rate[i] > kinship_in$ID2_call_rate[i]) {
						kinship_in$ID1_remove[which(kinship_in$ID1 == kinship_in$ID2[i])] <- 1
						kinship_in$ID2_remove[which(kinship_in$ID2 == kinship_in$ID2[i])] <- 1
					} else {
						kinship_in$ID1_remove[which(kinship_in$ID1 == kinship_in$ID1[i])] <- 1
						kinship_in$ID2_remove[which(kinship_in$ID2 == kinship_in$ID1[i])] <- 1
					}
				}
			}
		}
		samples_excl <- kinship_in$ID1[which(kinship_in$ID1_remove == 1)]
		samples_excl <- c(samples_excl, kinship_in$ID2[which(kinship_in$ID2_remove == 1)])
		pheno <- pheno[which(! pheno[,args$iid_col] %in% samples_excl),]
		id_map$removed_kinship[which((id_map$removed_nogeno == 0) & (id_map$removed_sampleqc == 0) & (id_map$removed_postqc_filters == 0) & (id_map$removed_cohort_filters == 0) & (id_map$removed_kinship_cross_array == 0) & (id_map$ID %in% samples_excl))] <- 1
		cat(paste0("removed ",as.character(length(id_map$removed_kinship[which(id_map$removed_kinship == 1)]))," samples due to within-array kinship"),"\n")
	}
} else {
	cat(paste0("removed 0 samples due to kinship"),"\n")
}

cat("extracting only complete observations\n")
pheno <- pheno[complete.cases(pheno),]
id_map$removed_incomplete_obs[which((id_map$removed_nogeno == 0) & (id_map$removed_sampleqc == 0) & (id_map$removed_postqc_filters == 0) & (id_map$removed_cohort_filters == 0) & (id_map$removed_kinship_cross_array == 0) & (id_map$removed_kinship == 0) & (! id_map$ID %in% pheno[,args$iid_col]))] <- 1
cat(paste0("removed ",as.character(length(id_map$removed_incomplete_obs[which(id_map$removed_incomplete_obs == 1)]))," samples with incomplete observations"),"\n")

cat("read post-qc variant exclusion list\n")
variants_excl <- c()
variants_excl_postqc <- try(read.table(args$variants_exclude_postqc,header=FALSE, as.is=TRUE, stringsAsFactors=FALSE, sep="\t"), silent=TRUE)
if(! inherits(variants_excl_postqc, "try-error")) {
	variants_excl <- c(variants_excl,variants_excl_postqc$V1)
}
variants_excl<-unique(variants_excl)

variants_incl<-geno_snps$idx[! geno_snps$name %in% variants_excl]

samples_incl<-pheno[,args$iid_col]

kinship<-NULL
if(args$test == "lmm") {
	cat(paste("memory before running king: ",mem_used() / (1024^2),sep=""),"\n")
	cat("running King robust to get kinship matrix\n")
	kinship <- calc_kinship(gds = args$gds_in, sam = samples_incl, vin = variants_incl, t = args$cpus)
	cat(paste("memory after running king and before running pcair: ",mem_used() / (1024^2),sep=""),"\n")
}

failed <- FALSE
if(length(unique(pheno[,args$pheno_col])) == 1) {
	cat(paste0("phenotype ",args$pheno_col," has zero variance\n"))
	failed <- TRUE
}
covars_factors <- unlist(strsplit(args$covars,split="\\+"))
for(cv in covars_factors) {
	cvv <- unlist(strsplit(cv,split=""))
	if(cvv[1] == "[" && cvv[length(cvv)] == "]") {
		cvb<-paste(cvv[2:(length(cvv)-1)],collapse="")
		if(length(unique(pheno[,cvb])) == 1) {
			cat(paste0("covariate ",cvb," has zero variance\n"))
			failed <- TRUE
		} else {
			for(val in sort(unique(pheno[,cvb]))[2:length(sort(unique(pheno[,cvb])))]) {
				pheno[,paste0(cvb,val)] <- 0
				pheno[,paste0(cvb,val)][which(pheno[,cvb] == val)] <- 1
				covars_factors <- c(covars_factors,paste0(cvb,val))
			}
		}
		covars_factors <- covars_factors[covars_factors != cv]
	} else {
		if(length(unique(pheno[,cv])) == 1) {
			cat(paste0("covariate ",cv," has zero variance\n"))
			failed <- TRUE
		}
	}
}
if(failed) {
	cat("exiting due to invalid model\n")
	quit(status=1)
}
covars_analysis<-paste(c(covars_factors,"1"),collapse="+")
out_cols<-c(out_cols,covars_factors[! covars_factors %in% out_cols])

geno <- GdsGenotypeReader(filename = args$gds_in)
genoData <- GenotypeData(geno)
iter <- 1
samples_remove <- c()
while(iter < 11) {
	cat("\n",paste0("pcair iteration #",iter),"\n")

	if(length(samples_incl[! samples_incl %in% samples_remove]) < args$max_pcs) {
		n_pcs <- length(samples_incl[! samples_incl %in% samples_remove])
	} else {
		n_pcs <- args$max_pcs
	}
	mypcair <- try(pcair(genoData = genoData, v = n_pcs, scan.include = samples_incl[! samples_incl %in% samples_remove], snp.include = variants_incl, kinMat = kinship, divMat = kinship, unrel.set = NULL, snp.block.size = 10000), silent=TRUE)
	if(inherits(mypcair, "try-error")) {
		mypcair <- try(pcair(genoData = genoData, v = n_pcs, scan.include = samples_incl[! samples_incl %in% samples_remove], snp.include = variants_incl, kinMat = NULL, divMat = NULL, unrel.set = NULL, snp.block.size = 10000), silent=TRUE)
		if(inherits(mypcair, "try-error")) {
			print("unable to run pcair with or without kinship adjustment")
			quit(status=1)
		}
	}
	cat(paste("memory after running pcair: ",mem_used() / (1024^2),sep=""),"\n")
	pcs<-data.frame(mypcair$vectors)
	names(pcs)[1:n_pcs]<-paste("PC",seq(1,n_pcs),sep="")
	pcs[,args$iid_col]<-row.names(pcs)
	out<-merge(pheno,pcs,all.y=T)

	if(args$trans == 'invn') {
		cat("calculating invn transformation\n")
		if(length(unique(out$ANCESTRY_INFERRED)) > 1) {
			cat(paste("including inferred ancestry as indicator in calculation of residuals",sep=""),"\n")
			mf <- summary(lm(as.formula(paste(args$pheno_col,"~factor(ANCESTRY_INFERRED)+",covars_analysis,sep="")),data=out))
		} else {
			mf <- summary(lm(as.formula(paste(args$pheno_col,"~",covars_analysis,sep="")),data=out))
		}
		out[,paste(args$pheno_col,"invn",paste(unlist(strsplit(covars,"\\+")),collapse="_"),sep="_")]<-INVN(residuals(mf))
		pcsin <- pcs_include(d = out, y = paste(args$pheno_col,"invn",paste(unlist(strsplit(covars,"\\+")),collapse="_"),sep="_"), cv = "", n = n_pcs)
		out_cols <- c(out_cols,paste(args$pheno_col,"invn",paste(unlist(strsplit(covars,"\\+")),collapse="_"),sep="_"))
	} else if(args$trans == 'log') {
		cat("calculating log transformation\n")
		out[,paste(args$pheno_col,"_log",sep="")]<-log(out[,args$pheno_col])
		pcsin <- pcs_include(d = out, y = paste(args$pheno_col,"_log",sep=""), cv = covars_analysis, n = n_pcs)
		out_cols <- c(out_cols,paste(args$pheno_col,"_log",sep=""))
	} else {
		cat("no transformation will be applied\n")
		pcsin <- pcs_include(d = out, y = args$pheno_col, cv = covars_analysis, n = n_pcs)
	}

	pc_outliers <- c()
	for(pc in pcsin) {
		pc_mean <- mean(out[,pc])
		pc_sd <- sd(out[,pc])
		lo <- pc_mean - args$n_stddevs * pc_sd
		hi <- pc_mean + args$n_stddevs * pc_sd
		pc_outliers <- c(pc_outliers,out[,args$iid_col][out[,pc] < lo | out[,pc] > hi])
	}
	if(length(pc_outliers) == 0) {
		break
	} else {
		samples_remove <- c(samples_remove, pc_outliers)
	}
	iter <- iter + 1
}
if(length(samples_remove) > 0) {
	cat("the following",length(samples_remove),"PC outliers were removed after",iter,"rounds of outlier removal\n")
	for(o in samples_remove) {
		cat(o,"\n")
	}
	id_map$removed_pc_outlier[which(id_map$ID %in% samples_remove)] <- 1
} else {
	cat("no PC outliers were found\n")
}

write.table(id_map, args$out_id_map, row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE, sep="\t")

if(args$min_pcs > length(pcsin)) {
	cat("setting minimum number of PCs to",args$min_pcs,"\n")
	pcsin <- paste("PC",seq(1,args$min_pcs,1),sep="")
}

if(length(pcsin) > 0) {
	cat(paste("   include PCs ",paste(pcsin,collapse="+")," in association testing",sep=""),"\n")
} else {
	cat("   no PCs to be included in association testing","\n")
}
write.table(pcsin,args$out_pcs,row.names=F,col.names=F,quote=F,sep="\t",append=F)

out_cols <- c(out_cols,paste("PC",seq(1,n_pcs,1),sep=""))

cat("writing phenotype file","\n")
write.table(out[,out_cols],args$out_pheno,row.names=F,col.names=T,quote=F,sep="\t",append=F, na="NA")
