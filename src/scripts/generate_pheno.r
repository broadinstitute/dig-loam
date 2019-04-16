library(GENESIS)
library(SNPRelate)
library(GWASTools)
library(gdsfmt)
library(pryr)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--cpus", dest="cpus", type="character", help="number of cpus to use for King")
parser$add_argument("--gds-in", dest="gds_in", type="character", help="a plink binary file path")
parser$add_argument("--pheno-in", dest="pheno_in", type="character", help="a phenotype file")
parser$add_argument("--ancestry-in", dest="ancestry_in", type="character", help="an ancestry file")
parser$add_argument("--ancestry-keep", dest="ancestry_keep", type="character", help="a comma separated list of population groups to keep (ie. EUR,AFR)")
parser$add_argument("--pheno-col", dest="pheno_col", type="character", help="a column name for phenotype")
parser$add_argument("--iid-col", dest="iid_col", help='a column name for sample ID in phenotype file')
parser$add_argument("--samples-include", dest="samples_include", type="character", help="a final list of sample IDs to include")
parser$add_argument("--samples-exclude", dest="samples_exclude", type="character", help="a list of sample IDs to exclude")
parser$add_argument("--variants-exclude", dest="variants_exclude", default=NULL, type="character", help="variant IDs file")
parser$add_argument("--test", dest="test", type="character", help="a test code")
parser$add_argument("--trans", dest="trans", type="character", help="a comma separated list of transformation codes")
parser$add_argument("--covars", dest="covars", type="character", help="a '+' separated list of covariates")
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
	mc <- m$coefficients
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
pheno<-pheno[,c(args$iid_col, args$pheno_col, covars)]
pheno<-pheno[complete.cases(pheno),]
out_cols<-colnames(pheno)

covars_factors <- unlist(strsplit(args$covars,split="\\+"))
for(cv in covars_factors) {
	cvv <- unlist(strsplit(cv,split=""))
	if(cvv[1] == "[" && cvv[length(cvv)] == "]") {
		cvb<-paste(cvv[2:(length(cvv)-1)],collapse="")
		for(val in sort(unique(pheno[,cvb]))[2:length(sort(unique(pheno[,cvb])))]) {
			pheno[,paste0(cvb,val)] <- 0
			pheno[,paste0(cvb,val)][which(pheno[,cvb] == val)] <- 1
			covars_factors <- c(covars_factors,paste0(cvb,val))
		}
		covars_factors <- covars_factors[covars_factors != cv]
	}
}
covars_analysis<-paste(covars_factors,collapse="+")
out_cols<-c(out_cols,covars_factors[! covars_factors %in% out_cols])

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
iids <- getScanID(geno)
vids <- getSnpID(geno)
close(geno)

cat("generating sample and variant ID inclusion lists\n")
samples_incl<-scan(file=args$samples_include,what="character")
variants_excl<-scan(file=args$variants_exclude,what="character")
samples_incl<-iids[iids %in% samples_incl & iids %in% pheno[,args$iid_col]]

if(args$samples_exclude != "") {
	samples_excl<-scan(file=args$samples_exclude,what="character")
    samples_incl<-samples_incl[! samples_incl %in% samples_excl]
}

variants_incl<-vids[! vids %in% variants_excl]

kinship<-NULL
if(args$test == "lmm") {
	cat(paste("memory before running king: ",mem_used() / (1024^2),sep=""),"\n")
	cat("running King robust to get kinship matrix\n")
	kinship <- calc_kinship(gds = args$gds_in, sam = samples_incl, vin = variants_incl, t = args$cpus)
	cat(paste("memory after running king and before running pcair: ",mem_used() / (1024^2),sep=""),"\n")
}

geno <- GdsGenotypeReader(filename = args$gds_in)
genoData <- GenotypeData(geno)
iter <- 1
samples_remove <- c()
while(iter < 11) {
	cat("\n",paste0("pcair iteration #",iter),"\n")

	if(length(samples_incl[! samples_incl %in% samples_remove]) < 20) {
		n_pcs <- length(samples_incl[! samples_incl %in% samples_remove])
	} else {
		n_pcs <- 20
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
		lo <- pc_mean - 6 * pc_sd
		hi <- pc_mean + 6 * pc_sd
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
} else {
	cat("no PC outliers were found\n")
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
