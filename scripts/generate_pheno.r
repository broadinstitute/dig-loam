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
parser$add_argument("--variants-exclude", nargs=2, dest="variants_exclude", default=NULL, type="character", help="A column name or 1-based number for variant IDs followed by the filename")
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

get_columns <- function(iid, p, cvs) {
	ck = c(iid,p)
	if(cvs != "") ck = c(ck, unlist(strsplit(cvs,"\\+")))
	return(ck)
}

pcs_include <- function(d, y, cv) {
	if(cv != "") {
		m <- summary(lm(as.formula(paste(y,"~",cv,"+",paste(paste("PC",seq(1,20,1),sep=""),collapse="+"),sep="")),data=d))
	} else {
		m <- summary(lm(as.formula(paste(y,"~",paste(paste("PC",seq(1,20,1),sep=""),collapse="+"),sep="")),data=d))
	}
	print(m)
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

print("extracting model specific columns from phenotype file")
pheno<-read.table(args$pheno_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
pheno<-pheno[,get_columns(iid = args$iid_col, p = args$pheno_col, cvs = args$covars)]
pheno<-pheno[complete.cases(pheno),]
out_cols<-colnames(pheno)

print("reading inferred ancestry from file")
ancestry<-read.table(args$ancestry_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
names(ancestry)[1]<-args$iid_col
names(ancestry)[2]<-"ANCESTRY_INFERRED"
pheno<-merge(pheno,ancestry,all.x=T)
print(names(pheno))
if(! is.null(args$ancestry_keep)) {
	anc_keep = unlist(strsplit(args$ancestry_keep,","))
	print(paste("keeping populations group/s",paste(anc_keep,collapse="+"),"for analysis",sep=" "))
	pheno <- pheno[pheno$ANCESTRY_INFERRED %in% anc_keep,]
}

print("reading sample and variant IDs from gds file")
geno <- GdsGenotypeReader(filename = args$gds_in)
iids <- getScanID(geno)
vids <- getSnpID(geno)
close(geno)

print("generating sample and variant ID inclusion lists")
samples_incl<-scan(file=args$samples_include,what="character")
variants_excl_df<-read.table(args$variants_exclude[2],header=T,as.is=T,stringsAsFactors=F,sep="\t")
variants_excl<-variants_excl_df[,args$variants_exclude[1]]
samples_incl<-iids[iids %in% samples_incl & iids %in% pheno[,args$iid_col]]

if(args$samples_exclude != "") {
	samples_excl<-scan(file=args$samples_exclude,what="character")
    samples_incl<-samples_incl[! samples_incl %in% samples_excl]
}

variants_incl<-vids[! vids %in% variants_excl]

kinship<-NULL
if(args$test == "lmm") {
	print(paste("memory before running king: ",mem_used() / (1024^2),sep=""))
	print("running King robust to get kinship matrix")
	kinship <- calc_kinship(gds = args$gds_in, sam = samples_incl, vin = variants_incl, t = 4)
	print(paste("memory after running king and before running pcair: ",mem_used() / (1024^2),sep=""))
}

print("running pcair")
geno <- GdsGenotypeReader(filename = args$gds_in)
genoData <- GenotypeData(geno)
mypcair <- pcair(genoData = genoData, scan.include = samples_incl, snp.include = variants_incl, kinMat = kinship, divMat = kinship, snp.block.size = 10000)
print(paste("memory after running pcair: ",mem_used() / (1024^2),sep=""))
pcs<-data.frame(mypcair$vectors)
names(pcs)[1:20]<-paste("PC",seq(1,20),sep="")
pcs[,args$iid_col]<-row.names(pcs)
out<-merge(pheno,pcs,all.y=T)

print("calculating transformations")
if(args$trans == 'invn') {
	print("performing invn transformation")
	if(length(unique(out$ANCESTRY_INFERRED)) > 1) {
		print(paste("including inferred ancestry as indicator in calculation of residuals",sep=""))
		mf <- summary(lm(as.formula(paste(args$pheno_col,"~factor(ANCESTRY_INFERRED)+",args$covars,sep="")),data=out))
	} else {
		mf <- summary(lm(as.formula(paste(args$pheno_col,"~",args$covars,sep="")),data=out))
	}
	out[,paste(args$pheno_col,"invn",paste(unlist(strsplit(args$covars,"\\+")),collapse="_"),sep="_")]<-INVN(residuals(mf))
	pcsin <- pcs_include(d = out, y = paste(args$pheno_col,"invn",paste(unlist(strsplit(args$covars,"\\+")),collapse="_"),sep="_"), cv = "")
	out_cols <- c(out_cols,paste(args$pheno_col,"invn",paste(unlist(strsplit(args$covars,"\\+")),collapse="_"),sep="_"))
} else if(args$trans == 'log') {
	print("performing log transformation")
	out[,paste(args$pheno_col,"_log",sep="")]<-log(out[,args$pheno_col])
	pcsin <- pcs_include(d = out, y = paste(args$pheno_col,"_log",sep=""), cv = args$covars)
	out_cols <- c(out_cols,paste(args$pheno_col,"_log",sep=""))
} else {
	print("no transformation will be applied")
	pcsin <- pcs_include(d = out, y = args$pheno_col, cv = args$covars)
}
if(length(pcsin) > 0) {
	print(paste("include PCs ",paste(pcsin,collapse="+")," in association testing",sep=""))
} else {
	print("no PCs to be included in association testing")
}

out_cols <- c(out_cols,paste("PC",seq(1,20,1),sep=""))
print("writing phenotype file")
write.table(out[,out_cols],args$out_pheno,row.names=F,col.names=T,quote=F,sep="\t",append=F, na="NA")
write.table(pcsin,args$out_pcs,row.names=F,col.names=F,quote=F,sep="\t",append=F)
