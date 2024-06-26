library(argparse)

set.seed(1)

parser <- ArgumentParser()
parser$add_argument("--pheno-in", dest="pheno_in", type="character", help="a preliminary phenotype file")
parser$add_argument("--pcs-in", dest="pcs_in", type="character", help="pca score file")
parser$add_argument("--iid-col", dest="iid_col", type="character", help='a column name for sample ID in phenotype file')
parser$add_argument("--pheno-table", dest="pheno_table", type="character", help="a pheno table file")
parser$add_argument("--binary", action='store_true', dest="binary", help="a flag to indicate if binary phenotype")
parser$add_argument("--covars", dest="covars", type="character", help="a '+' separated list of covariates")
parser$add_argument("--min-pcs", dest="min_pcs", type="integer", help="minimum number of pcs to include in analysis")
parser$add_argument("--max-pcs", dest="max_pcs", type="integer", help="maximum number of pcs to include in analysis")
parser$add_argument("--n-stddevs", dest="n_stddevs", type="integer", help="outlier detection threshold in number of standard deviations from the mean")
parser$add_argument("--out-pheno", dest="out_pheno", type="character", help="a phenotype output filename")
parser$add_argument("--out-pcs-include", dest="out_pcs_include", type="character", help="a pc inclusion filename")
parser$add_argument("--out-outliers", dest="out_outliers", type="character", help="an outlier filename")
args<-parser$parse_args()

print(args)

# standardization
STDZ <- function (x) {
	n <- length(x)
	x <- x - mean(x)
	sd <- sqrt( sum(x^2) / n)
	x / sd
}

# rank-based inverse normalization
INVN <- function(x){
	return(STDZ(qnorm((rank(x,na.last="keep") - 0.5)/sum(!is.na(x)))))
}

pcs_include_quant <- function(d, y, cv, n) {
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

pcs_include_binary <- function(d, y, cv, n) {
	if(cv != "") {
		m <- summary(glm(as.formula(paste(y,"~",cv,"+",paste(paste("PC",seq(1,n,1),sep=""),collapse="+"),sep="")),data=d,family="binomial"))
	} else {
		m <- summary(glm(as.formula(paste(y,"~",paste(paste("PC",seq(1,n,1),sep=""),collapse="+"),sep="")),data=d,family="binomial"))
	}
	print(m)
	mc <- as.data.frame(m$coefficients)
	s <- rownames(mc[mc[,"Pr(>|z|)"] <= 0.05,])
	spcs <- s[grep("^PC",s)]
	if(length(spcs) > 0) {
		mpc <- max(as.integer(gsub("PC","",spcs)))
		inpcs <- paste("PC",seq(1,mpc,1),sep="")
	} else {
		inpcs<-c()
	}
	return(inpcs)
}

if(args$covars != "___NONE___") {
	cat("removing factor indicators from covariates\n")
	covars <- gsub("\\]","",gsub("\\[","",unlist(strsplit(args$covars,split="\\+"))))
} else {
	covars <- c()
}

cat("read in preliminary phenotype file\n")
pheno<-read.table(args$pheno_in,header=T,as.is=T,stringsAsFactors=F,sep="\t",colClasses=c(eval(parse(text=paste0(args$iid_col,"=\"character\"")))))
out_cols<-colnames(pheno)

cat("read in pheno table from file")
phenoTable<-read.table(args$pheno_table,header=T,as.is=T,stringsAsFactors=F,sep="\t")
phenoTable[is.na(phenoTable)]<-"NA"
pheno_cols <- phenoTable$id

for(p in pheno_cols) {
	failed <- FALSE
	if(length(unique(pheno[,p])) == 1) {
		cat(paste0("phenotype ",p," has zero variance\n"))
		failed <- TRUE
	}
}
if(length(covars) > 0) {
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
					pheno[,paste0(cvb,"_",val)] <- 0
					pheno[,paste0(cvb,"_",val)][which(pheno[,cvb] == val)] <- 1
					covars_factors <- c(covars_factors,paste0(cvb,"_",val))
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
} else {
	covars_factors <- c()
}
if(failed) {
	cat("exiting due to invalid model\n")
	quit(status=1)
}
covars_analysis<-paste(c(covars_factors,"1"),collapse="+")
if(length(covars_factors) > 0) {
	out_cols<-c(out_cols,covars_factors[! covars_factors %in% out_cols])
}

cat("read in pcs and merge pheno into them\n")
pcs<-read.table(args$pcs_in,header=T,as.is=T,stringsAsFactors=F,sep="\t",colClasses=c("IID"="character"))
pcs$FID<-NULL
names(pcs)[1]<-args$iid_col
out<-merge(pheno,pcs,all.y=T)

cat("convert all model vars to numeric\n")
for(cv in c(pheno_cols,unlist(strsplit(covars_analysis,split="\\+")))) {
	if(cv %in% names(out)) {
		out[,cv]<-as.numeric(as.character(out[,cv]))
	}
}

if(ncol(pcs)-1 < args$max_pcs) {
	n_pcs <- ncol(pcs)-1
} else {
	n_pcs <- args$max_pcs
}

for(i in 1:nrow(phenoTable)) {
	modelPheno<-phenoTable$id[i]
	modelTrans<-phenoTable$trans[i]
	modelBinary<-phenoTable$binary[i]

	if(modelBinary == "false") {
		if(modelTrans == 'invn') {
			cat("calculating invn transformation\n")
			if(length(unique(out$ANCESTRY_INFERRED)) > 1) {
				cat(paste("including inferred ancestry as indicator in calculation of residuals",sep=""),"\n")
				mf <- summary(lm(as.formula(paste(modelPheno,"~factor(ANCESTRY_INFERRED)+",covars_analysis,sep="")),data=out))
			} else {
				mf <- summary(lm(as.formula(paste(modelPheno,"~",covars_analysis,sep="")),data=out))
			}
			out[,paste0(modelPheno,"_invn")]<-INVN(residuals(mf))
			out_cols <- c(out_cols,paste0(modelPheno,"_invn"))
		} else if(modelTrans == 'log') {
			cat("calculating log transformation\n")
			nzero <- nrow(out[out[,modelPheno] == 0,])
			if(nzero > 0) {
				cat("removing ",nzero," 0 values to allow for log transformation\n")
				out[,modelPheno][out[,modelPheno] == 0]<-NA
			}
			out[,paste0(modelPheno,"_log")]<-log(out[,modelPheno])
			out_cols <- c(out_cols,paste0(modelPheno,"_log"))
		} else {
			cat("no transformation will be applied\n")
		}
	} else {
		cat("no transformation will be applied\n")
	}
}

pc_outliers <- c()
pcsin <- c()
if(nrow(phenoTable) == 1 & n_pcs > 0) {
	modelPheno<-phenoTable$id[1]
	modelTrans<-phenoTable$trans[1]
	modelBinary<-phenoTable$binary[1]
	cat("calculating pcs to include\n")
	if(modelBinary == "false") {
		if(modelTrans == 'invn') {
			pcsin <- pcs_include_quant(d = out, y = paste0(modelPheno,"_invn"), cv = "", n = n_pcs)
		} else if(modelTrans == 'log') {
			pcsin <- pcs_include_quant(d = out, y = paste0(modelPheno,"_log"), cv = covars_analysis, n = n_pcs)
		} else {
			pcsin <- pcs_include_quant(d = out, y = modelPheno, cv = covars_analysis, n = n_pcs)
		}
	} else {
		pcsin <- pcs_include_binary(d = out, y = modelPheno, cv = covars_analysis, n = n_pcs)
	}

	for(pc in pcsin) {
		pc_mean <- mean(out[,pc])
		pc_sd <- sd(out[,pc])
		lo <- pc_mean - args$n_stddevs * pc_sd
		hi <- pc_mean + args$n_stddevs * pc_sd
		pc_outliers <- c(pc_outliers,out[,args$iid_col][out[,pc] < lo | out[,pc] > hi])
	}
	pc_outliers <- unique(pc_outliers)
}

if(length(pc_outliers) > 0) {
	cat(length(pc_outliers)," PC outliers were removed\n")
} else {
	cat("no PC outliers were found\n")
}
write.table(pc_outliers,args$out_outliers,row.names=F,col.names=F,quote=F,sep="\t",append=F)

if(args$min_pcs > length(pcsin)) {
	cat("setting minimum number of PCs to",args$min_pcs,"\n")
	pcsin <- paste("PC",seq(1,args$min_pcs,1),sep="")
}

if(length(pcsin) > 0) {
	cat(paste("   include PCs ",paste(pcsin,collapse="+")," in association testing",sep=""),"\n")
} else {
	cat("   no PCs to be included in association testing","\n")
}

write.table(pcsin,args$out_pcs_include,row.names=F,col.names=F,quote=F,sep="\t",append=F)

if(n_pcs > 0) {
	out_cols <- c(out_cols,paste("PC",seq(1,n_pcs,1),sep=""))
}

cat("writing phenotype file","\n")
write.table(out[,out_cols],args$out_pheno,row.names=F,col.names=T,quote=F,sep="\t",append=F, na="NA")
