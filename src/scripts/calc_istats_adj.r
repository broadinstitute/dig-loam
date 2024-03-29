library(reshape2)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--sampleqc-stats", dest="sampleqc_stats", type="character", help="A sample qc stats file")
parser$add_argument("--n-pcs", dest="n_pcs", type='integer', default = 10, help="number of PCs to adjust for in residual calculation")
parser$add_argument("--covars", dest="covars", default = "", type="character", help="a '+' separated list of covariates to adjust for in residual calculation")
parser$add_argument("--sample-file", dest="sample_file", type="character", help="a sample file")
parser$add_argument("--iid-col", dest="iid_col", help='a column name for sample ID in sample file')
parser$add_argument("--pca-scores", dest="pca_scores", type="character", help="A file containing PCA scores")
parser$add_argument("--incomplete-obs", dest="incomplete_obs", type="character", help="an output file name for incomplete observations")
parser$add_argument("--out", dest="out", type="character", help="an output file name for adjusted metrics")
args<-parser$parse_args()

print(args)

data<-read.table(args$sampleqc_stats,header=T,as.is=T,stringsAsFactors=F,colClasses=c("IID"="character"))
metrics <- names(data)[which(names(data) != "IID")]

if(args$covars != "") {
	cat("removing factor indicators from covariates\n")
	covars <- gsub("\\]","",gsub("\\[","",unlist(strsplit(args$covars,split="\\+"))))
} else {
	covars <- c()
}

cat("extracting model specific columns from phenotype file\n")
pheno<-read.table(args$sample_file,header=T,as.is=T,stringsAsFactors=F,sep="\t",colClasses=c(eval(parse(text=paste0(args$iid_col,"=\"character\"")))))
cat(paste0("extracting model specific columns: ", paste(c(args$iid_col, covars), collapse=",")),"\n")
pheno<-pheno[,c(args$iid_col, covars), drop=FALSE]
names(pheno)[1] <- "IID"

pcs<-read.table(args$pca_scores,header=T,as.is=T,stringsAsFactors=F,colClasses=c("IID"="character"))
pcs$FID<-NULL
out <- merge(data, pcs, all.x=T)
out <- merge(out, pheno, all.x=T)

if(args$covars != "") {
	covars_factors <- unlist(strsplit(args$covars,split="\\+"))
	for(cv in covars_factors) {
		cvv <- unlist(strsplit(cv,split=""))
		if(cvv[1] == "[" && cvv[length(cvv)] == "]") {
			cvb<-paste(cvv[2:(length(cvv)-1)],collapse="")
			if(length(unique(out[,cvb])) == 1) {
				cat(paste0("removing covariate ",cvb," with zero variance\n"))
			} else {
				out[,cvb] <- as.factor(out[,cvb])
				covars_factors <- c(covars_factors,paste0("factor(",cvb,")"))
			}
			covars_factors <- covars_factors[covars_factors != cv]
		} else {
			if(length(unique(out[,cv])) == 1) {
				cat(paste0("removing covariate ",cv," with zero variance\n"))
			}
		}
	}
} else {
	covars_factors <- c()
}
covars_analysis<-paste(c(covars_factors,"1",paste0("PC",seq(args$n_pcs))),collapse="+")

if(length(covars) > 0) {
	incomplete_covars <- out[! complete.cases(out[,covars]),]
} else {
	incomplete_covars <- out[NULL,]
}

print(paste0("removing ",length(incomplete_covars$IID)," samples due to incomplete covariate observations"))
out_check <- out[! out$IID %in% incomplete_covars$IID,]

metrics_use<-c()
print("excluding any metric x with var(x) == 0 or missingness(x) >= 0.02")
for(x in metrics) {
	metric_var = var(out_check[,x], na.rm=TRUE)
	metric_miss = nrow(out_check[is.na(out_check[,x]),])/nrow(out_check)
	metric_message<-paste0("var = ",metric_var,",  missingness = ",metric_miss)
	if(metric_var == 0 | metric_miss >= 0.02) {
		print(paste0("...excluding metric ",x,": ",metric_message))
	} else {
		print(paste0("...including metric ",x,": ",metric_message))
		
		metrics_use <- c(metrics_use,x)
	}
}

incomplete_metrics <- out_check[! complete.cases(out_check[,metrics_use]),]

print(paste0("removing ",length(incomplete_metrics$IID)," samples due to incomplete sample metric observations"))

if(length(covars) > 0) {
	cols_incl <- c(covars,metrics_use)
} else {
	cols_incl <- metrics_use
}
incomplete_obs <- out[! complete.cases(out[,cols_incl]),]

if(nrow(incomplete_obs) > 0) {
	write.table(incomplete_obs,args$incomplete_obs,row.names=F,col.names=T,quote=F,sep="\t",append=F)
} else {
	cat("", file = args$incomplete_obs)
}

out <- out[! out$IID %in% incomplete_obs,]

for(x in metrics_use) {
	print(paste("var(",x,") = ",var(out[,x])),sep="")
	if(nrow(out[! is.na(out[,x]),]) > 0) {
		if(var(out[,x]) != 0) {
			g <- glm(eval(parse(text=paste(x,"~",covars_analysis,sep=""))),data=out,family="gaussian")
			print(summary(g))
			out$res<-g$residuals
			names(out)[names(out) == "res"]<-paste(x,"_res",sep="")
		}
	}
}
write.table(out[,c("IID",names(out)[grep("_res",names(out))])],args$out,row.names=F,col.names=T,sep="\t",quote=F,append=F)
