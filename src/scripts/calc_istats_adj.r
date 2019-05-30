library(reshape2)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--sampleqc-stats", dest="sampleqc_stats", type="character", help="A sample qc stats file")
parser$add_argument("--n-pcs", dest="n_pcs", type='integer', default = 10, help="number of PCs to adjust for in residual calculation")
parser$add_argument("--covars", dest="covars", default = "", type="character", help="a '+' separated list of covariates to adjust for in residual calculation")
parser$add_argument("--pheno-in", dest="pheno_in", type="character", help="a phenotype file")
parser$add_argument("--iid-col", dest="iid_col", help='a column name for sample ID in phenotype file')
parser$add_argument("--pca-scores", dest="pca_scores", type="character", help="A file containing PCA scores")
parser$add_argument("--out", dest="out", type="character", help="an output file name")
args<-parser$parse_args()

print(args)

data<-read.table(args$sampleqc_stats,header=T,as.is=T,stringsAsFactors=F)

if(args$covars != "") {
	cat("removing factor indicators from covariates\n")
	covars <- gsub("\\]","",gsub("\\[","",unlist(strsplit(args$covars,split="\\+"))))
} else {
	covars <- c()
}

cat("extracting model specific columns from phenotype file\n")
pheno<-read.table(args$pheno_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
cat(paste0("extracting model specific columns: ", paste(c(args$iid_col, covars), collapse=",")),"\n")
pheno<-pheno[,c(args$iid_col, covars)]
names(pheno)[1] <- "IID"

pcs<-read.table(args$pca_scores,header=T,as.is=T,stringsAsFactors=F)
pcs$POP<-NULL
pcs$GROUP<-NULL
out <- merge(data, pcs, all.y=T)
out <- merge(out, pheno, all.x=T)

failed <- FALSE
covars_factors <- unlist(strsplit(args$covars,split="\\+"))
for(cv in covars_factors) {
	cvv <- unlist(strsplit(cv,split=""))
	if(cvv[1] == "[" && cvv[length(cvv)] == "]") {
		cvb<-paste(cvv[2:(length(cvv)-1)],collapse="")
		if(length(unique(pheno[,cvb])) == 1) {
			cat(paste0("covariate ",cvb," has zero variance\n"))
			failed <- TRUE
		} else {
			pheno[,cvb] <- as.factor(pheno[,cvb])
			covars_factors <- c(covars_factors,paste0("factor(",cvb,")"))
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
covars_analysis<-paste(c(covars_factors,"1",paste0("PC",seq(args$n_pcs))),collapse="+")

for(x in names(out)[! names(out) %in% c(names(out)[grep("^PC", names(out))], "IID", unlist(strsplit(covars_analysis, split="\\+")))]) {
	print(paste("var(",x,") = ",var(out[,x])),sep="")
	if(var(out[,x]) != 0) {
		g <- glm(eval(parse(text=paste(x,"~",covars_analysis,sep=""))),data=out,family="gaussian")
		print(summary(g))
		out$res<-g$residuals
		names(out)[names(out) == "res"]<-paste(x,"_res",sep="")
	}
}
write.table(out[,c("IID",names(out)[grep("_res",names(out))])],args$out,row.names=F,col.names=T,sep="\t",quote=F,append=F)
