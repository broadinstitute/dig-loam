library(argparse)
library(plyr)

parser <- ArgumentParser()
parser$add_argument("--pheno", dest="pheno", type="character", help="a preliminary phenotype file")
parser$add_argument("--pcs", dest="pcs", type="character", help="a file containing list of pcs to include as covariates")
parser$add_argument("--pheno-table", dest="pheno_table", type="character", help="a phenotype table file")
parser$add_argument("--iid-col", dest="iid_col", type="character", help='a column name for sample ID in phenotype file')
parser$add_argument("--batch", dest="batch", type="character", help='a batch number (integer)')
parser$add_argument("--covars-analyzed", dest="covars_analyzed", type="character", help="a '+' separated list of covariates used in analysis")
parser$add_argument("--pheno-out", dest="pheno_out", type="character", help="a ped output filename")
parser$add_argument("--covars-out", dest="covars_out", type="character", help="a ped output filename")
args<-parser$parse_args()

print(args)

cat("read in phenotype file\n")
pheno<-read.table(args$pheno, header=T, as.is=T, stringsAsFactors=F, sep="\t")
out_cols<-colnames(pheno)

cat("read in pcs to include\n")
pcs<-scan(file = args$pcs,what="character")

covars_analyzed <- unlist(strsplit(args$covars_analyzed,split="\\+"))
for(cv in covars_analyzed) {
	cvv <- unlist(strsplit(cv,split=""))
	if(cvv[1] == "[" && cvv[length(cvv)] == "]") {
		cvb<-paste(cvv[2:(length(cvv)-1)],collapse="")
		for(val in sort(unique(pheno[,cvb]))[2:length(sort(unique(pheno[,cvb])))]) {
			pheno[,paste0(cvb,val)] <- 0
			pheno[,paste0(cvb,val)][which(pheno[,cvb] == val)] <- 1
			covars_analyzed <- c(covars_analyzed,paste0(cvb,val))
		}
		covars_analyzed <- covars_analyzed[covars_analyzed != cv]
	}
}
covars_analyzed <- c(covars_analyzed,pcs)

cat(paste0("read in pheno table from file and selecting batch ",args$batch))
phenoTable<-read.table(args$pheno_table,header=T,as.is=T,stringsAsFactors=F,sep="\t")
phenoTable<-phenoTable[phenoTable$batch == args$batch,]

pheno_df_out<-pheno[,c(args$iid_col, phenoTable$idAnalyzed)]
if(! "FID" %in% names(pheno_df_out)) {
	pheno_df_out$FID<-pheno_df_out[,args$iid_col]
}
if(! "IID" %in% names(pheno_df_out)) {
	pheno_df_out$IID<-pheno_df_out[,args$iid_col]
}
pheno_df_out<-pheno_df_out[,c("FID","IID",phenoTable$idAnalyzed)]

cat("writing phenotype file","\n")
write.table(pheno_df_out, args$pheno_out, row.names = F,col.names = T,quote = F,sep = "\t", append = F, na = "NA")

if(length(covars_analyzed) > 0) {
	covars_df_out<-pheno[,c(args$iid_col, covars_analyzed)]
	if(! "FID" %in% names(covars_df_out)) {
		covars_df_out$FID<-covars_df_out[,args$iid_col]
	}
	if(! "IID" %in% names(covars_df_out)) {
		covars_df_out$IID<-covars_df_out[,args$iid_col]
	}
	covars_df_out<-covars_df_out[,c("FID","IID",covars_analyzed)]

	cat("writing covariates file","\n")
	write.table(covars_df_out, args$covars_out, row.names = F,col.names = T,quote = F,sep = "\t", append = F, na = "NA")
} else {
	write.table(pheno_df_out[,c("FID","IID")], args$covars_out, row.names = F,col.names = T,quote = F,sep = "\t", append = F, na = "NA")
}

