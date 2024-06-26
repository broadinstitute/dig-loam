library(argparse)
library(plyr)

parser <- ArgumentParser()
parser$add_argument("--pheno", dest="pheno", type="character", help="a preliminary phenotype file")
parser$add_argument("--pcs", dest="pcs", type="character", help="a file containing list of pcs to include as covariates")
parser$add_argument("--pheno-analyzed", dest="pheno_analyzed", type="character", help="a column name for phenotype used in analysis")
parser$add_argument("--iid-col", dest="iid_col", type="character", help='a column name for sample ID in phenotype file')
parser$add_argument("--sex-col", dest="sex_col", type="character", help='a column name for sample sex in phenotype file')
parser$add_argument("--male-code", dest="male_code", type="character", help='--sex-col value for male samples')
parser$add_argument("--female-code", dest="female_code", type="character", help='--sex-col value for female samples')
parser$add_argument("--trans", dest="trans", type="character", help="a comma separated list of transformation codes")
parser$add_argument("--covars-analyzed", dest="covars_analyzed", type="character", help="a '+' separated list of covariates used in analysis")
parser$add_argument("--model-vars", dest="model_vars", type="character", help="a model vars output filename")
parser$add_argument("--ped", dest="ped", type="character", help="a ped output filename")
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
			pheno[,paste0(cvb,"_",val)] <- 0
			pheno[,paste0(cvb,"_",val)][which(pheno[,cvb] == val)] <- 1
			covars_analyzed <- c(covars_analyzed,paste0(cvb,"_",val))
		}
		covars_analyzed <- covars_analyzed[covars_analyzed != cv]
	}
}
covars_analyzed <- c(covars_analyzed,pcs)

cat(pheno_analyzed, "\n", file=args$model_vars, append=FALSE)
if(length(covars_analyzed) > 0) {
	cat(paste(covars_analyzed, collapse="\n"), "\n", file=args$model_vars, append=TRUE)
}

if(! is.null(args$sex_col)) {
	merlin_header <- c(paste0("#FAM_ID_",args$iid_col), paste0("IND_ID_", args$iid_col), "FAT_ID", "MOT_ID", args$sex_col, pheno_analyzed)
} else {
	merlin_header <- c(paste0("#FAM_ID_",args$iid_col), paste0("IND_ID_", args$iid_col), "FAT_ID", "MOT_ID", "SEX", pheno_analyzed)
}
header <- c(merlin_header, colnames(pheno)[! colnames(pheno) %in% c(args$iid_col, merlin_header, pcs)])

pheno$FAT_ID <- 0
pheno$MOT_ID <- 0

names(pheno)[names(pheno) == args$iid_col] <- paste0("#FAM_ID_", args$iid_col)
pheno[, paste0("IND_ID_", args$iid_col)] <- pheno[, paste0("#FAM_ID_", args$iid_col)]

if(! is.null(args$sex_col)) {
	pheno[,args$sex_col] <- mapvalues(pheno[,args$sex_col], from = c(args$male_code, args$female_code), to = c(1, 2))
} else {
	pheno$SEX <- 0
}

pheno<-pheno[,c(header,pcs)]

cat("writing phenotype file","\n")
write.table(pheno, args$ped, row.names = F,col.names = T,quote = F,sep = "\t", append = F, na = "NA")
