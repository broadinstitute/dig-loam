library(argparse)
library(plyr)

parser <- ArgumentParser()
parser$add_argument("--pheno", dest="pheno", type="character", help="a preliminary phenotype file")
parser$add_argument("--pcs", dest="pcs", type="character", help="a file containing list of pcs to include as covariates")
parser$add_argument("--pheno-col", dest="pheno_col", type="character", help="a column name for phenotype")
parser$add_argument("--iid-col", dest="iid_col", type="character", help='a column name for sample ID in phenotype file')
parser$add_argument("--sex-col", dest="sex_col", type="character", help='a column name for sample sex in phenotype file')
parser$add_argument("--male-code", dest="male_code", type="character", help='--sex-col value for male samples')
parser$add_argument("--female-code", dest="female_code", type="character", help='--sex-col value for female samples')
parser$add_argument("--ped", dest="ped", type="character", help="a ped output filename")
args<-parser$parse_args()

print(args)

cat("read in phenotype file\n")
pheno<-read.table(args$pheno, header=T, as.is=T, stringsAsFactors=F, sep="\t")
out_cols<-colnames(pheno)

cat("read in pcs to include\n")
pcs<-scan(file = args$pcs,what="character")

merlin_header <- c(paste0("#FAM_ID_",args$iid_col), paste0("IND_ID_", args$iid_col), "FAT_ID", "MOT_ID", args$sex_col, args$pheno_col)
header <- c(merlin_header, colnames(pheno)[! colnames(pheno) %in% c(args$iid_col, merlin_header)])

pheno$FAT_ID <- 0
pheno$MOT_ID <- 0

names(pheno)[names(pheno) == args$iid_col] <- paste0("#FAM_ID_", args$iid_col)
pheno[, paste0("IND_ID_", args$iid_col)] <- pheno[, paste0("#FAM_ID_", args$iid_col)]

pheno[,args$sex_col] <- mapvalues(pheno[,args$sex_col], from = c(args$male_code, args$female_code), to = c(1, 2))

pheno<-pheno[,header]

cat("writing phenotype file","\n")
write.table(pheno, args$ped, row.names = F,col.names = T,quote = F,sep = "\t", append = F, na = "NA")
