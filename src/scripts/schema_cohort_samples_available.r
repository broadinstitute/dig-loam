library(argparse)
library(reshape2)

parser <- ArgumentParser()
parser$add_argument("--pheno-in", dest="pheno_in", type="character", help="a pheno file")
parser$add_argument("--fam-in", dest="fam_in", type="character", help="a fam file for genotyped samples (ie pre-qc)")
parser$add_argument("--ancestry-in", dest="ancestry_in", type="character", help="an ancestry file")
parser$add_argument("--strat", nargs=4, action = 'append', dest="strat", type="character", help="ancestry, strat column, and strat codes")
parser$add_argument("--iid-col", dest="iid_col", help='a column name for sample ID in phenotype file')
parser$add_argument("--samples-exclude", dest="samples_exclude", type="character", help="a list of sample IDs to exclude")
parser$add_argument("--out-id-map", dest="out_id_map", type="character", help="an output filename for the id removal map")
parser$add_argument("--out-cohorts-map", dest="out_cohorts_map", type="character", help="an output filename for the cohort sample map")
parser$add_argument("--out", dest="out", type="character", help="a sample list filename")
args<-parser$parse_args()

print(args)

cat("read in pheno file\n")
pheno<-read.table(args$pheno_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")

cat("reading inferred ancestry from file\n")
ancestry<-read.table(args$ancestry_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
names(ancestry)[1]<-args$iid_col
names(ancestry)[2]<-"ANCESTRY_INFERRED"
pheno<-merge(pheno,ancestry,all.x=T)

anc_keep <- c()
samples_keep <- c()
cohorts_map <- data.frame(IID = pheno[,args$iid_col], cohort = NA)
names(cohorts_map)[1] <- args$iid_col
for(i in 1:nrow(args$strat)) {
	s <- args$strat[i,]
	cohort <- s[1]
	anc <- unlist(strsplit(s[2],","))
    stratcol <- s[3]
	stratcol_vals <- unlist(strsplit(s[4],split=","))
	anc_keep <- c(anc_keep, anc)
	if(stratcol != "N/A" & paste(stratcol_vals, collapse=",") != "N/A") {
		if(! stratcol %in% names(pheno)) {
			cat(paste0("exiting due to strat col ", stratcol, " missing from pheno file"),"\n")
			quit(status=1)
		}
		extract <- pheno[,args$iid_col][which((pheno$ANCESTRY_INFERRED %in% anc) & (pheno[,stratcol] %in% stratcol_vals))]
		cat(paste0("found ",as.character(length(extract))," samples with inferred ancestry in ",paste(anc,collapse=",")," and ",paste(stratcol_vals,collapse="")," in strat col ",stratcol),"\n")
	} else {
		extract <- pheno[,args$iid_col][which((pheno$ANCESTRY_INFERRED %in% anc))]
		cat(paste0("found ",as.character(length(extract))," samples with inferred ancestry in ",paste(anc,collapse=",")),"\n")
	}
	samples_keep <- c(samples_keep, extract)
	cohorts_map$cohort[cohorts_map[,args$iid_col] %in% extract] <- cohort
}
pheno <- pheno[pheno[,args$iid_col] %in% samples_keep,]
cat(paste0("extracted ",as.character(nrow(pheno))," samples for this schema"),"\n")

cat("reading in genotyped samples IDs\n")
fam<-read.table(args$fam_in,header=F,as.is=T,stringsAsFactors=F,sep="\t")
iids<-fam$V2

id_map <- data.frame(ID = pheno[,args$iid_col])
id_map$removed_nogeno <- 0
id_map$removed_excluded <- 0
id_map$removed_nogeno[which(! id_map$ID %in% iids)] <- 1
pheno <- pheno[which(pheno[,args$iid_col] %in% iids),]

cat("read in sample IDs to exclude\n")
if(args$samples_exclude != "") {
	samples_excl<-scan(file=args$samples_exclude,what="character")
	pheno <- pheno[which(! pheno[,args$iid_col] %in% samples_excl),]
	id_map$removed_excluded[which((id_map$removed_nogeno == 0) & (id_map$ID %in% samples_excl))] <- 1
	cat(paste0("removed ",as.character(length(id_map$removed_excluded[which(id_map$removed_excluded == 1)]))," samples that were excluded"),"\n")
}

cohorts_map <- cohorts_map[cohorts_map[,args$iid_col] %in% pheno[,args$iid_col],]

write.table(cohorts_map, args$out_cohorts_map, row.names=FALSE, col.names=FALSE, quote=FALSE, append=FALSE, sep="\t")

write.table(id_map, args$out_id_map, row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE, sep="\t")

write.table(pheno[,args$iid_col],args$out, row.names=FALSE, col.names=FALSE, quote=FALSE, append=FALSE, sep="\t")
