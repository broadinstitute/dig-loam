library(argparse)

parser <- ArgumentParser()
parser$add_argument("--fam-in", dest="fam_in", type="character", help="a fam file")
parser$add_argument("--ancestry-in", dest="ancestry_in", type="character", help="an ancestry file")
parser$add_argument("--ancestry-keep", dest="ancestry_keep", type="character", help="a comma separated list of population groups to keep (ie. EUR,AFR)")
parser$add_argument("--pheno-in", dest="pheno_in", type="character", help="a phenotype file")
parser$add_argument("--iid-col", dest="iid_col", help='a column name for sample ID in phenotype file')
parser$add_argument("--strat-col", dest="strat_col", type="character", help="a phenotype file column name")
parser$add_argument("--strat-codes", dest="strat_codes", type="character", help="a list of values in --strat-col")
parser$add_argument("--out", dest="out", type="character", help="a sample list filename")
args<-parser$parse_args()

print(args)

cat("read in pheno file\n")
pheno<-read.table(args$pheno_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")

if(! is.null(args$strat_col) & ! is.null(args$strat_codes)) {
	cat("filter pheno file based on strat column\n")
	if(! args$strat_col %in% names(pheno)) {
		cat("exiting due to strat col missing from pheno file\n")
		quit(status=1)
	}
	pheno<-pheno[which(pheno[,args$strat_col] %in% unlist(strsplit(args$strat_codes,split=","))),]
	cat(paste0("extracted ",as.character(nrow(pheno))," samples with ",args$strat_codes," in strat col ",args$strat_col),"\n")
}

cat("reading inferred ancestry from file\n")
ancestry<-read.table(args$ancestry_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
names(ancestry)[1]<-args$iid_col
names(ancestry)[2]<-"ANCESTRY_INFERRED"
pheno<-merge(pheno,ancestry,all.x=T)
if(! is.null(args$ancestry_keep)) {
	anc_keep = unlist(strsplit(args$ancestry_keep,","))
	pheno <- pheno[pheno$ANCESTRY_INFERRED %in% anc_keep,]
	cat(paste0("extracted ",as.character(nrow(pheno))," samples in inferred population group/s ",paste(anc_keep,collapse="+")),"\n")
}

cat("reading in fam file\n")
fam<-read.table(args$fam_in,header=F,as.is=T,stringsAsFactors=F,sep="\t")

pheno<-pheno[pheno[,args$iid_col] %in% fam$V2,]
cat(paste0("extracted ",as.character(nrow(pheno))," samples in genotype fam file"),"\n")

write.table(pheno[,c(args$iid_col,args$iid_col)],args$out,row.names=F,col.names=F,quote=F,sep="\t",append=F)
