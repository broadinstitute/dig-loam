library(GENESIS)
library(SNPRelate)
library(GWASTools)
library(gdsfmt)
library(pryr)
library(argparse)

set.seed(1)

parser <- ArgumentParser()
parser$add_argument("--pheno-in", dest="pheno_in", type="character", help="a phenotype file")
parser$add_argument("--ancestry-in", dest="ancestry_in", type="character", help="an ancestry file")
parser$add_argument("--ancestry-keep", dest="ancestry_keep", type="character", help="a comma separated list of population groups to keep (ie. EUR,AFR)")
parser$add_argument("--pheno-col", dest="pheno_col", type="character", help="a column name for phenotype")
parser$add_argument("--iid-col", dest="iid_col", help='a column name for sample ID in phenotype file')
parser$add_argument("--strat-col", dest="strat_col", type="character", help="a phenotype file column name")
parser$add_argument("--strat-codes", dest="strat_codes", type="character", help="a list of values in --strat-col")
parser$add_argument("--sampleqc-in", dest="sampleqc_in", type="character", help="a sampleqc file")
parser$add_argument("--kinship-in", dest="kinship_in", type="character", help="a kinship file containing related pairs")
parser$add_argument("--samples-exclude-qc", dest="samples_exclude_qc", type="character", help="a list of sample IDs to exclude based on sample qc")
parser$add_argument("--samples-exclude-postqc", dest="samples_exclude_postqc", type="character", help="a list of sample IDs to exclude based on postqc filters")
parser$add_argument("--variants-exclude-postqc", dest="variants_exclude_postqc", default=NULL, type="character", help="a list of variant IDs to exclude based on post-qc filtering")
parser$add_argument("--test", dest="test", type="character", help="a test code")
parser$add_argument("--covars", dest="covars", type="character", help="a '+' separated list of covariates")
parser$add_argument("--out-id-map", dest="out_id_map", type="character", help="an output filename for the id removal map")
parser$add_argument("--out", dest="out", type="character", help="a sample list filename")
args<-parser$parse_args()

print(args)

cat("removing factor indicators from covariates\n")
covars <- gsub("\\]","",gsub("\\[","",unlist(strsplit(args$covars,split="\\+"))))

cat("extracting model specific columns from phenotype file\n")
pheno<-read.table(args$pheno_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
cat(paste0("extracting model specific columns: ", paste(c(args$iid_col, args$pheno_col, covars), collapse=",")),"\n")
pheno<-pheno[,c(args$iid_col, args$pheno_col, covars)]
out_cols<-colnames(pheno)

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

id_map <- data.frame(ID = pheno[,args$iid_col])
id_map$removed_nogeno <- 0
id_map$removed_sampleqc <- 0
id_map$removed_postqc_filters <- 0
id_map$removed_incomplete_obs <- 0
id_map$removed_kinship <- 0
id_map$removed_nogeno[which(! id_map$ID %in% iids)] <- 1
pheno <- pheno[which(pheno[,args$iid_col] %in% iids),]

cat("read in sample IDs excluded during sample qc\n")
if(args$samples_exclude_qc != "") {
	samples_excl<-scan(file=args$samples_exclude_qc,what="character")
	pheno <- pheno[which(! pheno[,args$iid_col] %in% samples_excl),]
	id_map$removed_sampleqc[which((id_map$removed_nogeno == 0) & (id_map$ID %in% samples_excl))] <- 1
	cat(paste0("removed ",as.character(length(id_map$removed_sampleqc[which(id_map$removed_sampleqc == 1)]))," samples that did not pass sample qc"),"\n")
}

cat("read in sample IDs excluded during post-qc filtering\n")
if(args$samples_exclude_postqc != "") {
	samples_excl<-scan(file=args$samples_exclude_postqc,what="character")
	pheno <- pheno[which(! pheno[,args$iid_col] %in% samples_excl),]
	id_map$removed_postqc_filters[which((id_map$removed_nogeno == 0) & (id_map$removed_sampleqc == 0) & (id_map$ID %in% samples_excl))] <- 1
	cat(paste0("removed ",as.character(length(id_map$removed_postqc_filters[which(id_map$removed_postqc_filters == 1)]))," samples that did not pass post-qc filters"),"\n")
}

cat("reading in kinship values for related pairs\n")
kinship_in <- read.table(args$kinship_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
kinship_in <- kinship_in[which((kinship_in$ID1 %in% pheno[,args$iid_col]) & (kinship_in$ID2 %in% pheno[,args$iid_col])),]
kinship_in <- kinship_in[which((! kinship_in$ID1 %in% id_map$ID[which((id_map$removed_nogeno == 1) | (id_map$removed_sampleqc == 1) | (id_map$removed_postqc_filters == 1) | (! kinship_in$ID2 %in% id_map$ID[which((id_map$removed_nogeno == 1) | (id_map$removed_sampleqc == 1) | (id_map$removed_postqc_filters == 1))])),]
if(nrow(kinship_in) > 0) {
	kinship_in$pair_idx <- row.names(kinship_in)
	kinship_in$rand_choice <- sample.int(2, replace = TRUE, size = nrow(kinship_in))
	sampleqc_in <- read.table(args$sampleqc_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
	sampleqc_in <- sampleqc_in[,c("IID","call_rate")]
	names(sampleqc_in)[1] <- args$iid_col
	sampleqc_in <- merge(sampleqc_in, pheno[,c(args$iid_col, args$pheno_col)], all.x=TRUE)
	sampleqc_in <- sampleqc_in[,c(args$iid_col, "call_rate", args$pheno_col)]
	names(sampleqc_in)[1] <- "ID1"
	names(sampleqc_in)[2] <- "ID1_call_rate"
	names(sampleqc_in)[3] <- "ID1_pheno"
	kinship_in <- merge(kinship_in, sampleqc_in, all.x=T)
	names(sampleqc_in)[1] <- "ID2"
	names(sampleqc_in)[2] <- "ID2_call_rate"
	names(sampleqc_in)[3] <- "ID2_pheno"
	kinship_in <- merge(kinship_in, sampleqc_in, all.x=T)
	kinship_in <- kinship_in[order(-kinship_in$Kinship),]
	
	cat("removing lower quality sample for each related pair starting with the highest kinship value pair, until no more pairs remain\n")
	kinship_in$ID1_remove <- 0
	kinship_in$ID2_remove <- 0
	samples_excl <- c()
	if(args$test != "lmm") {
		for(i in 1:nrow(kinship_in)) {
			if(kinship_in$ID1_remove[i] == 0 & kinship_in$ID2_remove[i] == 0) {
				if(length(unique(pheno[,args$pheno_col])) == 2 & kinship_in$ID1_pheno[i] != kinship_in$ID2_pheno[i]) {
					if(kinship_in$ID1_pheno[i] > kinship_in$ID2_pheno[i]) {
						kinship_in$ID1_remove[which(kinship_in$ID1 == kinship_in$ID2[i])] <- 1
						kinship_in$ID2_remove[which(kinship_in$ID2 == kinship_in$ID2[i])] <- 1
					} else {
						kinship_in$ID1_remove[which(kinship_in$ID1 == kinship_in$ID1[i])] <- 1
						kinship_in$ID2_remove[which(kinship_in$ID2 == kinship_in$ID1[i])] <- 1
					}
				} else {
					if(kinship_in$ID1_call_rate[i] == kinship_in$ID2_call_rate[i]) {
						kinship_in$ID1_remove[which(kinship_in$rand_choice == 1)] <- 1
						kinship_in$ID2_remove[which(kinship_in$rand_choice == 2)] <- 1
					} else if(kinship_in$ID1_call_rate[i] > kinship_in$ID2_call_rate[i]) {
						kinship_in$ID1_remove[which(kinship_in$ID1 == kinship_in$ID2[i])] <- 1
						kinship_in$ID2_remove[which(kinship_in$ID2 == kinship_in$ID2[i])] <- 1
					} else {
						kinship_in$ID1_remove[which(kinship_in$ID1 == kinship_in$ID1[i])] <- 1
						kinship_in$ID2_remove[which(kinship_in$ID2 == kinship_in$ID1[i])] <- 1
					}
				}
			}
		}
		samples_excl <- kinship_in$ID1[which(kinship_in$ID1_remove == 1)]
		samples_excl <- c(samples_excl, kinship_in$ID2[which(kinship_in$ID2_remove == 1)])
		pheno <- pheno[which(! pheno[,args$iid_col] %in% samples_excl),]
		id_map$removed_kinship[which((id_map$removed_nogeno == 0) & (id_map$removed_sampleqc == 0) & (id_map$removed_postqc_filters == 0) & (id_map$ID %in% samples_excl))] <- 1
		cat(paste0("removed ",as.character(length(id_map$removed_kinship[which(id_map$removed_kinship == 1)]))," samples due to within-array kinship"),"\n")
	}
} else {
	cat(paste0("removed 0 samples due to kinship"),"\n")
}

cat("extracting only complete observations\n")
pheno <- pheno[complete.cases(pheno),]
id_map$removed_incomplete_obs[which((id_map$removed_nogeno == 0) & (id_map$removed_sampleqc == 0) & (id_map$removed_postqc_filters == 0) & (id_map$removed_kinship == 0) & (! id_map$ID %in% pheno[,args$iid_col]))] <- 1
cat(paste0("removed ",as.character(length(id_map$removed_incomplete_obs[which(id_map$removed_incomplete_obs == 1)]))," samples with incomplete observations"),"\n")

cat("read post-qc variant exclusion list\n")
variants_excl <- c()
variants_excl_postqc <- try(read.table(args$variants_exclude_postqc,header=FALSE, as.is=TRUE, stringsAsFactors=FALSE, sep="\t"), silent=TRUE)
if(! inherits(variants_excl_postqc, "try-error")) {
	variants_excl <- c(variants_excl,variants_excl_postqc$V1)
}
variants_excl<-unique(variants_excl)

variants_incl<-geno_snps$idx[! as.character(geno_snps$name) %in% variants_excl]
samples_incl<-pheno[,args$iid_col]

failed <- FALSE
if(length(unique(pheno[,args$pheno_col])) == 1) {
	cat(paste0("phenotype ",args$pheno_col," has zero variance\n"))
	failed <- TRUE
}
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
				pheno[,paste0(cvb,val)] <- 0
				pheno[,paste0(cvb,val)][which(pheno[,cvb] == val)] <- 1
				covars_factors <- c(covars_factors,paste0(cvb,val))
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
if(failed) {
	cat("exiting due to invalid model\n")
	quit(status=1)
}
covars_analysis<-paste(c(covars_factors,"1"),collapse="+")
out_cols<-c(out_cols,covars_factors[! covars_factors %in% out_cols])

write.table(id_map, args$out_id_map, row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE, sep="\t")

cat("writing phenotype file","\n")
write.table(out[,out_cols],args$out_pheno,row.names=F,col.names=T,quote=F,sep="\t",append=F, na="NA")
