library(argparse)
library(reshape2)

parser <- ArgumentParser()
parser$add_argument("--pheno-in", dest="pheno_in", type="character", help="a phenotype file")
parser$add_argument("--cohorts-map-in", dest="cohorts_map_in", type="character", help="a fam file")
parser$add_argument("--ancestry-in", dest="ancestry_in", type="character", help="an ancestry file")
parser$add_argument("--cohorts", dest="cohorts", type="character", help="A comma separated list of cohorts")
#parser$add_argument("--pheno-cols", dest="pheno_cols", type="character", help="a comma separated list of column names for phenotypes")
parser$add_argument("--pheno-table", dest="pheno_table", type="character", help="a pheno table file")
parser$add_argument("--sex-col", dest="sex_col", type="character", help="a column name for sex")
parser$add_argument("--iid-col", dest="iid_col", help='a column name for sample ID in phenotype file')
parser$add_argument("--sampleqc-in", dest="sampleqc_in", type="character", help="a sampleqc file")
parser$add_argument("--kinship-in", dest="kinship_in", type="character", help="a kinship file containing related pairs")
parser$add_argument("--cckinship", dest="cckinship", type="character", help="a cross cohort kinship file")
parser$add_argument("--meta-prior-samples", dest="meta_prior_samples", type="character", help="a comma separated list of samples available to previous meta cohorts")
parser$add_argument("--meta-cohorts", dest="meta_cohorts", type="character", help="A comma separated list of meta cohorts")
parser$add_argument("--keep-related", action="store_true", dest="keep_related", help="whether or not to keep related samples")
parser$add_argument("--covars", dest="covars", type="character", help="a '+' separated list of covariates")
parser$add_argument("--out-id-map", dest="out_id_map", type="character", help="an output filename for the id removal map")
parser$add_argument("--out-cohorts-map", dest="out_cohorts_map", type="character", help="an output filename for the cohort sample map")
parser$add_argument("--out-pheno-prelim", dest="out_pheno_prelim", type="character", help="a preliminary pheno file output name")
parser$add_argument("--out", dest="out", type="character", help="a sample list filename")
args<-parser$parse_args()

print(args)

cat("read in pheno file: ")
pheno<-read.table(args$pheno_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
cat(paste0(nrow(pheno)," samples\n"))

cat("reading cohorts from file: ")
cohorts_map<-read.table(args$cohorts_map_in,header=F,as.is=T,stringsAsFactors=F,sep="\t")
cat(paste0(nrow(cohorts_map)," samples\n"))
names(cohorts_map)[1]<-args$iid_col
names(cohorts_map)[2]<-"COHORT"
pheno<-merge(pheno,cohorts_map,all.y=T)

cat("reading inferred ancestry from file: ")
ancestry<-read.table(args$ancestry_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
cat(paste0(nrow(ancestry)," samples\n"))
names(ancestry)[1]<-args$iid_col
names(ancestry)[2]<-"ANCESTRY_INFERRED"
pheno<-merge(pheno,ancestry,all.x=T)

cat("limiting to cohorts in list: ")
pheno<-pheno[pheno$COHORT %in% unlist(strsplit(args$cohorts,",")),]
cat(paste0(nrow(pheno)," samples remaining\n"))

cat("read in pheno table from file: ")
phenoTable<-read.table(args$pheno_table,header=T,as.is=T,stringsAsFactors=F,sep="\t")
cat(paste0(nrow(phenoTable)," phenotypes\n"))
phenoTable[is.na(phenoTable)]<-"NA"
pheno_cols <- phenoTable$id

if(! is.null(args$sex_col)) {
	cat(paste0("adding ",args$sex_col," to output fields list\n"))
	cols_extract <- c(args$iid_col, pheno_cols, args$sex_col)
} else {
	cols_extract <- c(args$iid_col, pheno_cols)
}

if(! is.null(args$covars)) {
	cat("removing factor indicators from covariates\n")
	covars <- gsub("\\]","",gsub("\\[","",unlist(strsplit(args$covars,split="\\+"))))
	cat(paste0("extracting model specific columns from pheno file: ", paste(unique(c(cols_extract, covars)), collapse=",")),"\n")
	print(colnames(pheno))
	pheno<-pheno[,unique(c(cols_extract, covars))]
} else {
	cat(paste0("extracting model specific columns from pheno file: ", paste(cols_extract, collapse=",")),"\n")
	pheno<-pheno[,cols_extract]
}

out_cols<-colnames(pheno)

id_map <- data.frame(ID = pheno[,args$iid_col])
id_map$flagged_kinship <- 0
id_map$removed_kinship <- 0
id_map$flagged_cckinship <- 0
id_map$removed_cckinship <- 0
id_map$flagged_incomplete_obs <- 0
id_map$removed_incomplete_obs <- 0

cat("reading in kinship values for related pairs: ")
kinship_in <- read.table(args$kinship_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
kinship_in <- kinship_in[which((kinship_in$ID1 %in% pheno[,args$iid_col]) & (kinship_in$ID2 %in% pheno[,args$iid_col])),]
cat(paste0(nrow(kinship_in)," pairs found\n"))

if(nrow(kinship_in) > 0) {
	kinship_in$pair_idx <- row.names(kinship_in)
	kinship_in$rand_choice <- sample.int(2, replace = TRUE, size = nrow(kinship_in))
	sampleqc_in <- read.table(args$sampleqc_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
	sampleqc_in <- sampleqc_in[,c("IID","call_rate")]
	names(sampleqc_in)[1] <- args$iid_col
	sampleqc_in <- merge(sampleqc_in, pheno[,c(args$iid_col, pheno_cols)], all.x=TRUE)
	sampleqc_in <- sampleqc_in[,c(args$iid_col, "call_rate", pheno_cols)]
	names(sampleqc_in)[1] <- "ID1"
	names(sampleqc_in)[2] <- "ID1_call_rate"
	for(i in 3:(length(pheno_cols)+2)) {
		names(sampleqc_in)[i] <- paste0("ID1_",pheno_cols[i-2])
	}
	kinship_in <- merge(kinship_in, sampleqc_in, all.x=T)
	names(sampleqc_in)[1] <- "ID2"
	names(sampleqc_in)[2] <- "ID2_call_rate"
	for(i in 3:(length(pheno_cols)+2)) {
		names(sampleqc_in)[i] <- paste0("ID2_",pheno_cols[i-2])
	}
	kinship_in <- merge(kinship_in, sampleqc_in, all.x=T)
	kinship_in <- kinship_in[order(-kinship_in$KINSHIP),]
	
	cat("removing lower quality sample for each related pair starting with the highest kinship value pair, until no more pairs remain\n")
	kinship_in$ID1_remove <- 0
	kinship_in$ID2_remove <- 0
	samples_excl <- c()
	if(! args$keep_related) {
		for(i in 1:nrow(kinship_in)) {
			if(kinship_in$ID1_remove[i] == 0 & kinship_in$ID2_remove[i] == 0) {
				pheno_mismatch<-0
				for(pheno_tmp in pheno_cols) {
					if(kinship_in[,paste0("ID1_",pheno_tmp)][i] != kinship_in[,paste0("ID2_",pheno_tmp)][i]) {
						pheno_mismatch<-1
					}
				}
				if(pheno_mismatch == 1) {
					ID1_votes<-0
					ID2_votes<-0
					for(pheno_tmp in pheno_cols) {
						if(length(unique(kinship_in[,paste0("ID1_",pheno_tmp)])) == 2 & length(unique(kinship_in[,paste0("ID2_",pheno_tmp)])) == 2) {
							if(kinship_in[,paste0("ID1_",pheno_tmp)][i] > kinship_in[,paste0("ID1_",pheno_tmp)][i]) {
								ID1_votes<-ID1_votes+1
							} else {
								ID2_votes<-ID2_votes+1
							}
						}
					}
					if(ID1_votes > ID2_votes) {
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
		id_map$flagged_kinship[which(id_map$ID %in% samples_excl)] <- 1
		id_map$removed_kinship[which(id_map$ID %in% samples_excl)] <- 1
		cat(paste0("removed ",as.character(length(id_map$removed_kinship[which(id_map$removed_kinship == 1)])),"/",as.character(length(id_map$flagged_kinship[which(id_map$flagged_kinship == 1)]))," flagged samples due to within-array kinship: "))
	}
} else {
	cat("removed 0 samples due to kinship: ")
}
cat(paste0(nrow(pheno)," samples remaining\n"))

get_id<-function(s) {
	paste(unlist(strsplit(s,"_"))[1:(length(unlist(strsplit(s,"_")))-1)],collapse="_")
}

get_cohort<-function(s) {
	unlist(strsplit(s,"_"))[length(unlist(strsplit(s,"_")))]
}

if(! is.null(args$cckinship)) {
	cat("reading in cross-cohort kinship samples to remove\n")
	if(! is.null(args$cohorts)) {
		cohorts <- unlist(strsplit(args$cohorts,","))
	} else {
		cat("exiting because --cckinship was provided, but --cohorts was not provided\n")
		quit(status=1)
	}
	if(! is.null(args$meta_cohorts)) {
		meta<-unlist(strsplit(args$meta_cohorts,","))
	} else {
		cat("exiting because --cckinship was provided, but --meta-cohorts was not provided\n")
		quit(status=1)
	}
	k <- read.table(args$cckinship, header=T, as.is=T, stringsAsFactors=F)
	k$id1<-sapply(k$ID1,get_id)
	k$id2<-sapply(k$ID2,get_id)
	k$c1<-sapply(k$ID1,get_cohort)
	k$c2<-sapply(k$ID2,get_cohort)
	i <- 0
	if(! is.null(args$meta_prior_samples)) {
		samples_excl <- c()
		for(mps in unlist(strsplit(args$meta_prior_samples,","))) {
			i <- i + 1
			prior_samples <- scan(file=mps,what="character")
			k <- k[which(k$c1 %in% cohorts | k$c2 %in% cohorts),]
			k <- k[which(((k$c1 == meta[i]) & (k$id1 %in% prior_samples)) | ((k$c2 == meta[i]) & (k$id2 %in% prior_samples))),]
			samples_excl <- c(samples_excl, unique(c(k$id1[k$c1 %in% cohorts], k$id2[k$c2 %in% cohorts])))
		}
	} else {
		cat("exiting because --cckinship and --meta-cohorts were provided, but --meta-prior-samples was not provided\n")
		quit(status=1)
	}
	pheno <- pheno[which(! pheno[,args$iid_col] %in% samples_excl),]
	id_map$flagged_cckinship[which(id_map$ID %in% samples_excl)] <- 1
	id_map$removed_cckinship[which((id_map$removed_kinship == 0) & (id_map$ID %in% samples_excl))] <- 1
	cat(paste0("removed ",as.character(length(id_map$removed_cckinship[which(id_map$removed_cckinship == 1)])),"/",as.character(length(id_map$flagged_cckinship[which(id_map$flagged_cckinship == 1)]))," flagged samples due to cross cohort kinship: "))
}
cat(paste0(nrow(pheno)," samples remaining\n"))

cat("extracting only complete observations\n")
pheno <- subset(pheno, rowSums(! is.na(pheno[pheno_cols])) > 0)
id_map$removed_incomplete_obs[which((id_map$removed_kinship == 0) & (id_map$removed_cckinship == 0) & (! id_map$ID %in% pheno[,args$iid_col]))] <- 1
cat(paste0("removed ",as.character(length(id_map$removed_incomplete_obs[which(id_map$removed_incomplete_obs == 1)])),"/",as.character(length(id_map$flagged_incomplete_obs[which(id_map$flagged_incomplete_obs == 1)]))," flagged samples with incomplete observations: "))
cat(paste0(nrow(pheno)," samples remaining\n"))

cohorts_map <- cohorts_map[cohorts_map[,args$iid_col] %in% pheno[,args$iid_col],]

write.table(cohorts_map, args$out_cohorts_map, row.names=FALSE, col.names=FALSE, quote=FALSE, append=FALSE, sep="\t")

write.table(pheno, args$out_pheno_prelim, row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE, sep="\t")

write.table(id_map, args$out_id_map, row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE, sep="\t")

write.table(pheno[,args$iid_col],args$out, row.names=FALSE, col.names=FALSE, quote=FALSE, append=FALSE, sep="\t")
