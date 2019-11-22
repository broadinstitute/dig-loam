library(argparse)
library(reshape2)

parser <- ArgumentParser()
parser$add_argument("--pheno-in", dest="pheno_in", type="character", help="a phenotype file")
parser$add_argument("--fam-in", dest="fam_in", type="character", help="a fam file")
parser$add_argument("--ancestry-in", dest="ancestry_in", type="character", help="an ancestry file")
parser$add_argument("--strat", nargs=4, action = 'append', dest="strat", type="character", help="ancestry, strat column, and strat codes")
parser$add_argument("--pheno-col", dest="pheno_col", type="character", help="a column name for phenotype")
parser$add_argument("--iid-col", dest="iid_col", help='a column name for sample ID in phenotype file')
parser$add_argument("--sampleqc-in", dest="sampleqc_in", type="character", help="a sampleqc file")
parser$add_argument("--kinship-in", dest="kinship_in", type="character", help="a kinship file containing related pairs")
parser$add_argument("--samples-exclude-qc", dest="samples_exclude_qc", type="character", help="a list of sample IDs to exclude based on sample qc")
parser$add_argument("--samples-exclude-postqc", dest="samples_exclude_postqc", type="character", help="a list of sample IDs to exclude based on postqc filters")
parser$add_argument("--cohorts", dest="cohorts", type="character", help="A comma separated list of cohorts")
parser$add_argument("--cckinship", dest="cckinship", type="character", help="a cross cohort kinship file")
parser$add_argument("--meta-prior-samples", dest="meta_prior_samples", type="character", help="a comma separated list of samples available to previous meta cohorts")
parser$add_argument("--meta-cohorts", dest="meta_cohorts", type="character", help="A comma separated list of meta cohorts")
parser$add_argument("--test", dest="test", type="character", help="a test code")
parser$add_argument("--covars", dest="covars", type="character", help="a '+' separated list of covariates")
parser$add_argument("--out-id-map", dest="out_id_map", type="character", help="an output filename for the id removal map")
parser$add_argument("--out-cohorts-map", dest="out_cohorts_map", type="character", help="an output filename for the cohort sample map")
parser$add_argument("--out-pheno-prelim", dest="out_pheno_prelim", type="character", help="a preliminary pheno file output name")
parser$add_argument("--out", dest="out", type="character", help="a sample list filename")
args<-parser$parse_args()

print(args)

cat("removing factor indicators from covariates\n")
covars <- gsub("\\]","",gsub("\\[","",unlist(strsplit(args$covars,split="\\+"))))

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
cat(paste0("extracted ",as.character(nrow(pheno))," samples for this model"),"\n")

cat(paste0("extracting model specific columns from pheno file: ", paste(c(args$iid_col, args$pheno_col, covars), collapse=",")),"\n")
pheno<-pheno[,c(args$iid_col, args$pheno_col, covars)]
out_cols<-colnames(pheno)

cat("reading in genotyped samples IDs from pre-qc plink files\n")
fam<-read.table(args$fam_in,header=F,as.is=T,stringsAsFactors=F,sep="\t")
iids<-fam$V2

id_map <- data.frame(ID = pheno[,args$iid_col])
id_map$removed_nogeno <- 0
id_map$removed_sampleqc <- 0
id_map$removed_postqc_filters <- 0
id_map$removed_incomplete_obs <- 0
id_map$removed_kinship <- 0
id_map$removed_cckinship <- 0
id_map$cohort <- NA
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
kinship_in <- kinship_in[which((! kinship_in$ID1 %in% id_map$ID[which((id_map$removed_nogeno == 1) | (id_map$removed_sampleqc == 1) | (id_map$removed_postqc_filters == 1))]) & (! kinship_in$ID2 %in% id_map$ID[which((id_map$removed_nogeno == 1) | (id_map$removed_sampleqc == 1) | (id_map$removed_postqc_filters == 1))])),]

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
	k$id1<-colsplit(k$ID1,"_",names=c("id1","X"))$id1
	k$id2<-colsplit(k$ID2,"_",names=c("id2","X"))$id2
	k$c1<-colsplit(k$ID1,"_",names=c("X","c1"))$c1
	k$c2<-colsplit(k$ID2,"_",names=c("X","c2"))$c2
	i <- 0
	if(! is.null(args$meta_prior_samples)) {
		samples_excl <- c()
		for(mps in unlist(strsplit(args$meta_prior_samples,","))) {
			i <- i + 1
			prior_samples <- scan(file=mps,what="character")
			k <- k[which(k$c1 %in% cohorts | k$c2 %in% cohorts),]
			k <- k[which(((k$c1 == meta[i]) && (k$id1 %in% prior_samples)) | ((k$c2 == meta[i]) && (k$id2 %in% prior_samples))),]
			samples_excl <- c(samples_excl, unique(c(k$id1[k$c1 %in% cohorts], k$id2[k$c2 %in% cohorts])))
		}
	} else {
		cat("exiting because --cckinship and --meta-cohorts were provided, but --meta-prior-samples was not provided\n")
		quit(status=1)
	}
	id_map$removed_cckinship[which((id_map$removed_nogeno == 0) & (id_map$removed_sampleqc == 0) & (id_map$removed_postqc_filters == 0) & (id_map$removed_kinship == 0) & (id_map$ID %in% samples_excl))] <- 1
	cat(paste0("removed ",as.character(length(id_map$removed_cckinship[which(id_map$removed_cckinship == 1)]))," samples due to cross cohort kinship"),"\n")
}

cat("extracting only complete observations\n")
pheno <- pheno[complete.cases(pheno),]
id_map$removed_incomplete_obs[which((id_map$removed_nogeno == 0) & (id_map$removed_sampleqc == 0) & (id_map$removed_postqc_filters == 0) & (id_map$removed_kinship == 0) & (! id_map$ID %in% pheno[,args$iid_col]))] <- 1
cat(paste0("removed ",as.character(length(id_map$removed_incomplete_obs[which(id_map$removed_incomplete_obs == 1)]))," samples with incomplete observations"),"\n")

cohorts_map <- cohorts_map[cohorts_map[,args$iid_col] %in% pheno[,args$iid_col],]

write.table(pheno, args$out_pheno_prelim, row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE, sep="\t")

write.table(cohorts_map, args$out_cohorts_map, row.names=FALSE, col.names=FALSE, quote=FALSE, append=FALSE, sep="\t")

write.table(id_map, args$out_id_map, row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE, sep="\t")

write.table(pheno[,args$iid_col],args$out, row.names=FALSE, col.names=FALSE, quote=FALSE, append=FALSE, sep="\t")
