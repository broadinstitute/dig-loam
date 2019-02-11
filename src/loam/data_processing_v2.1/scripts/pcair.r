library(argparse)

parser <- ArgumentParser()
parser$add_argument("--cpus", dest="cpus", type="integer", help="number of cpus to use for King")
parser$add_argument("--plink-in", dest="plink_in", type="character", help="Plink fileset name")
parser$add_argument("--gds-out", dest="gds_out", type="character", help="Bioconductor gds file name")
parser$add_argument("--exclude", dest="exclude", default=NULL, type="character", help="A single column file with no header containing sample IDs to exclude")
parser$add_argument("--ancestry", dest="ancestry", default=NULL, type="character", help="An inferred ancestry filename (format: IID ANCESTRY)")
parser$add_argument("--id", dest="id", type="character", help="project ID (ex: CAMP)")
parser$add_argument("--scores", dest="scores", type="character", help="An output filename for PC-AiR scores")
parser$add_argument("--force-unrel", nargs=2, dest="force_unrel", default=NULL, type="character", help="A column name for sample IDs that are to be forced into unrelated subset used to calculate initial PCs followed by the filename")
parser$add_argument("--update-pop", nargs=3, dest="update_pop", default=NULL, type="character", help="A column name for sample ID, a column name for POP, and the filename. This argument updates the POP field for all overlapping sample IDs in the file")
parser$add_argument("--update-group", nargs=3, dest="update_group", default=NULL, type="character", help="A column name for sample ID, a column name for GROUP, and the filename. This argument updates the GROUP field for all overlapping sample IDs in the file")
parser$add_argument("--rdata", dest="rdata", type="character", help="An output filename for PC-AiR Rdata file (to be read in for pcrelate)")
args<-parser$parse_args()

print(args)

library(GENESIS)
library(SNPRelate)
library(GWASTools)
library(gdsfmt)
library(pryr)

print("converting binary Plink data to GDS format")
if(! is.null(args$exclude)) {
	print("excluding samples from GDS file")
	temp_gds<-gsub(unlist(strsplit(args$gds_out,'\\.'))[length(unlist(strsplit(args$gds_out,'\\.')))],paste("temp.",unlist(strsplit(args$gds_out,'\\.'))[length(unlist(strsplit(args$gds_out,'\\.')))],sep=""),args$gds_out)
	snpgdsBED2GDS(bed.fn = paste(args$plink_in,".bed",sep=""), bim.fn = paste(args$plink_in,".bim",sep=""), fam.fn = paste(args$plink_in,".fam",sep=""), out.gdsfn = temp_gds)
	geno <- GdsGenotypeReader(filename = temp_gds)
	iids <- getScanID(GenotypeData(geno))
	close(geno)
	excl<-scan(file=args$exclude,what="character")
	samples_incl<-iids[! iids %in% excl]
	gdsSubset(temp_gds, args$gds_out, sample.include=samples_incl, snp.include=NULL, sub.storage=NULL, compress="LZMA_RA", block.size=5000, verbose=TRUE)
} else {
	snpgdsBED2GDS(bed.fn = paste(args$plink_in,".bed",sep=""), bim.fn = paste(args$plink_in,".bim",sep=""), fam.fn = paste(args$plink_in,".fam",sep=""), out.gdsfn = args$gds_out)
}

print(paste("memory before running king: ",mem_used() / (1024^2),sep=""))

print("running King robust to get kinship matrix")
genofile <- snpgdsOpen(args$gds_out)
king<-snpgdsIBDKING(genofile, sample.id=NULL, snp.id=NULL, autosome.only=TRUE, remove.monosnp=TRUE, maf=NaN, missing.rate=NaN, type="KING-robust", family.id=NULL, num.thread=args$cpus, verbose=TRUE)
kinship <- king$kinship
rownames(kinship)<-king$sample.id
colnames(kinship)<-king$sample.id
snpgdsClose(genofile)

print(paste("memory after running king and before running pcair: ",mem_used() / (1024^2),sep=""))

print("load genotype data from GDS file")
geno <- GdsGenotypeReader(filename = args$gds_out)
genoData <- GenotypeData(geno)
iids <- getScanID(genoData)

unrel_iids<-NULL
if(! is.null(args$force_unrel)) {
	print("reading list of unrelated samples from file")
	unrel_df<-read.table(file=args$force_unrel[2],header=TRUE,as.is=T,stringsAsFactors=FALSE)
	unrel_iids<-unrel_df[,grep(args$force_unrel[1],names(unrel_df))]
}

print("running pcair")
mypcair <- pcair(genoData = genoData, kinMat = kinship, divMat = kinship, unrel.set = unrel_iids, snp.block.size = 10000)

print(paste("memory after running pcair: ",mem_used() / (1024^2),sep=""))

if(! is.null(args$rdata)) {
	print("saving pcair results to rdata file")
	save(mypcair, file=args$rdata)
}

print("converting PCs to output format")
out<-data.frame(mypcair$vectors)
names(out)[1:20]<-paste("PC",seq(1,20),sep="")
out$IID<-row.names(out)
if(! is.null(args$id)) {
	out$POP<-args$id
	out$GROUP<-args$id
} else {
	out$POP<-NA
	out$GROUP<-NA
}

if(! is.null(args$ancestry)) {
	print("adding inferred ancestry to output")
	anc_df<-read.table(file=args$ancestry,header=FALSE,as.is=T,stringsAsFactors=FALSE)
	names(anc_df)[1]<-"IID"
	names(anc_df)[2]<-"GROUP_NEW"
	out<-merge(out,anc_df,all.x=TRUE)
	out$GROUP[! is.na(out$GROUP_NEW)]<-out$GROUP_NEW[! is.na(out$GROUP_NEW)]
	out$GROUP_NEW<-NULL
}

if(! is.null(args$update_pop)) {
	print("updating population information from file")
	pop_df<-read.table(file=args$update_pop[3],header=TRUE,as.is=T,stringsAsFactors=FALSE)
	pop_df<-pop_df[,c(args$update_pop[1],args$update_pop[2])]
	names(pop_df)[1]<-"IID"
	names(pop_df)[2]<-"POP_NEW"
	out<-merge(out,pop_df,all.x=TRUE)
	out$POP[! is.na(out$POP_NEW)]<-out$POP_NEW[! is.na(out$POP_NEW)]
	out$POP_NEW<-NULL
}

if(! is.null(args$update_group)) {
	print("updating group information from file")
	group_df<-read.table(file=args$update_group[3],header=TRUE,as.is=T,stringsAsFactors=FALSE)
	group_df<-group_df[,c(args$update_group[1],args$update_group[2])]
	names(group_df)[1]<-"IID"
	names(group_df)[2]<-"GROUP_NEW"
	out<-merge(out,group_df,all.x=TRUE)
	out$GROUP[! is.na(out$GROUP_NEW)]<-out$GROUP_NEW[! is.na(out$GROUP_NEW)]
	out$GROUP_NEW<-NULL
}

out_cols<-c("IID","POP","GROUP",paste("PC",seq(1,20),sep=""))
if(all(is.na(out$POP))) {
	out_cols<-out_cols[! out_cols == "POP"]
}
if(all(is.na(out$GROUP))) {
	out_cols<-out_cols[! out_cols == "GROUP"]
}

print("writing PCs to file")
write.table(out[,out_cols],args$scores,row.names=F,col.names=T,quote=F,sep="\t",append=F)
