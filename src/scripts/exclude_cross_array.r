library(argparse)
library(reshape2)

parser <- ArgumentParser()
parser$add_argument("--kinship", dest="kinship", type="character", help="An output file from King")
parser$add_argument("--samples", dest="samples", type="character", help="A samples file (FID IID)")
parser$add_argument("--meta-cohorts", dest="meta_cohorts", type="character", help="A comma separated list of meta cohorts")
parser$add_argument("--cohort", dest="cohort", type="character", help="A cohort")
parser$add_argument("--out", dest="out", type="character", help="An output sample exclude list")
args<-parser$parse_args()

print(args)

meta<-unlist(strsplit(args$meta_cohorts,","))

if(args$cohort == meta[1]) {
	write.table(data.frame(),args$out,col.names=F,row.names=F,quote=F,append=F)
} else {

	k <- read.table(args$kinship, header=T, as.is=T, stringsAsFactors=F, colClasses=c("FID1"="character","ID1"="character","FID2"="character","ID2"="character"))
	k$id1<-colsplit(k$ID1,"_",names=c("id1","X"))$id1
	k$id2<-colsplit(k$ID2,"_",names=c("id2","X"))$id2
	k$c1<-colsplit(k$ID1,"_",names=c("X","c1"))$c1
	k$c2<-colsplit(k$ID2,"_",names=c("X","c2"))$c2
	
	k <- k[which(k$c1 != k$c2),]
	k <- k[which(k$c1 == args$cohort | k$c2 == args$cohort),]
	
	s <- read.table(args$samples, header=F, as.is=T, stringsAsFactors=F, colClasses=c("V1"="character","V2"="character"))
	s$V2 <- paste(s$V2,args$cohort,sep="_")

	k <- k[which(k$c1 %in% meta[1:(grep(args$cohort,meta)-1)] | k$c2 %in% meta[1:(grep(args$cohort,meta)-1)]),]
	samples_exclude <- unique(c(k$id1[k$c1 == args$cohort], k$id2[k$c2 == args$cohort]))
	write.table(samples_exclude,args$out,col.names=F,row.names=F,quote=F,append=F)

}
