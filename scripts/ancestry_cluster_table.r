library(argparse)

parser <- ArgumentParser()
parser$add_argument("--cluster-in", nargs='+', dest="cluster_in", type="character", help="a list of label and cluster group file sets, each separated by comma")
parser$add_argument("--ancestry-in", nargs='+', dest="ancestry_in", type="character", help="a list of label and ancestry file sets, each separated by comma")
parser$add_argument("--final-in", dest="final_in", type="character", help="a merged ancestry file")
parser$add_argument("--cluster-out", dest="cluster_out", type="character", help="an output filename for cluster table")
parser$add_argument("--final-out", dest="final_out", type="character", help="an output filename for final ancestry assignments")
args<-parser$parse_args()

print(args)

clusters<-list()
for(f in args$cluster_in) {
	x<-unlist(strsplit(f,","))[1]
	y<-unlist(strsplit(f,","))[2]
	clusters[[x]]<-read.table(y,header=F,as.is=T,stringsAsFactors=F)
}

ancestry<-list()
for(f in args$ancestry_in) {
	x<-unlist(strsplit(f,","))[1]
	y<-unlist(strsplit(f,","))[2]
	ancestry[[x]]<-read.table(y,header=F,as.is=T,stringsAsFactors=F)
}

cat("Data\tPopulation\tClusters\tSamples\n",file=args$cluster_out)
i <- 0
for(f in args$cluster_in) {
	i <- i + 1
	l<-unlist(strsplit(f,","))[1]
	j <- 0
	for(x in c("AFR","AMR","EAS","EUR","SAS")) {
		if(! is.na(clusters[[l]]$V2[clusters[[l]]$V1 == x])) {
			j <- j + 1
			if(j == 1) {
				first <- l
			} else {
				first <- "NA"
			}
			cat(gsub("_","\\\\_",paste(paste(c(first,x,clusters[[l]]$V2[clusters[[l]]$V1 == x],nrow(ancestry[[l]][ancestry[[l]]$V2 == x,])),collapse="\t"),"\n",sep="")),file=args$cluster_out,append=T)
		}
	}
	cat(gsub("_","\\\\_",paste(paste(c("NA","Outliers","1",nrow(ancestry[[l]][ancestry[[l]]$V2 == "OUTLIERS",])),collapse="\t"),"\n",sep="")),file=args$cluster_out,append=T)
}

final<-read.table(args$final_in,header=T,as.is=T,stringsAsFactors=F)
cat("Population\tSamples\n",file=args$final_out)
for(x in c("AFR","AMR","EAS","EUR","SAS")) {
	if(nrow(final[final$FINAL == x,]) > 0) {
		cat(gsub("_","\\\\_",paste(paste(x,nrow(final[final$FINAL == x,]),sep="\t"),"\n",sep="")),file=args$final_out,append=T)
	}
}
cat(gsub("_","\\\\_",paste(paste("Outliers",nrow(final[final$FINAL == "OUTLIERS",]),sep="\t"),"\n",sep="")),file=args$final_out,append=T)
