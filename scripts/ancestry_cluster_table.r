library(argparse)

parser <- ArgumentParser()
parser$add_argument("--cluster-in", dest="cluster_in", type="character", help="a comma separated list of label and cluster group file sets, each separated by 3 underscores")
parser$add_argument("--ancestry-in", dest="ancestry_in", type="character", help="a comma separated list of label and ancestry file sets, each separated by 3 underscores")
parser$add_argument("--final-in", dest="final_in", type="character", help="a merged ancestry file")
parser$add_argument("--cluster-out", dest="cluster_out", type="character", help="an output filename for cluster table")
parser$add_argument("--final-out", dest="final_out", type="character", help="an output filename for final ancestry assignments")
args<-parser$parse_args()

print(args)

clusters<-list()
for(f in unlist(strsplit(args$cluster_in,','))) {
	x<-unlist(strsplit(f,"___"))[1]
	y<-unlist(strsplit(f,"___"))[2]
	clusters[[x]]<-read.table(y,header=F,as.is=T,stringsAsFactors=F)
}

ancestry<-list()
for(f in unlist(strsplit(args$ancestry_in,','))) {
	x<-unlist(strsplit(f,"___"))[1]
	y<-unlist(strsplit(f,"___"))[2]
	ancestry[[x]]<-read.table(y,header=F,as.is=T,stringsAsFactors=F)
}

cat("Data\tPopulation\tClusters\tSamples\n",file=args$cluster_out)
cat("{}\t\\textbf{Population}\t\\textbf{Clusters}\t\\textbf{Samples}\n",file=args$cluster_out,append=T)
i <- 0
for(f in unlist(strsplit(args$cluster_in,','))) {
	i <- i + 1
	l<-unlist(strsplit(f,"___"))[1]
	if(i %% 2 == 0) {
		pre<-""
	} else {
		pre<-"\\cellcolor{Gray}"
	}
	cat(paste(pre,"\\textbf{",gsub("_","\\\\_",l),"}\t",paste(rep(paste(pre,"{}",sep=""),3),collapse="\t"),"\n",sep=""),file=args$cluster_out,append=T)
	for(x in c("AFR","AMR","EAS","EUR","SAS")) {
		if(is.na(clusters[[l]]$V2[clusters[[l]]$V1 == x])) {
			cat(paste(paste(pre,c("{}",x,"NA","0\n"),sep=""),collapse="\t"),file=args$cluster_out,append=T)
		} else {
			cat(paste(paste(paste(pre,c("{}",x,clusters[[l]]$V2[clusters[[l]]$V1 == x],nrow(ancestry[[l]][ancestry[[l]]$V2 == x,])),sep=""),collapse="\t"),"\n",sep=""),file=args$cluster_out,append=T)
		}
	}
	cat(paste(paste(paste(pre,c("{}","Outliers","1",nrow(ancestry[[l]][ancestry[[l]]$V2 == "OUTLIERS",])),sep=""),collapse="\t"),"\n",sep=""),file=args$cluster_out,append=T)
	if(i < length(unlist(strsplit(args$cluster_in,',')))) {
		cat(paste(paste(rep(paste(pre,"{}",sep=""),4),collapse="\t"),"\n",sep=""),file=args$cluster_out,append=T)
	}
}

final<-read.table(args$final_in,header=T,as.is=T,stringsAsFactors=F)
cat("Population\tSamples\n",file=args$final_out)
cat("\\textbf{Population}\t\\textbf{Samples}\n",file=args$final_out,append=T)
for(x in c("AFR","AMR","EAS","EUR","SAS")) {
	cat(paste(paste(x,nrow(final[final$FINAL == x,]),sep="\t"),"\n",sep=""),file=args$final_out,append=T)
}
cat(paste(paste("Outliers",nrow(final[final$FINAL == "OUTLIERS",]),sep="\t"),"\n",sep=""),file=args$final_out,append=T)
