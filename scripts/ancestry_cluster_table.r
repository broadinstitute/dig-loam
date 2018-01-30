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
i <- 0
for(f in unlist(strsplit(args$cluster_in,','))) {
	i <- i + 1
	l<-unlist(strsplit(f,"___"))[1]
	cat(paste(gsub("_","\\\\_",l),"\t{}\t{}\t{}\n",sep=""),file=args$cluster_out,append=T)
	for(x in c("AFR","AMR","EAS","EUR","SAS")) {
		if(is.na(clusters[[l]]$V2[clusters[[l]]$V1 == x])) {
			cat("{}\t",x,"\tNA\t0\n",file=args$cluster_out,append=T)
		} else {
			cat("{}\t",x,"\t",clusters[[l]]$V2[clusters[[l]]$V1 == x],"\t",nrow(ancestry[[l]][ancestry[[l]]$V2 == x,]),"\n",file=args$cluster_out,append=T)
		}
	}
	cat("{}\tOutliers\t1\t",nrow(ancestry[[l]][ancestry[[l]]$V2 == "OUTLIERS",]),"\n",file=args$cluster_out,append=T)
	if(i < length(unlist(strsplit(args$cluster_in,',')))) {
		cat("{}\t{}\t{}\t{}\n",file=args$cluster_out,append=T)
	}
}

final<-read.table(args$final_in,header=T,as.is=T,stringsAsFactors=F)
cat("Population\tSamples\n",file=args$final_out)
for(x in c("AFR","AMR","EAS","EUR","SAS")) {
	cat(x,"\t",nrow(final[final$FINAL == x,]),"\n",file=args$final_out,append=T)
}
cat("Outliers\t",nrow(final[final$FINAL == "OUTLIERS",]),"\n",file=args$final_out,append=T)
