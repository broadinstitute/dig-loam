library(reshape2)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--sampleqc-stats", dest="sampleqc_stats", type="character", help="A sample qc stats file")
parser$add_argument("--pca-scores", dest="pca_scores", type="character", help="A file containing PCA scores")
parser$add_argument("--out", dest="out", type="character", help="an output file name")
args<-parser$parse_args()

print(args)

data<-read.table(args$sampleqc_stats,header=T,as.is=T,stringsAsFactors=F)
pcs<-read.table(args$pca_scores,header=T,as.is=T,stringsAsFactors=F)
pcs$POP<-NULL
pcs$GROUP<-NULL
out<-merge(data,pcs,all.y=T)
for(x in names(out)[grep("^PC|IID",names(out),invert=TRUE)]) {
	print(paste("var(",x,") = ",var(out[,x])),sep="")
	if(var(out[,x]) != 0) {
		out$res<-glm(eval(parse(text=paste(x,"~PC1+PC2+PC3+PC4+PC5+PC6+PC7+PC8+PC9+PC10",sep=""))),data=out,family="gaussian")$residuals
		names(out)[names(out) == "res"]<-paste(x,"_res",sep="")
	}
}
write.table(out[,c("IID",names(out)[grep("_res",names(out))])],args$out,row.names=F,col.names=T,sep="\t",quote=F,append=F)
