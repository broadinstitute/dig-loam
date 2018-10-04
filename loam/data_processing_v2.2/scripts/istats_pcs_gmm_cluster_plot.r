library(reshape2)
library(ggplot2)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--pca-scores", dest="pca_scores", type="character", help="A file containing PCA scores")
parser$add_argument("--cluster", dest="cluster", type="character", help="A klustakwik cluster file")
parser$add_argument("--outliers", dest="outliers", type="character", help="An output outlier file")
parser$add_argument("--plots", dest="plots", type="character", help="An output file name for plots")
parser$add_argument("--xtabs", dest="xtabs", type="character", help="An output file name for cross tabs")
parser$add_argument("--id", dest="id", type="character", help="A project id")
args<-parser$parse_args()

print(args)

data<-read.table(args$pca_scores, header=T,as.is=T,stringsAsFactors=F)
cl<-read.table(args$cluster, as.is=T, skip=1)
names(cl)[1]<-"CLUSTER"
data<-cbind(data,cl)
if(length(unique(data$CLUSTER)) > 1) {
	write.table(data$IID[data$CLUSTER == 1],args$outliers,row.names=F,col.names=F,sep="\t",append=F,quote=F)
} else {
	sink(file=args$outliers)
	sink()
}

gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}
color<-gg_color_hue(max(data$CLUSTER))

pdf(args$plots,width=7, height=7)
for(i in seq(2,ncol(data)-2)) {
	p<-ggplot(data, aes(data[,i],data[,i+1])) +
		geom_point(aes(color=factor(CLUSTER))) +
		labs(x=paste("PC",i-1,sep=""),y=paste("PC",i,sep=""),colour="Cluster") +
		theme_bw() +
		ggtitle(paste(args$id," istats clusters",sep="")) +
		guides(col = guide_legend(override.aes = list(shape = 15, size = 10))) +
		theme(axis.line = element_line(colour = "black"), 
		plot.title = element_text(size = 16,face="bold"),
		panel.grid.major = element_blank(),
		panel.grid.minor = element_blank(),
		panel.border = element_blank(),
		panel.background = element_blank(),
		legend.key = element_blank())
	plot(p)
}	
dev.off()
sink(file=args$xtabs)
table(data$CLUSTER)
sink()
