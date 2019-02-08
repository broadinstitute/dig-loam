library(caret)
library("corrplot")
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--sampleqc-stats-adj", dest="sampleqc_stats_adj", type="character", help="A file containing adjusted sample qc stats")
parser$add_argument("--corr-plots", dest="corr_plots", type="character", help="An output correlation plots file")
parser$add_argument("--pca-loadings", dest="pca_loadings", type="character", help="An output PCA loadings file")
parser$add_argument("--pca-scores-plots", dest="pca_scores_plots", type="character", help="An output PCA plot file")
parser$add_argument("--pca-scores", dest="pca_scores", type="character", help="An output PCA scores file")
args<-parser$parse_args()

print(args)

x<-read.table(args$sampleqc_stats_adj,header=T,as.is=T,stringsAsFactors=F)

# set samples whose values are high on unidirectional metrics to the mean of the metric across all samples
for(m in c("call_rate_res","n_called_res")) {
	x[,m][x[,m] > mean(x[,m])] <- mean(x[,m][x[,m] > mean(x[,m])])
}

row.names(x)<-x$IID
trans = preProcess(x[,2:ncol(x)], method=c("BoxCox", "medianImpute", "center", "scale"),thresh=1.0)
trans.data = predict(trans, x[,2:ncol(x)])
correlations<-cor(trans.data)
pdf(args$corr_plots,width=7, height=7)
corrplot(correlations,method="color", order="hclust")
dev.off()
trans = preProcess(x[,2:ncol(x)], method=c("BoxCox", "medianImpute", "center", "scale", "pca"),thresh=1.0)
PC = predict(trans, x[,2:ncol(x)])
sink(file=args$pca_loadings)
head(PC)
trans$rotation
sink()
pdf(args$pca_scores_plots,width=7, height=7)
for(i in seq(1,ncol(PC)-1)) {
	p<-ggplot(PC, aes(PC[,i],PC[,i+1])) +
		geom_point() +
		labs(x=paste("PC",i,sep=""),y=paste("PC",i+1,sep="")) +
		theme_bw() +
		theme(axis.line = element_line(colour = "black"), 
		panel.grid.major = element_blank(),
		panel.grid.minor = element_blank(),
		panel.border = element_blank(),
		panel.background = element_blank(),
		legend.key = element_blank())
	plot(p)
}
dev.off()
PC$IID<-row.names(PC)

write.table(PC[,c(ncol(PC),1:(ncol(PC)-1))],args$pca_scores,row.names=F,col.names=T,sep="\t",quote=F,append=F)
