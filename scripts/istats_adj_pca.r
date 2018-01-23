args<-commandArgs(trailingOnly=T)
library(caret)
library("corrplot")
x<-read.table(args[1],header=T,as.is=T,stringsAsFactors=F)
row.names(x)<-x$IID
trans = preProcess(x[,2:ncol(x)], method=c("BoxCox", "medianImpute", "center", "scale"),thresh=1.0)
trans.data = predict(trans, x[,2:ncol(x)])
correlations<-cor(trans.data)
pdf(args[2],width=7, height=7)
corrplot(correlations,method="color", order="hclust")
dev.off()
trans = preProcess(x[,2:ncol(x)], method=c("BoxCox", "medianImpute", "center", "scale", "pca"),thresh=1.0)
PC = predict(trans, x[,2:ncol(x)])
sink(file=args[3])
head(PC)
trans$rotation
sink()
pdf(args[4],width=7, height=7)
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

write.table(PC[,c(ncol(PC),1:(ncol(PC)-1))],args[5],row.names=F,col.names=T,sep="\t",quote=F,append=F)
