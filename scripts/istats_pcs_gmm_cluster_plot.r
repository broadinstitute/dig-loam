#library(scatterplot3d)
#library(Hmisc)
library(reshape2)
library(ggplot2)
args<-commandArgs(trailingOnly=T)
data<-read.table(args[1], header=T,as.is=T,stringsAsFactors=F)
cl<-read.table(args[2], as.is=T, skip=1)
names(cl)[1]<-"CLUSTER"
data<-cbind(data,cl)
if(length(unique(data$CLUSTER)) > 1) {
	write.table(data$IID[data$CLUSTER == 1],args[3],row.names=F,col.names=F,sep="\t",append=F,quote=F)
} else {
	sink(file=args[3])
	sink()
}

gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}
color<-gg_color_hue(max(data$CLUSTER))

pdf(args[4],width=7, height=7)
for(i in seq(2,ncol(data)-2)) {
	p<-ggplot(data, aes(data[,i],data[,i+1])) +
		geom_point(aes(color=factor(CLUSTER))) +
		labs(x=paste("PC",i-1,sep=""),y=paste("PC",i,sep=""),colour="Cluster") +
		theme_bw() +
		ggtitle(paste(args[6]," istats clusters",sep="")) +
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
sink(file=args[5])
table(data$CLUSTER)
sink()
