library(reshape2)
library(ggplot2)
library(gridExtra)
args<-commandArgs(trailingOnly=T)
gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}
data<-read.table(args[1],header=T,as.is=T,stringsAsFactors=F)
oliers<-readLines(args[2])
data$OUTLIER_PCA<-0
if( length(oliers) > 0) {
	data$OUTLIER_PCA[data$IID %in% oliers]<-1
}
i<-0
data$DECISION<-"KEEP"
data$DECISION[data$OUTLIER_PCA == 1]<-"OUTLIER"
data$DECISION<-factor(data$DECISION)
oliers_plot<-data$IID[data$DECISION == "OUTLIER"]
data<-melt(data,id.vars=c("IID","DECISION","OUTLIER_PCA"))
pdf(args[3],width=20, height=4)
i<-0
cmd<-"grid.arrange("
for(f in unique(data$variable)) {
	i<-i+1
	pl<-ggplot(data[data$variable == f,],aes_string(x="variable",y="value")) +
		geom_jitter(data=data[data$variable == f & data$DECISION=="KEEP",],color="lightgray",position=position_jitter(height=0)) +
		geom_boxplot(outlier.size=NA,fill=NA) +
		geom_jitter(data=data[data$variable == f & data$DECISION=="OUTLIER",],color="blue",position=position_jitter(height=0)) +
		theme_bw() +
		theme(legend.position="none",
		axis.line = element_line(colour = "black"), 
		plot.title = element_text(size = 16,face="bold"),
		axis.title.x = element_blank(),
		axis.title.y = element_blank(),
		panel.grid.major = element_blank(),
		panel.grid.minor = element_blank(),
		panel.border = element_blank(),
		panel.background = element_blank(),
		legend.key = element_blank())
	assign(paste("plot_",f,sep=""),pl)
	cmd<-paste(cmd,paste("plot_",f,sep=""),",",sep="")
}
cmd<-paste(cmd,"ncol=",i,")",sep="")
print(cmd)
eval(parse(text=cmd))
opn<-0
for(o in oliers_plot) {
	opn<-opn+1
	i<-0
	cmd<-"grid.arrange("
	for(f in unique(data$variable)) {
		i<-i+1
		pl<-ggplot(data[data$variable == f,],aes_string(x="variable",y="value")) +
			geom_jitter(data=data[data$variable == f & data$DECISION=="KEEP",],color="lightgray",position=position_jitter(height=0)) +
			geom_boxplot(outlier.size=NA,fill=NA) +
			geom_jitter(data=data[data$variable == f & data$DECISION=="OUTLIER" & data$IID == o,],color="blue",position=position_jitter(height=0)) +
			theme_bw() +
			theme(legend.position="none",
			axis.line = element_line(colour = "black"), 
			plot.title = element_text(size = 16,face="bold"),
			axis.title.x = element_blank(),
			axis.title.y = element_blank(),
			panel.grid.major = element_blank(),
			panel.grid.minor = element_blank(),
			panel.border = element_blank(),
			panel.background = element_blank(),
			legend.key = element_blank())
		assign(paste("plot_",f,sep=""),pl)
		cmd<-paste(cmd,paste("plot_",f,sep=""),",",sep="")
	}
	cmd<-paste(cmd,"ncol=",i,",main=textGrob(\"",o,"\",just=\"top\"))",sep="")
	print(paste("outlier ",o," (",opn," of ",length(oliers_plot),") ",cmd,sep=""))
	eval(parse(text=cmd))
}
dev.off()
print(warnings())
