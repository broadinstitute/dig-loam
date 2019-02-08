library(reshape2)
library(ggplot2)
library(gridExtra)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--ind-clu-files", dest="ind_clu_files", type="character", help="a comma separated list of metrics and cluster files, each separated by 3 underscores")
parser$add_argument("--stats", dest="stats", type="character", help="A sample qc stats file")
parser$add_argument("--stats-adj", dest="stats_adj", type="character", help="An adjusted sample qc stats file")
parser$add_argument("--outliers", dest="outliers", type="character", help="A sample qc outliers file")
parser$add_argument("--boxplots", dest="boxplots", type="character", help="An output filename for boxplots")
parser$add_argument("--discreteness", dest="discreteness", type="character", help="An output filename for discreteness")
parser$add_argument("--outliers-table", dest="outliers_table", type="character", help="An output filename for an outliers table")
args<-parser$parse_args()

print(args)

gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}

clust_files_list<-list()
for(f in unlist(strsplit(args$ind_clu_files,","))) {
	metric<-unlist(strsplit(f,"___"))[1]
	metric_file<-unlist(strsplit(f,"___"))[2]
	clust_files_list[metric]<-metric_file
}

metrics<-names(clust_files_list)
np<-0
np<-np+1
data_orig<-read.table(args$stats,header=T,as.is=T,stringsAsFactors=F)
data<-read.table(args$stats_adj,header=T,as.is=T,stringsAsFactors=F)
data_names<-names(data)[names(data) %in% metrics]
oliers<-readLines(args$outliers)
data$OUTLIER_PCA<-0
if( length(oliers) > 0) {
	data$OUTLIER_PCA[data$IID %in% oliers]<-1
}
pdf(args$boxplots,width=ceiling(length(data_names)/10)*7, height=7)
for(m in metrics) {
    print(paste(m,clust_files_list[[m]],sep=" "))
	cl<-read.table(clust_files_list[[m]], as.is=T, skip=1)
	cl_levels<-c()
	cl_names<-c()
	if(1 %in% cl$V1) {
		cl_names<-c(cl_names,"X")
		cl_levels<-c(cl_levels,1)
	}
	for(c in sort(unique(cl$V1[cl$V1 != 1]))) {
		cl_names<-c(cl_names,c-1)
		cl_levels<-c(cl_levels,c)
	}
	cl$V1<-factor(cl$V1, levels=cl_levels, labels=cl_names, ordered = TRUE)
	names(cl)[1]<-paste(m,"_CLUSTER",sep="")
	data<-cbind(data,cl)
	color<-gg_color_hue(max(as.numeric(data[,c(paste(m,"_CLUSTER",sep=""))])))
	if("X" %in% cl_names) {
		color[1]<-"#808080"
	}
	pl<-ggplot(data,aes_string(paste(m,"_CLUSTER",sep=""), y=m)) +
		geom_boxplot(data=data[data[,c(paste(m,"_CLUSTER",sep=""))] != "X",],aes_string(colour=paste(m,"_CLUSTER",sep=""))) +
		geom_point(aes_string(colour=paste(m,"_CLUSTER",sep=""))) +
		geom_rug(sides="l") +
		scale_x_discrete(limits=cl_names) +
		scale_colour_manual(breaks=cl_names,limits=cl_names,values=color) +
		theme_bw() +
		guides(color=guide_legend(override.aes = list(shape = 15))) +
		theme(axis.line = element_line(colour = "black"), 
		plot.title = element_text(size = 16,face="bold"),
		panel.grid.major = element_blank(),
		panel.grid.minor = element_blank(),
		panel.border = element_blank(),
		panel.background = element_blank(),
		legend.key = element_blank())
	plot(pl)
	cat(file=args$discreteness,paste("   ",m,": ",nrow(data)," total, ",length(unique(data[,c(m)]))," unique, ",(length(unique(data[,c(m)])) / nrow(data))*100,"%\n",sep=""),append=T)
}
dev.off()
data<-merge(data,data_orig,all=T)
i<-0
for(cl in names(data)[grep("_CLUSTER",names(data))]) {
	print(cl)
	i<-i+1
	f<-gsub("_CLUSTER","",cl)
	f_orig<-gsub("_res","",f)
	temp<-data[,c(f,cl,f_orig,"OUTLIER_PCA","IID")]
	names(temp)[1]<-"VALUE"
	names(temp)[2]<-"CLUSTER"
	names(temp)[3]<-"VALUE_ORIG"
	temp$METRIC<-f
	if(i == 1) {
		sdata<-temp
	} else {
		sdata<-rbind(sdata,temp)
	}
}
sdata$DECISION<-"KEEP"
sdata$DECISION[sdata$CLUSTER == "X" & sdata$OUTLIER_PCA == 0]<-"OUTLIER_IND"
sdata$DECISION[sdata$CLUSTER == "X" & sdata$OUTLIER_PCA == 1]<-"OUTLIER_IND_PCA"
sdata$DECISION[sdata$CLUSTER != "X" & sdata$OUTLIER_PCA == 1]<-"OUTLIER_PCA"
sdata$DECISION<-factor(sdata$DECISION)
write.table(sdata[sdata$DECISION != "KEEP",],args$outliers_table,row.names=F,col.names=T,quote=F,sep="\t",append=F)
print(warnings())
