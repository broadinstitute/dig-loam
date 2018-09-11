library(argparse)

parser <- ArgumentParser()
parser$add_argument("--ind-clu-files", dest="ind_clu_files", type="character", help="a comma separated list of metrics and cluster files, each separated by 3 underscores")
parser$add_argument("--stats-unadj", dest="stats_unadj", type="character", help="a stats file")
parser$add_argument("--stats-adj", dest="stats_adj", type="character", help="a stats adj file")
parser$add_argument("--metric-pca-outliers", dest="metric_pca_outliers", type="character", help="a metric pca outliers file")
parser$add_argument("--out", dest="out", type="character", help="an output filename for plot")
args<-parser$parse_args()

print(args)

library(reshape2)
library(ggplot2)
library(gridExtra)
library(gtable)

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

stats_unadj<-read.table(args$stats_unadj,header=T,as.is=T,stringsAsFactors=F)
stats_adj<-read.table(args$stats_adj,header=T,as.is=T,stringsAsFactors=F)
stats_unadj<-stats_unadj[stats_unadj$IID %in% stats_adj$IID,]
oliers<-readLines(args$metric_pca_outliers)
stats_adj$OUTLIER_PCA<-0
if( length(oliers) > 0) {
	stats_adj$OUTLIER_PCA[stats_adj$IID %in% oliers]<-1
}
for(f in ls(clust_files_list)) {
	cl<-read.table(clust_files_list[[f]], as.is=T, skip=1)
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
	names(cl)[1]<-paste(f,"_CLUSTER",sep="")
	stats_adj<-cbind(stats_adj,cl)
	color<-gg_color_hue(max(as.numeric(stats_adj[,c(paste(f,"_CLUSTER",sep=""))])))
	if("X" %in% cl_names) {
		color[1]<-"#808080"
	}
}
stats_adj<-merge(stats_adj,stats_unadj,all=T)
i<-0
for(cl in names(stats_adj)[grep("_CLUSTER",names(stats_adj))]) {
	i<-i+1
	f<-gsub("_CLUSTER","",cl)
	f_orig<-gsub("_res","",f)
	temp<-stats_adj[,c(f,cl,f_orig,"OUTLIER_PCA","IID")]
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
sdata$METRIC_ORIG<-gsub("_res","",sdata$METRIC)
sdata$DECISION<-"KEEP"
sdata$DECISION[sdata$CLUSTER == "X" & sdata$OUTLIER_PCA == 0]<-"OUTLIER_IND"
sdata$DECISION[sdata$CLUSTER == "X" & sdata$OUTLIER_PCA == 1]<-"OUTLIER_IND_PCA"
sdata$DECISION[sdata$CLUSTER != "X" & sdata$OUTLIER_PCA == 1]<-"OUTLIER_PCA"
sdata$DECISION<-factor(sdata$DECISION)
id_list<-list(all=unique(sdata$IID))
oliers_plot<-unique(sdata$IID[sdata$DECISION %in% c("OUTLIER_PCA","OUTLIER_IND","OUTLIER_IND_PCA")])

for(p in c("all")) {
	pdf(args$out, width=0, height=0, paper="a4", onefile=FALSE)
	i<-0
	sdata_temp<-sdata[sdata$IID %in% id_list[[p]],]
	if(nrow(sdata_temp) > 0) {
		grobs<-list()
		for(f in unique(sdata_temp$METRIC)) {
			i<-i+1
			p1<-ggplot(sdata_temp[sdata_temp$METRIC == f,],aes_string(x="METRIC",y="VALUE")) +
				geom_point(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="KEEP",],color="lightgray") +
				geom_boxplot(outlier.shape=NA, outlier.size=NA,fill=NA) +
				geom_point(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="OUTLIER_PCA",],color="blue") +
				geom_point(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="OUTLIER_IND_PCA",],color="green") +
				geom_point(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="OUTLIER_IND",],color="orange") +
				coord_flip() +
				theme_bw() +
				theme(legend.position="none",
				axis.line = element_line(colour = "black"), 
				plot.title = element_blank(),
				axis.title.x = element_blank(),
				axis.title.y = element_blank(),
				axis.text.x = element_text(),
				panel.grid.major = element_blank(),
				panel.grid.minor = element_blank(),
				panel.border = element_blank(),
				panel.background = element_blank(),
				legend.key = element_blank())
			p2<-ggplot(sdata_temp[sdata_temp$METRIC == f,],aes_string(x="VALUE")) +
				geom_density() +
				theme_bw() +
				theme(legend.position="none",
				axis.line = element_line(colour = "black"), 
				plot.title = element_blank(),
				axis.title.x = element_blank(),
				axis.title.y = element_blank(),
				axis.text.x = element_text(),
				panel.grid.major = element_blank(),
				panel.grid.minor = element_blank(),
				panel.border = element_blank(),
				panel.background = element_blank(),
				legend.key = element_blank())
			g1 <- ggplot_gtable(ggplot_build(p1))
			g2 <- ggplot_gtable(ggplot_build(p2))
			pp <- c(subset(g1$layout, name == "panel", se = t:r))
			g <- gtable_add_grob(g1, g2$grobs[[which(g2$layout$name == "panel")]], pp$t, pp$l, pp$b, pp$l)
			grobs[[i]]<-g
		}
		widths<-list()
		for(j in 1:length(grobs)){
			widths[[j]] <- grobs[[j]]$widths[2:5]
		}
		maxwidth<-do.call(grid::unit.pmax, widths)
		for(j in 1:length(grobs)){
			grobs[[j]]$widths[2:5] <- as.list(maxwidth)
		}
		do.call("grid.arrange", c(grobs, ncol = 1))
		dev.off()
	}
}
