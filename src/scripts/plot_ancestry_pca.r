library(reshape2)
library(ggplot2)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--id", dest="id", type="character", help="A project id")
parser$add_argument("--pca-scores", dest="pca_scores", type="character", help="A file containing PCA scores")
parser$add_argument("--update-pop", nargs=3, dest="update_pop", default=NULL, type="character", help="A column name for sample ID, a column name for POP, and the filename. This argument updates the POP field for all overlapping sample IDs in the file")
parser$add_argument("--update-group", nargs=3, dest="update_group", default=NULL, type="character", help="A column name for sample ID, a column name for GROUP, and the filename. This argument updates the GROUP field for all overlapping sample IDs in the file")
parser$add_argument("--out", dest="out", type="character", help="an output file name")
args<-parser$parse_args()

print(args)

data<-read.table(args$pca_scores, header=T, as.is=T, colClasses=c("IID"="character"))
data$POP<-args$id
data$GROUP<-args$id

if(! is.null(args$update_pop)) {
	print("updating population information from file")
	pop_df<-read.table(file=args$update_pop[3],header=TRUE,as.is=T,stringsAsFactors=FALSE,colClasses=c(eval(parse(text=paste0(args$update_pop[1],"=\"character\"")))))
	pop_df<-pop_df[,c(args$update_pop[1],args$update_pop[2])]
	names(pop_df)[1]<-"IID"
	names(pop_df)[2]<-"POP_NEW"
	data<-merge(data,pop_df,all.x=TRUE)
	data$POP[! is.na(data$POP_NEW)]<-data$POP_NEW[! is.na(data$POP_NEW)]
	data$POP_NEW<-NULL
}

if(! is.null(args$update_group)) {
	print("updating group information from file")
	group_df<-read.table(file=args$update_group[3],header=TRUE,as.is=T,stringsAsFactors=FALSE,colClasses=c(eval(parse(text=paste0(args$update_pop[1],"=\"character\"")))))
	group_df<-group_df[,c(args$update_group[1],args$update_group[2])]
	names(group_df)[1]<-"IID"
	names(group_df)[2]<-"GROUP_NEW"
	data<-merge(data,group_df,all.x=TRUE)
	data$GROUP[! is.na(data$GROUP_NEW)]<-data$GROUP_NEW[! is.na(data$GROUP_NEW)]
	data$GROUP_NEW<-NULL
}
data$GROUP<-factor(data$GROUP)

gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}
color<-gg_color_hue(length(unique(data$GROUP)))
color[grep(args$id,levels(data$GROUP))]<-"#191919"

pdf(args$out,width=7,height=7)
p<-ggplot() +
	geom_point(data=data[which(data$GROUP != args$id),],aes(PC1,PC2,color=GROUP), size=2, shape=16) +
	geom_point(data=data[which(data$GROUP == args$id),],aes(PC1,PC2,color=GROUP), size=2, shape=1) +
	scale_colour_manual(name = "GROUP",values = color) +
	theme_bw() +
	guides(col = guide_legend(override.aes=list(shape = as.integer(levels(data$GROUP) != args$id)*15 + 1, size=rep(4, length(levels(data$GROUP)))))) +
	theme(axis.line = element_line(colour = "black"), 
	panel.grid.major = element_blank(),
	panel.grid.minor = element_blank(),
	panel.border = element_blank(),
	panel.background = element_blank(),
	legend.key = element_blank())
plot(p)

p<-ggplot() +
	geom_point(data=data[which(data$GROUP != args$id),],aes(PC2,PC3,color=GROUP), size=2, shape=16) +
	geom_point(data=data[which(data$GROUP == args$id),],aes(PC2,PC3,color=GROUP), size=2, shape=1) +
	scale_colour_manual(name = "GROUP",values = color) +
	theme_bw() +
	guides(col = guide_legend(override.aes=list(shape = as.integer(levels(data$GROUP) != args$id)*15 + 1, size=rep(4, length(levels(data$GROUP)))))) +
	theme(axis.line = element_line(colour = "black"), 
	panel.grid.major = element_blank(),
	panel.grid.minor = element_blank(),
	panel.border = element_blank(),
	panel.background = element_blank(),
	legend.key = element_blank())
plot(p)

dev.off()
