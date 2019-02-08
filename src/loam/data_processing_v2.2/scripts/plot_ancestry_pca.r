library(reshape2)
library(ggplot2)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--id", dest="id", type="character", help="A project id")
parser$add_argument("--pca-scores", dest="pca_scores", type="character", help="A file containing PCA scores")
parser$add_argument("--out", dest="out", type="character", help="an output file name")
args<-parser$parse_args()

print(args)

data<-read.table(args$pca_scores, header=T, as.is=T)
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
	guides(col = guide_legend(override.aes=list(shape = as.integer(levels(data$GROUP) != "BioMe")*15 + 1, size=rep(4, length(levels(data$GROUP)))))) +
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
