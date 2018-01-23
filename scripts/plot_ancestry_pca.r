library(reshape2)
library(ggplot2)
args<-commandArgs(trailingOnly=T)
data<-read.table(args[2], header=T, as.is=T)
data$GROUP<-factor(data$GROUP)
gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}
color<-gg_color_hue(length(unique(data$GROUP)))
color[grep(args[1],levels(data$GROUP))]<-"#000000"
pdf(args[3],width=7,height=7)
p<-ggplot() +
	geom_point(data=data[which(data$GROUP != args[1]),],aes(PC1,PC2,color=GROUP), size=2) +
	geom_point(data=data[which(data$GROUP == args[1]),],aes(PC1,PC2,color=GROUP), size=2) +
	scale_colour_manual(name = "GROUP",values = color) +
	theme_bw() +
	guides(col = guide_legend(override.aes = list(shape = 15, size = 10))) +
	theme(axis.line = element_line(colour = "black"), 
	panel.grid.major = element_blank(),
	panel.grid.minor = element_blank(),
	panel.border = element_blank(),
	panel.background = element_blank(),
	legend.key = element_blank())
plot(p)

p<-ggplot() +
	geom_point(data=data[which(data$GROUP != args[1]),],aes(PC2,PC3,color=GROUP), size=2) +
	geom_point(data=data[which(data$GROUP == args[1]),],aes(PC2,PC3,color=GROUP), size=2) +
	scale_colour_manual(name = "GROUP",values = color) +
	theme_bw() +
	guides(col = guide_legend(override.aes = list(shape = 15, size = 10))) +
	theme(axis.line = element_line(colour = "black"), 
	panel.grid.major = element_blank(),
	panel.grid.minor = element_blank(),
	panel.border = element_blank(),
	panel.background = element_blank(),
	legend.key = element_blank())
plot(p)

dev.off()
