library(argparse)

parser <- ArgumentParser()
parser$add_argument("--sampleqc", dest="sampleqc", type="character", help="a sampleqc stats file")
parser$add_argument("--metric", dest="metric", type="character", help="a metric (column in stats file)")
parser$add_argument("--out", dest="out", type="character", help="an output filename for plot")
args<-parser$parse_args()

print(args)

library(ggplot2)

d<-read.table(args$sampleqc,header=T,as.is=T,stringsAsFactors=F,colClasses=c("IID"="character"))

pdf(args$out, width=8,height=1,onefile=FALSE)
p<-ggplot(d,aes_string(x=args$metric)) +
	geom_density() +
	theme_bw() +
	theme(legend.position="none",
	axis.line = element_line(colour = "black"), 
	plot.title = element_blank(),
	axis.title.x = element_blank(),
	axis.title.y = element_blank(),
	axis.text.x = element_text(),
	axis.text.y = element_blank(),
	axis.ticks.y = element_blank(),
	panel.grid.major = element_blank(),
	panel.grid.minor = element_blank(),
	panel.border = element_blank(),
	panel.background = element_blank(),
	legend.key = element_blank())
plot(p)
dev.off()
