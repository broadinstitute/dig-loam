library(argparse)

parser <- ArgumentParser()
parser$add_argument("--input", nargs='+', dest="input", type="character", help="a list of labels and files, each delimited by comma (eg. ex,file1 omni,file2)")
parser$add_argument("--type", choices=c("bim","fam"), dest="type", type="character", help="a file type")
parser$add_argument("--ancestry", dest="ancestry", type="character", help="an inferred ancestry file")
parser$add_argument("--out", dest="out", type="character", help="an output filename ending in '.png' or '.pdf'")
args<-parser$parse_args()

print(args)

barcolors <- list(AFR="#08306B",AMR="#41AB5D",EAS="#000000",EUR="#F16913",SAS="#3F007D")

ids<-list()
for(inp in args$input) {
	l<-unlist(strsplit(inp,","))[1]
	f<-unlist(strsplit(inp,","))[2]
	tbl<-read.table(f,header=F,as.is=T,stringsAsFactors=F)
	if(args$type == "fam") {
		if(! is.null(args$ancestry)) {
			anc<-read.table(args$ancestry,header=T,as.is=T,stringsAsFactors=F)
			anc <- anc[! anc$FINAL == "OUTLIERS",]
			anc <- anc[anc$IID %in% tbl[,2],]
			for(c in unique(anc$FINAL)) {
				ids[[paste(l," (",c,")",sep="")]]<-tbl[,2][tbl[,2] %in% anc$IID[anc$FINAL == c]]
			}
		} else {
			ids[[l]]<-tbl[,2]
		}
		xLabel = "Samples"
	} else if(args$type == "bim") {
		tbl$id<-paste(tbl$V1,tbl$V4,tbl$V5,tbl$V6,sep=":")
		ids[[l]]<-tbl$id
		xLabel = "Variants"
	} else {
		stop(paste("file type ",args$type," not supported",sep=""))
	}
}

if(! is.null(args$ancestry) && args$type == "fam") {
	ids <- ids[c(grep("AFR",names(ids)), grep("AMR",names(ids)), grep("EAS",names(ids)), grep("EUR",names(ids)), grep("SAS",names(ids)))]
}

if(unlist(strsplit(args$out,"\\."))[length(unlist(strsplit(args$out,"\\.")))] == "pdf") {
	pdf(args$out,width=0,height=0,paper="a4r",onefile=FALSE)
} else {
	stop(paste("output extension ",unlist(strsplit(args$out,"\\."))[length(unlist(strsplit(args$out,"\\.")))]," not supported",sep=""))
}

#library(VennDiagram)
#olap<-calculate.overlap(ids)
#print(names(olap))
#for(idx in names(olap)) {
#	print(length(olap[[idx]]))
#}

library(UpSetR)
# green: #16BE72
# blue: #1F76B4
if(length(ids) <= 4) { 
	text_scale <- c(3, 3, 2, 2, 3, 2)
	point_size <- 8
	line_size <- 3
	mb_ratio = c(0.7, 0.3)
} else {
	text_scale <- c(3, 3, 2, 2, 2, 2)
	point_size <- 6
	line_size <- 2
	mb_ratio = c(0.5, 0.5)
}

upset(fromList(ids), nsets=length(ids), order.by = "freq", sets.bar.color="#1F76B4", line.size = line_size, number.angles = -35, point.size = point_size, empty.intersections = NULL, mainbar.y.label = "Intersection Size", sets.x.label = xLabel, text.scale = text_scale, mb.ratio = mb_ratio)
dev.off()
