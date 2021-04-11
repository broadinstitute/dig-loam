library(argparse)

parser <- ArgumentParser()
parser$add_argument("--input", nargs='+', dest="input", type="character", help="a list of labels and files, each delimited by comma (eg. ex,file1 omni,file2)")
parser$add_argument("--exclusions", nargs='+', dest="exclusions", type="character", help="a list of labels and files, each delimited by comma (eg. ex,file1 omni,file2)")
parser$add_argument("--type", choices=c("bim","fam"), dest="type", type="character", help="a file type")
parser$add_argument("--ancestry", dest="ancestry", type="character", help="an inferred ancestry file")
parser$add_argument("--out", dest="out", type="character", help="an output filename ending in '.png' or '.pdf'")
args<-parser$parse_args()

print(args)

barcolors <- list(AFR="#08306B",AMR="#41AB5D",EAS="#000000",EUR="#F16913",SAS="#3F007D")

if(! is.null(args$ancestry)) {
	anc<-read.table(args$ancestry,header=T,as.is=T,stringsAsFactors=F,colClasses=c("IID"="character"))
	anc <- anc[! anc$FINAL == "OUTLIERS",]
}

ids<-list()
for(inp in args$input) {
	l<-unlist(strsplit(inp,","))[1]
	f<-unlist(strsplit(inp,","))[2]
	if(! l %in% names(ids)) {
		ids[[l]]<-c()
	}
	if(args$type == "fam") {
		tbl<-read.table(f,header=F,as.is=T,stringsAsFactors=F,colClasses=c("V1"="character","V2"="character"))
		if(! is.null(args$ancestry)) {
			anc <- anc[anc$IID %in% tbl[,2],]
			for(c in unique(anc$FINAL)) {
				if(paste(l," (",c,")",sep="") %in% names(ids)) {
					ids[[paste(l," (",c,")",sep="")]]<-c(ids[[paste(l," (",c,")",sep="")]],tbl[,2][tbl[,2] %in% anc$IID[anc$FINAL == c]])
				} else {
					ids[[paste(l," (",c,")",sep="")]]<-tbl[,2][tbl[,2] %in% anc$IID[anc$FINAL == c]]
				}
			}
		} else {
			ids[[l]]<-c(ids[[l]],tbl[,2])
		}
		xLabel = "Samples"
	} else if(args$type == "bim") {
		tbl<-read.table(f,header=F,as.is=T,stringsAsFactors=F)
		tbl$id<-paste(tbl$V1,tbl$V4,tbl$V5,tbl$V6,sep=":")
		ids[[l]]<-c(ids[[l]],tbl$id)
		xLabel = "Variants"
	} else {
		stop(paste("file type ",args$type," not supported",sep=""))
	}
}

if(! is.null(args$exclusions)) {
	for(excl in args$exclusions) {
		l<-unlist(strsplit(excl,","))[1]
		f<-unlist(strsplit(excl,","))[2]
		if(args$type == "fam") {
			excl_list<-scan(f, what="character")
			if(! is.null(args$ancestry)) {
				for(c in unique(anc$FINAL)) {
					ids[[paste(l," (",c,")",sep="")]]<-ids[[paste(l," (",c,")",sep="")]][! ids[[paste(l," (",c,")",sep="")]] %in% excl_list]
				}
			} else {
				ids[[l]]<-ids[[l]][! ids[[l]] %in% excl_list]
			}
		} else {
			x<-try(excl_df<-read.table(f,header=F,as.is=T,stringsAsFactors=F,sep="\t"))
			if (! inherits(x, "try-error")) {
				excl_df$ID<-gsub(",",":",gsub("\\]","",gsub("\\[","",paste(x$V1,x$V2,sep=":"))))
				ids[[l]]<-ids[[l]][! ids[[l]] %in% excl_df$ID]
			}
		}
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
if(length(ids) <= 2) {
	text_scale <- c(2.5, 2, 1.5, 1.5, 2.5, 2.5)
	point_size <- 10
	line_size <- 5
	mb_ratio = c(0.7, 0.3)
	num_angles = 0
} else if(length(ids) <= 3) { 
	text_scale <- c(2.5, 2, 1.5, 1.5, 2.5, 1.7)
	point_size <- 8
	line_size <- 4
	mb_ratio = c(0.7, 0.3)
	num_angles = 0
} else if(length(ids) <= 4) { 
	text_scale <- c(2.5, 2, 1.5, 1.5, 2.5, 1.7)
	point_size <- 6
	line_size <- 3
	mb_ratio = c(0.7, 0.3)
	num_angles = -25
} else if(length(ids) <= 5) { 
	text_scale <- c(2, 1.5, 1, 1, 2, 1)
	point_size <- 3.5
	line_size <- 1.5
	mb_ratio = c(0.65, 0.35)
	num_angles = -35
} else {
	text_scale <- c(2, 1.5, 1, 1, 2, 1)
	point_size <- 3.5
	line_size <- 1.5
	mb_ratio = c(0.6, 0.4)
	num_angles = -35
}

upset(fromList(ids), nsets=length(ids), order.by = "freq", sets.bar.color="#1F76B4", line.size = line_size, number.angles = num_angles, point.size = point_size, empty.intersections = NULL, mainbar.y.label = "Intersection Size", sets.x.label = xLabel, text.scale = text_scale, mb.ratio = mb_ratio)
dev.off()
