library(argparse)

parser <- ArgumentParser()
parser$add_argument("--kin0", dest="kin0", type="character", help="King kin0 file")
parser$add_argument("--out", dest="out", type="character", help="an output file name")
args<-parser$parse_args()

print(args)

x<-try(read.table(args$kin0,header=T,as.is=T,stringsAsFactors=F,colClasses=c("FID1"="character","ID1"="character","FID2"="character","ID2"="character")), silent=TRUE)
if(inherits(x, "try-error")) {
	file.create(args$out)
} else {
	if(nrow(x) == 0) {
		file.create(args$out)
	} else {
		ids<-c(x$ID1,x$ID2)
		out<-as.data.frame(sort(table(ids),decreasing=T))
		names(out)[1]<-"IID"
		names(out)[2]<-"FREQ"
		write.table(out[,c("IID","FREQ")],args$out,row.names=F,col.names=F,quote=F,append=F,sep="\t")
	}
}
