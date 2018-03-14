library(argparse)

parser <- ArgumentParser()
parser$add_argument("--kin0", dest="kin0", type="character", help="King kin0 file")
parser$add_argument("--out", dest="out", type="character", help="an output file name")
args<-parser$parse_args()

print(args)

x<-try(read.table(args$kin0,header=T,as.is=T,stringsAsFactors=F), silent=TRUE)
if(inherits(x, "try-error")) {
	file.create(args$out)
} else {
	ids<-c(x$ID1,x$ID2)
	out<-as.data.frame(sort(table(ids),decreasing=T))
	names(out)[1]<-"FREQ"
	out$IID<-row.names(out)
	write.table(out[,c("IID","FREQ")],args$out,row.names=F,col.names=F,quote=F,append=F,sep="\t")
}
