library(argparse)

parser <- ArgumentParser()
parser$add_argument("--inferred", dest="inferred", type="character", help="a merged ancestry file")
parser$add_argument("--out", dest="out", type="character", help="an output filename for final ancestry assignments")
args<-parser$parse_args()

print(args)

final<-read.table(args$inferred,header=T,as.is=T,stringsAsFactors=F,colClasses=c("character","character"))
cat("Population\tSamples\n",file=args$out)
for(x in c("AFR","AMR","EAS","EUR","SAS")) {
	if(nrow(final[final$FINAL == x,]) > 0) {
		cat(gsub("_","\\\\_",paste(paste(x,nrow(final[final$FINAL == x,]),sep="\t"),"\n",sep="")),file=args$out,append=T)
	}
}
