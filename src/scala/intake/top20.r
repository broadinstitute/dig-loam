library(argparse)

parser <- ArgumentParser()
parser$add_argument("--results", dest="results", type="character", help="results file")
parser$add_argument("--p", dest="p", type="character", help="a pvalue column name")
parser$add_argument("--out", dest="out", type="character", help="an output filename")
args<-parser$parse_args()

print(args)

x<-read.table(args$results,header=T,as.is=T,sep="\t")

x <- x[order(x[,args$p]),]
x <- x[! duplicated(x$GENE),]
x <- head(x, n=20)

write.table(x, args$out, row.names=F, col.names=T, quote=F, append=F, sep="\t", na="nan")
