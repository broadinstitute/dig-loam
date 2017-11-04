library(argparse)

parser <- ArgumentParser()
parser$add_argument("--results", dest="results", type="character", help="results file")
parser$add_argument("--genes", dest="genes", type="character", help="a top results gene file")
parser$add_argument("--p", dest="p", type="character", help="a pvalue column name")
parser$add_argument("--test", dest="test", type="character", help="a statistical test")
parser$add_argument("--out", dest="out", type="character", help="an output filename")
args<-parser$parse_args()

print(args)

x<-read.table(args$results,header=T,as.is=T,sep="\t",comment.char="")
y<-read.table(args$genes,header=T,as.is=T,sep="\t",comment.char="")

names(y)[1]<-"gene"
names(y)[2]<-"X.chr"
names(y)[3]<-"pos"

if(args$test == "wald") {
	x$or <- exp(x$beta)
}

x<-merge(x,y,all=T)

for(i in 1:nrow(x)) {
	if(x$beta[i] < 0) {
		ref<-x$ref[i]
		alt<-x$alt[i]
		x$ref[i]<-alt
		x$alt[i]<-ref
		if("beta" %in% names(x)) { x$beta[i] <- -1 * x$beta[i] }
		if("or" %in% names(x)) { x$or[i] <- 1 / x$or[i] }
		if("zstat" %in% names(x)) { x$zstat[i] = -1 * x$zstat[i] }
		if("tstat" %in% names(x)) { x$tstat[i] = -1 * x$tstat[i] }
		if("dir" %in% names(x)) { x$dir[i] = gsub("b","\\+",gsub("a","-",gsub("-","b",gsub("\\+","a",x$dir[i])))) }
		if("ac" %in% names(x) & "n" %in% names(x)) { x$ac = x$n * 2 - x$ac }
		if("af" %in% names(x)) { x$af = 1 - x$af }
	}
}

cat(paste("#",paste(gsub("X.","",names(x)),collapse="\t"),"\n",sep=""), file=paste(args$out,sep=""))
write.table(x[order(x[,args$p]),], paste(args$out,sep=""), row.names=F, col.names=F, quote=F, append=T, sep="\t")