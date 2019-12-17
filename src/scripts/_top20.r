library(argparse)

parser <- ArgumentParser()
parser$add_argument("--results", dest="results", type="character", help="results file")
parser$add_argument("--chr", dest="chr", type="character", help="chromosome column name")
parser$add_argument("--pos", dest="pos", type="character", help="position column name")
parser$add_argument("--genes", dest="genes", type="character", help="a top results gene file")
parser$add_argument("--known-loci", dest="known_loci", help='a comma separated list of known loci files')
parser$add_argument("--p", dest="p", type="character", help="a pvalue column name")
parser$add_argument("--test", dest="test", type="character", help="a statistical test")
parser$add_argument("--out", dest="out", type="character", help="an output filename")
args<-parser$parse_args()

print(args)

x<-read.table(args$results,header=T,as.is=T,sep="\t",comment.char="")

if(args$test %in% c("hail.wald","hail.lrt","hail.firth")) {
	x$or <- exp(x$beta)
	pre<-names(x)[1:(grep("\\bpval\\b",names(x))-1)]
	pre<-pre[grep("\\bor\\b",pre,invert=TRUE)]
	post<-names(x)[grep("\\bpval\\b",names(x)):length(names(x))]
	post<-post[grep("\\bor\\b",post,invert=TRUE)]
	x <- x[,c(pre,"or",post)]
}

y<-try(read.table(args$genes,header=F,as.is=T,sep="\t",comment.char=""), silent=TRUE)
if(! inherits(y, "try-error")) {
	names(y)[1]<-"gene"
	if(grepl("#",args$chr )) {
		names(y)[2]<-gsub("#","X.",args$chr)
	} else {
		names(y)[2]<-args$chr
	}
	names(y)[3]<-args$pos
	x<-merge(x,y,all=T)
	x$gene[is.na(x$gene)]<-"NA"
} else {
	x$gene<-NA
}

for(i in 1:nrow(x)) {
	if(x$beta[i] < 0) {
		ref<-x$ref[i]
		alt<-x$alt[i]
		x$ref[i]<-alt
		x$alt[i]<-ref
		if("beta" %in% names(x)) { x$beta[i] <- -1 * x$beta[i] }
		if("or" %in% names(x)) { x$or[i] <- 1 / x$or[i] }
		if("z_stat" %in% names(x)) { x$z_stat[i] = -1 * x$z_stat[i] }
		if("t_stat" %in% names(x)) { x$t_stat[i] = -1 * x$t_stat[i] }
		if("dir" %in% names(x)) { x$dir[i] = gsub("b","\\+",gsub("a","-",gsub("-","b",gsub("\\+","a",x$dir[i])))) }
		if("ac" %in% names(x) & "n" %in% names(x)) { x$ac = x$n * 2 - x$ac }
		if("af" %in% names(x)) { x$af = 1 - x$af }
	}
}

x <- x[order(x[,args$p]),]
x <- x[! duplicated(x$gene),]
x <- head(x, n=20)

known_vars <- c()
known_genes <- c()
if(args$known_loci != "") {
	if(length(unlist(strsplit(args$known_loci,","))) > 0) {
		for(k in unlist(strsplit(args$known_loci,","))) {
			known <- read.table(k, header=T, as.is=T, stringsAsFactors=F)
			known_vars <- c(known_vars, known$SNP_A, known$SNP_B)
			known_genes <- c(known_genes, known$CLOSEST_GENE)
		}
	}
}
known_vars <- unique(known_vars)
known_genes <- unique(known_genes)

x$id[x$id %in% known_vars]<-paste("\\large{\\textbf{",x$id[x$id %in% known_vars],"}}",sep="")
x$gene[x$gene %in% known_genes]<-paste("\\large{\\textbf{",x$gene[x$gene %in% known_genes],"}}",sep="")

## replace _ with \_ to make compatible with pgfplotstabletypeset
# depracated due to unknown changes
#x <- data.frame(lapply(x, FUN=function(z) gsub("_","\\\\_",z)))

# remove any columns with all missing values
x <- Filter(function(a) !(all(is.na(a))), x)

cat(paste(paste(gsub("X.","",names(x)),collapse="\t"),"\n",sep=""), file=paste(args$out,sep=""))

write.table(x, args$out, row.names=F, col.names=F, quote=F, append=T, sep="\t", na="nan")
