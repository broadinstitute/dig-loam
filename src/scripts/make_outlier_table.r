library(argparse)

parser <- ArgumentParser()
parser$add_argument("--ancestry-inferred-outliers", nargs='+', dest="ancestry_inferred_outliers", type="character", help="a list of labels and files, each delimited by comma (eg. ex,file1 omni,file2)")
parser$add_argument("--kinship-related", nargs='+', dest="kinship_related", type="character", help="a list of labels and files, each delimited by comma (eg. ex,file1 omni,file2)")
parser$add_argument("--kinship-famsizes", nargs='+', dest="kinship_famsizes", type="character", help="a list of labels and files, each delimited by comma (eg. ex,file1 omni,file2)")
parser$add_argument("--imiss", nargs='+', dest="imiss", type="character", help="a list of labels and files, each delimited by comma (eg. ex,file1 omni,file2)")
parser$add_argument("--sampleqc-outliers", nargs='+', dest="sampleqc_outliers", type="character", help="a list of labels and files, each delimited by comma (eg. ex,file1 omni,file2)")
parser$add_argument("--sexcheck-problems", nargs='+', dest="sexcheck_problems", type="character", help="a list of labels and files, each delimited by comma (eg. ex,file1 omni,file2)")
parser$add_argument("--final-exclusions", nargs='+', dest="final_exclusions", type="character", help="a list of labels and files, each delimited by comma (eg. ex,file1 omni,file2)")
parser$add_argument("--out", dest="out", type="character", help="an output filename ending in '.png' or '.pdf'")
args<-parser$parse_args()

print(args)

ids<-list()

print("reading ancestry inferred file")
for(a in args$ancestry_inferred_outliers) {
	l<-unlist(strsplit(a,","))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,","))[2]
	ancestry_df<-read.table(f,header=F,as.is=T,stringsAsFactors=F)
	x<-ancestry_df[ancestry_df$V2 == "OUTLIERS",]
	ids[[l]][["ancestry outlier"]]<-x$V1
}

print("reading kinship file")
for(a in args$kinship_related) {
	l<-unlist(strsplit(a,","))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,","))[2]
	kinship_df<-read.table(f,header=T,as.is=T,stringsAsFactors=F,colClasses=c("FID1"="character","ID1"="character","FID2"="character","ID2"="character"))
	dups_df <- kinship_df[kinship_df$Kinship > 0.4,]
	ids[[l]][['duplicate']]<-unique(c(dups_df$ID1,dups_df$ID2))
}

print("reading famsizes file")
for(a in args$kinship_famsizes) {
	l<-unlist(strsplit(a,","))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,","))[2]
	famsizes_df<-try(read.table(f,header=F,as.is=T,stringsAsFactors=F,colClasses=c("V1"="character")), silent=TRUE)
	if(inherits(famsizes_df, "try-error")) {
		ids[[l]][['cryptic relatedness']]<-c()
	} else {
		y<-famsizes_df[famsizes_df$V2 >= 10,]
		ids[[l]][['cryptic relatedness']]<-unique(y$V1[y$V2 >= 10])
	}
}

print("reading imiss file")
for(a in args$imiss) {
	l<-unlist(strsplit(a,","))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,","))[2]
	imiss_df<-try(read.table(f,header=F,as.is=T,stringsAsFactors=F,colClasses=c("V1"="character","V2"="character")), silent=TRUE)
	if(inherits(imiss_df, "try-error")) {
		ids[[l]][['extreme missingness']]<-c()
	} else {
		ids[[l]][['extreme missingness']]<-unique(imiss_df$V2)
	}
}

print("reading sexcheck problems file")
for(a in args$sexcheck_problems) {
	l<-unlist(strsplit(a,","))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,","))[2]
	sexcheck_df<-read.table(f,header=T,as.is=T,stringsAsFactors=F,colClasses=c("IID"="character"))
	ids[[l]][['sex check']]<-unique(sexcheck_df$IID)
}

metrics<-list()
print("reading sampleqc outliers file")
for(a in args$sampleqc_outliers) {
	metrics[[a]]<-c()
	l<-unlist(strsplit(a,","))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,","))[2]
	sampleqc_df<-read.table(f,header=T,as.is=T,stringsAsFactors=F,colClasses=c("IID"="character"))
	for(metric in unique(sampleqc_df$METRIC)) {
		ids[[l]][[metric]]<-unique(sampleqc_df$IID[sampleqc_df$METRIC == metric])
		metrics[[l]] <- unique(c(metrics[[l]], metric))
	}
	ids[[l]][['metric pca']]<-unique(sampleqc_df$IID[sampleqc_df$OUTLIER_PCA == 1])
}

print("reading final exclusions files")
for(a in args$final_exclusions) {
	l<-unlist(strsplit(a,","))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,","))[2]
	final<-scan(f, what="character")
	ids[[l]][['final']]<-unique(final)
}

for(a in ls(ids)) {
	ids[[a]][['all removed']]<-ids[[a]][["ancestry outlier"]]
	for(l in c("extreme missingness","duplicate","cryptic relatedness","sex check",metrics[[a]],"metric pca")) {
		ids[[a]][['all removed']]<-unique(c(ids[[a]][['all removed']],ids[[a]][[l]]))
	}
	ids[[a]][['manually reinstated']]<-ids[[a]][['all removed']][! ids[[a]][['all removed']] %in% ids[[a]][['final']]]
}

header="Method"
arrays<-ls(ids)
ncols = 1
for(a in arrays) {
	header = paste(header,paste("\t",a,sep=""),sep="")
	ncols = ncols + 1
}
header = paste(header,"\tTotal",sep="")
ncols = ncols + 1
cat(paste(header,"\n",sep=""),file=args$out)

for(m in unique(unlist(metrics))) {
	l = gsub("_","\\\\_", gsub("_res$","",m))
	for(a in arrays) {
		l = paste(l,paste("\t",length(ids[[a]][[m]]),sep=""),sep="")
	}
	n<-unlist(sapply(ids, function(z) z[m]))
	n<-n[! is.na(n)]
	l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
	cat(paste(l,"\n",sep=""),file=args$out, append=T)
}
spacer = "{}"
for(i in 1:(ncols-1)) {
	spacer<-paste(spacer,"\t{}",sep="")
}
spacer<-paste(spacer,sep="")
cat(paste(spacer,"\n",sep=""),file=args$out,append=T)

l="PCA(Metrics)"
for(a in arrays) {
	l = paste(l,paste("\t",length(ids[[a]][['metric pca']]),sep=""),sep="")
}
n<-unlist(sapply(ids, function(z) z['metric pca']))
n<-n[! is.na(n)]
l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
cat(paste(l,"\n",sep=""),file=args$out, append=T)

cat(paste(spacer,"\n",sep=""),file=args$out,append=T)

l = "Metrics+PCA(Metrics)"
ids_allmetrics<-c()
for(a in arrays) {
	id=c()
	for(m in c(unique(unlist(metrics)),'metric pca')) {
		id = unique(c(id,ids[[a]][[m]]))
	}
	l = paste(l,paste("\t",length(unique(id)),sep=""),sep="")
	ids_allmetrics<-c(ids_allmetrics,id)
}
l = paste(l,paste("\t",length(unique(ids_allmetrics)),sep=""),sep="")
cat(paste(l,"\n",sep=""),file=args$out, append=T)

cat(paste(spacer,"\n",sep=""),file=args$out,append=T)

l="Extreme Missingness"
for(a in arrays) {
	l = paste(l,paste("\t",length(ids[[a]][['extreme missingness']]),sep=""),sep="")
}
n<-unlist(sapply(ids, function(z) z['extreme missingness']))
if(length(n) > 0) {
	n<-n[! is.na(n)]
}
l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
cat(paste(l,"\n",sep=""),file=args$out, append=T)

l="Duplicates"
for(a in arrays) {
	l = paste(l,paste("\t",length(ids[[a]][['duplicate']]),sep=""),sep="")
}
n<-unlist(sapply(ids, function(z) z['duplicate']))
n<-n[! is.na(n)]
l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
cat(paste(l,"\n",sep=""),file=args$out, append=T)

l="Cryptic Relatedness"
for(a in arrays) {
	l = paste(l,paste("\t",length(ids[[a]][['cryptic relatedness']]),sep=""),sep="")
}
n<-unlist(sapply(ids, function(z) z['cryptic relatedness']))
if(length(n) > 0) {
	n<-n[! is.na(n)]
}
l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
cat(paste(l,"\n",sep=""),file=args$out, append=T)

l="Sexcheck"
for(a in arrays) {
	l = paste(l,paste("\t",length(ids[[a]][['sex check']]),sep=""),sep="")
}
n<-unlist(sapply(ids, function(z) z['sex check']))
n<-n[! is.na(n)]
l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
cat(paste(l,"\n",sep=""),file=args$out, append=T)

l="Ancestry Outlier"
for(a in arrays) {
	l = paste(l,paste("\t",length(ids[[a]][['ancestry outlier']]),sep=""),sep="")
}
n<-unlist(sapply(ids, function(z) z['ancestry outlier']))
if(length(n) > 0) {
	n<-n[! is.na(n)]
}
l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
cat(paste(l,"\n",sep=""),file=args$out, append=T)

cat(paste(spacer,"\n",sep=""),file=args$out,append=T)

l = "Total"
ids_allmetrics<-c()
for(a in arrays) {
	l = paste(l,paste("\t",length(unique(unlist(ids[[a]]))),sep=""),sep="")
	ids_allmetrics<-c(ids_allmetrics,unique(unlist(ids[[a]])))
}
l = paste(l,paste("\t",length(unique(ids_allmetrics)),sep=""),sep="")
cat(paste(l,"\n",sep=""),file=args$out, append=T)
