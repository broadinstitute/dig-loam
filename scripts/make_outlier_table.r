library(argparse)

parser <- ArgumentParser()
parser$add_argument("--ancestry-inferred-outliers", dest="ancestry_inferred_outliers", type="character", help="a comma separated list of labels and files, each delimited by three underscores (eg. ex___file1,omni___file2)")
parser$add_argument("--kinship-related", dest="kinship_related", type="character", help="a comma separated list of labels and files, each delimited by three underscores (eg. ex___file1,omni___file2)")
parser$add_argument("--kinship-famsizes", dest="kinship_famsizes", type="character", help="a comma separated list of labels and files, each delimited by three underscores (eg. ex___file1,omni___file2)")
parser$add_argument("--sampleqc-outliers", dest="sampleqc_outliers", type="character", help="a comma separated list of labels and files, each delimited by three underscores (eg. ex___file1,omni___file2)")
parser$add_argument("--sexcheck-problems", dest="sexcheck_problems", type="character", help="a comma separated list of labels and files, each delimited by three underscores (eg. ex___file1,omni___file2)")
parser$add_argument("--final-exclusions", dest="final_exclusions", type="character", help="a comma separated list of labels and files, each delimited by three underscores (eg. ex___file1,omni___file2)")
parser$add_argument("--out", dest="out", type="character", help="an output filename ending in '.png' or '.pdf'")
args<-parser$parse_args()

print(args)

ids<-list()

print("reading ancestry inferred file")
x<-scan(args$ancestry_inferred_outliers,what="character")
if(length(x) > 0) {
	ids[["ancestry outlier"]]<-x
}

print("reading kinship file")
x<-unlist(strsplit(args$kinship_related,","))
for(a in x) {
	l<-unlist(strsplit(a,"___"))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,"___"))[2]
	kinship_df<-read.table(f,header=T,as.is=T,stringsAsFactors=F)
	dups_df <- kinship_df[kinship_df$Kinship > 0.4,]
	ids[[l]][['duplicate']]<-unique(c(dups_df$ID1,dups_df$ID2))
}

print("reading famsizes file")
x<-unlist(strsplit(args$kinship_famsizes,","))
for(a in x) {
	l<-unlist(strsplit(a,"___"))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,"___"))[2]
	famsizes_df<-read.table(f,header=F,as.is=T,stringsAsFactors=F)
	y<-famsizes_df[famsizes_df$V2 >= 10,]
	ids[[l]][['cryptic relatedness']]<-unique(y$V1[y$V2 >= 10])
}

print("reading sexcheck problems file")
x<-unlist(strsplit(args$sexcheck_problems,","))
for(a in x) {
	l<-unlist(strsplit(a,"___"))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,"___"))[2]
	sexcheck_df<-read.table(f,header=T,as.is=T,stringsAsFactors=F)
	ids[[l]][['sex check']]<-unique(sexcheck_df$IID)
}

metrics<-list()
print("reading sampleqc outliers file")
x<-unlist(strsplit(args$sampleqc_outliers,","))
for(a in x) {
	metrics[[a]]<-c()
	l<-unlist(strsplit(a,"___"))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,"___"))[2]
	sampleqc_df<-read.table(f,header=T,as.is=T,stringsAsFactors=F)
	for(metric in unique(sampleqc_df$METRIC)) {
		ids[[l]][[metric]]<-unique(sampleqc_df$IID[sampleqc_df$METRIC == metric])
		metrics[[l]] <- unique(c(metrics[[l]], metric))
	}
	ids[[l]][['metric pca']]<-unique(sampleqc_df$IID[sampleqc_df$OUTLIER_PCA == 1])
}

print("reading final exclusions files")
x<-unlist(strsplit(args$final_exclusions,","))
for(a in x) {
	l<-unlist(strsplit(a,"___"))[1]
	if(! l %in% ls(ids)) ids[[l]]<-list()
	f<-unlist(strsplit(a,"___"))[2]
	final<-scan(f, what="character")
	ids[[l]][['final']]<-unique(final)
}

for(a in ls(ids)[ls(ids) != "ancestry outlier"]) {
	ids[[a]][['all removed']]<-ids[["ancestry outlier"]]
	for(l in c("duplicate","cryptic relatedness","sex check",metrics[[a]],"metric pca")) {
		ids[[a]][['all removed']]<-unique(c(ids[[a]][['all removed']],ids[[a]][[l]]))
	}
	ids[[a]][['manually reinstated']]<-ids[[a]][['all removed']][! ids[[a]][['all removed']] %in% ids[[a]][['final']]]
}

header="Method"
arrays<-ls(ids)[ls(ids) != "ancestry outlier"]
ncols = 1
for(a in arrays) {
	header = paste(header,paste("\t",a,sep=""),sep="")
	ncols = ncols + 1
}
header = paste(header,"\tTotal",sep="")
ncols = ncols + 1
cat(header,"\n",file=args$out)

for(m in unique(unlist(metrics))) {
	l = gsub("_res$","",m)
	for(a in arrays) {
		l = paste(l,paste("\t",length(ids[[a]][[m]]),sep=""),sep="")
	}
	n<-unlist(sapply(ids, function(z) z[m]))
	n<-n[! is.na(n)]
	l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
	cat(l,"\n",file=args$out, append=T)
}
spacer = "{}"
for(i in 1:(ncols-1)) {
	spacer<-paste(spacer,"\t{}",sep="")
}
spacer<-paste(spacer,sep="")
cat(spacer,"\n",file=args$out,append=T)

l="PCA(Metrics)"
for(a in arrays) {
	l = paste(l,paste("\t",length(ids[[a]][['metric pca']]),sep=""),sep="")
}
n<-unlist(sapply(ids, function(z) z['metric pca']))
n<-n[! is.na(n)]
l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
cat(l,"\n",file=args$out, append=T)

cat(spacer,"\n",file=args$out,append=T)

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
cat(l,"\n",file=args$out, append=T)

cat(spacer,"\n",file=args$out,append=T)
cat(spacer,"\n",file=args$out,append=T)
cat(spacer,"\n",file=args$out,append=T)

l="Duplicates"
for(a in arrays) {
	l = paste(l,paste("\t",length(ids[[a]][['duplicate']]),sep=""),sep="")
}
n<-unlist(sapply(ids, function(z) z['duplicate']))
n<-n[! is.na(n)]
l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
cat(l,"\n",file=args$out, append=T)

cat(spacer,"\n",file=args$out,append=T)

l="Cryptic Relatedness"
for(a in arrays) {
	l = paste(l,paste("\t",length(ids[[a]][['cryptic relatedness']]),sep=""),sep="")
}
n<-unlist(sapply(ids, function(z) z['cryptic relatedness']))
n<-n[! is.na(n)]
l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
cat(l,"\n",file=args$out, append=T)

cat(spacer,"\n",file=args$out,append=T)

l="Sexcheck"
for(a in arrays) {
	l = paste(l,paste("\t",length(ids[[a]][['sex check']]),sep=""),sep="")
}
n<-unlist(sapply(ids, function(z) z['sex check']))
n<-n[! is.na(n)]
l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
cat(l,"\n",file=args$out, append=T)

cat(spacer,"\n",file=args$out,append=T)

l="Ancestry Outlier"
for(a in arrays) {
	l = paste(l,paste("\t",length(ids[[a]][['ancestry outlier']]),sep=""),sep="")
}
n<-unlist(sapply(ids, function(z) z['ancestry outlier']))
n<-n[! is.na(n)]
l = paste(l,paste("\t",length(unique(n)),sep=""),sep="")
cat(l,"\n",file=args$out, append=T)

cat(spacer,"\n",file=args$out,append=T)
cat(spacer,"\n",file=args$out,append=T)
cat(spacer,"\n",file=args$out,append=T)

l = "Total"
ids_allmetrics<-c()
for(a in arrays) {
	l = paste(l,paste("\t",length(unique(unlist(ids[[a]]))),sep=""),sep="")
	ids_allmetrics<-c(ids_allmetrics,unique(unlist(ids[[a]])))
}
l = paste(l,paste("\t",length(unique(ids_allmetrics)),sep=""),sep="")
cat(l,"\n",file=args$out, append=T)
