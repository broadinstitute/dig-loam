library(argparse)

parser <- ArgumentParser()
parser$add_argument("--ancestry-in", dest="ancestry_in", type="character", help="comma separated list of filenames containing inferred ancestry in 'IID ANCESTRY' format (eg. SAMPLE1   AFR). The order of the files indicates the hierarchy under which samples are assigned to ancestry groups.")
parser$add_argument("--out-table", dest="out_table", type="character", help="an output filename for ancestry inferrence table")
parser$add_argument("--out-outliers", dest="out_outliers", type="character", help="an output filename for list of ancestry outliers")
args<-parser$parse_args()

print(args)

library(reshape2)

files = unlist(strsplit(args$ancestry_in, split=","))

x<-data.frame(FILE=files,stringsAsFactors=F)
for(i in 1:length(files)) {
	df<-read.table(files[i],header=F,as.is=T,stringsAsFactors=F)
	names(df)<-c("IID",as.character(i))
	if(i == 1) {
		out<-df
	} else {
		out<-merge(out,df,all=T)
	}
}
out$FINAL<-NA
out$AGREE<-0

for(i in 1:nrow(out)) {
	for(l in as.character(rev(1:length(files)))) {
		if(! is.na(out[,l][i]) & out[,l][i] != "OUTLIERS") {
			out$FINAL[i]<-out[,l][i]
		}
	}
	un<-unique(unlist(out[i,2:(ncol(out)-2)]))
	if(length(un[! is.na(un) & un != "OUTLIERS"]) == 1) {
		out$AGREE[i]<-1
	}
}

out$FINAL[is.na(out$FINAL)]<-"OUTLIERS"
write.table(out[,c("IID","FINAL")],args$out_table,row.names=F,col.names=T,sep="\t",append=F,quote=F)
write.table(out$IID[out$FINAL == "OUTLIERS"],args$out_outliers,row.names=F,col.names=F,sep="\t",append=F,quote=F)
print(table(out$FINAL))

