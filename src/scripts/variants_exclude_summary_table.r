library(argparse)
library(reshape2)

parser <- ArgumentParser()
parser$add_argument("--stats", nargs='+', dest="stats", type="character", help="Plink --freq file list")
parser$add_argument("--out", dest="out", type="character", help="an output filename")
args<-parser$parse_args()

print(args)

all_filters<-c()
dfs<-list()
vars_list<-list()
for(f in args$stats) {
	x<-unlist(strsplit(f,","))[1]
	y<-unlist(strsplit(f,","))[2]
	h<-names(read.table(y, header=T, nrows = 1, as.is=T, stringsAsFactors=F, sep="\t"))
	h_filter_cols<-h[grep("ls_filters",h)]
	h_df<-as.data.frame(h)
	h_df$t<-"NULL"
	for(c in h_filter_cols) {
		h_df$t[h_df$h == c]<-"integer"
	}
	h_df$t[h_df$h == "variant_qc.MAF"]<-"numeric"
	dfs[[x]]<-read.table(y, header=T, as.is=T, stringsAsFactors=F, sep="\t", colClasses = h_df$t)
	names(dfs[[x]])[grep("variant_qc.MAF",names(dfs[[x]]))]<-"MAF"
	filters<-gsub("ls_filters.","",h_filter_cols[grep("exclude",h_filter_cols,invert=T)])
	all_filters <- unique(c(all_filters,filters))
	n_all<-nrow(dfs[[x]])
	dfs[[x]]$freq_group<-factor(NA,levels=c("[0]","(0,0.001)","[0.001,0.005)","[0.005,0.01)","[0.01,0.03)","[0.03,0.05)","[0.05,0.10)","[0.10,0.50]"))
	for(f in filters) {
		dfs[[x]][paste0(f,"_group")]<-factor(NA,levels=c("YES","NO"))
	}
	dfs[[x]]$freq_group[dfs[[x]]$MAF == 0]<-"[0]"
	dfs[[x]]$freq_group[dfs[[x]]$MAF > 0 & dfs[[x]]$MAF < 0.001]<-"(0,0.001)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.001 & dfs[[x]]$MAF < 0.005]<-"[0.001,0.005)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.005 & dfs[[x]]$MAF < 0.01]<-"[0.005,0.01)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.01 & dfs[[x]]$MAF < 0.03]<-"[0.01,0.03)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.01 & dfs[[x]]$MAF < 0.03]<-"[0.01,0.03)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.03 & dfs[[x]]$MAF < 0.05]<-"[0.03,0.05)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.05 & dfs[[x]]$MAF < 0.10]<-"[0.05,0.10)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.1 & dfs[[x]]$MAF <= 0.50]<-"[0.10,0.50]"
	for(f in filters) {
		dfs[[x]][paste0(f,"_group")]<-"NO"
		dfs[[x]][paste0(f,"_group")][dfs[[x]][paste0("ls_filters.",f)] == 1]<-"YES"
	}
	dfs[[x]]$exclude <- rowSums(dfs[[x]][h_filter_cols[grep("exclude",h_filter_cols,invert=T)]] == 1)
	dfs[[x]]$exclude_group <- "NO"
	dfs[[x]]$exclude_group[dfs[[x]]$exclude > 0] <- "YES"
	n<-0
	for(f in filters) {
		n<-n+1
		new_df<-as.data.frame.matrix(table(dfs[[x]][,c("freq_group",paste0(f,"_group"))]))
		if(! "YES" %in% names(new_df)) {
			new_df$YES<-0
		}
		names(new_df)[2]<-f
		new_df$NO<-NULL
		if(n == 1) {
			vars_df<-new_df
		} else {
			vars_df<-cbind(vars_df,new_df)
		}
	}
	new_df<-as.data.frame.matrix(table(dfs[[x]][,c("freq_group","exclude_group")]))
	if(! "YES" %in% names(new_df)) {
		new_df$YES<-0
	}
	names(new_df)[2]<-"Total"
	new_df$NO<-NULL
	vars_df<-cbind(vars_df,new_df)
	vars_df<-rbind(vars_df,colSums(vars_df))
	row.names(vars_df)[9]<-"Total"
	vars_list[[x]]<-vars_df
}
h1 <- "Batch\tMAF"
for(f in all_filters) {
	h1 <- paste0(h1,"\t",f)
}
h1 <- paste0(h1,"\tTotal\n")
cat(h1,file=args$out)
h2 <- "NA\tMAF"
for(f in all_filters) {
	h2 <- paste0(h2,"\t",f)
}
h2 = paste0(h2,"\tTotal\n")
cat(h2,file=args$out,append=T)

for(x in names(vars_list)) {
	outdf<-data.frame(cbind(c(x,rep('NA',length(row.names(vars_list[[x]]))-1)),row.names(vars_list[[x]]),vars_list[[x]]))
	outdf<-data.frame(lapply(outdf,function(x) gsub("_","\\\\_",x)))
	write.table(outdf,args$out,row.names=F,col.names=F,quote=F,sep="\t",append=T)
}
