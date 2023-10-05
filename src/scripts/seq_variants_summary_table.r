library(argparse)
library(reshape2)

parser <- ArgumentParser()
parser$add_argument("--stats-in", nargs='+', dest="stats_in", type="character", help="raw sequence data stats file")
parser$add_argument("--out", dest="out", type="character", help="an output filename")
args<-parser$parse_args()

print(args)

dfs<-list()
vars_list<-list()
for(f in args$stats_in) {
	x<-unlist(strsplit(f,","))[1]
	y<-unlist(strsplit(f,","))[2]

	h<-names(read.table(y, header=T, nrows = 1, as.is=T,stringsAsFactors=F))
	h_df<-as.data.frame(h)
	h_df$t<-"NULL"
	h_df$t[h_df$h == "locus"]<-"character"
	h_df$t[h_df$h == "alleles"]<-"character"
	h_df$t[h_df$h == "qual"]<-"character"
	h_df$t[h_df$h == "filters"]<-"character"
	h_df$t[h_df$h == "was_split"]<-"character"
	h_df$t[h_df$h == "variant_qc_raw.MAF"]<-"numeric"
	dfs[[x]]<-read.table(y,header=T,as.is=T,stringsAsFactors=F,colClasses = h_df$t)
	names(dfs[[x]])[6]<-"MAF"
	dfs[[x]]$chr<-as.character(colsplit(dfs[[x]]$locus,":",names=c("chr","pos"))$chr)
	n_all=nrow(dfs[[x]])

	dfs[[x]]$freq_group<-factor(NA,levels=c("NA","[0]","(0,0.001)","[0.001,0.005)","[0.005,0.01)","[0.01,0.03)","[0.03,0.05)","[0.05,0.10)","[0.10,0.50]"))
	dfs[[x]]$indel_group<-factor(NA,levels=c("YES","NO"))
	dfs[[x]]$was_split_group<-factor(NA,levels=c("YES","NO"))
	dfs[[x]]$chr_class<-factor("Unpl",levels=c("Unpl","Auto","X","Y","XY","MT"))

	dfs[[x]]$chr_class[dfs[[x]]$chr %in% as.character(seq(1,22)) | dfs[[x]]$chr %in% paste0("chr",as.character(seq(1,22)))]<-"Auto"
	dfs[[x]]$chr_class[dfs[[x]]$chr == "23" | dfs[[x]]$chr == "X" | dfs[[x]]$chr == "chrX"]<-"X"
	dfs[[x]]$chr_class[dfs[[x]]$chr == "24" | dfs[[x]]$chr == "Y" | dfs[[x]]$chr == "chrY"]<-"Y"
	dfs[[x]]$chr_class[dfs[[x]]$chr == "25" | dfs[[x]]$chr == "XY" | dfs[[x]]$chr == "chrXY"]<-"XY"
	dfs[[x]]$chr_class[dfs[[x]]$chr == "26" | dfs[[x]]$chr == "MT" | dfs[[x]]$chr == "M" | dfs[[x]]$chr == "chrMT" | dfs[[x]]$chr == "chrM"]<-"MT"
	dfs[[x]]$freq_group[is.na(dfs[[x]]$MAF)]<-"NA"
	dfs[[x]]$freq_group[dfs[[x]]$MAF == 0]<-"[0]"
	dfs[[x]]$freq_group[dfs[[x]]$MAF > 0 & dfs[[x]]$MAF < 0.001]<-"(0,0.001)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.001 & dfs[[x]]$MAF < 0.005]<-"[0.001,0.005)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.005 & dfs[[x]]$MAF < 0.01]<-"[0.005,0.01)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.01 & dfs[[x]]$MAF < 0.03]<-"[0.01,0.03)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.03 & dfs[[x]]$MAF < 0.05]<-"[0.03,0.05)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.05 & dfs[[x]]$MAF < 0.10]<-"[0.05,0.10)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.1 & dfs[[x]]$MAF <= 0.50]<-"[0.10,0.50]"
	dfs[[x]]$indel_group[nchar(gsub(",","",gsub("\\]","",gsub("\\[","",gsub("\"","",dfs[[x]]$alleles))))) > 2]<-"YES"
	dfs[[x]]$indel_group[! (nchar(gsub(",","",gsub("\\]","",gsub("\\[","",gsub("\"","",dfs[[x]]$alleles))))) > 2)]<-"NO"
	dfs[[x]]$was_split_group[dfs[[x]]$was_split == "true"]<-"YES"
	dfs[[x]]$was_split_group[dfs[[x]]$was_split == "false"]<-"NO"
	
	vars_df<-as.data.frame.matrix(table(dfs[[x]][,c("freq_group","chr_class")]))
	indel_df<-as.data.frame.matrix(table(dfs[[x]][,c("freq_group","indel_group")]))
	names(indel_df)[1]<-"InDel"
	indel_df$NO<-NULL
	was_split_df<-as.data.frame.matrix(table(dfs[[x]][,c("freq_group","was_split_group")]))
	names(was_split_df)[1]<-"WasSplit"
	was_split_df$NO<-NULL
	vars_df<-cbind(vars_df,indel_df,was_split_df)

	vars_df$Total<-rowSums(vars_df[,c("Unpl","Auto","X","Y","XY","MT")])
	vars_df<-rbind(vars_df,colSums(vars_df))
	row.names(vars_df)[10]<-"Total"
	vars_list[[x]]<-vars_df
}

cat("Batch\tFreq\tUnpl\tAuto\tX\tY\tXY\tMT\tInDel\tWasSplit\tTotal\n",file=args$out)
cat("NA\tFreq\tUnpl\tAuto\tX\tY\tXY\tMT\tInDel\tWasSplit\tTotal\n",file=args$out,append=T)
for(x in names(vars_list)) {
	outdf<-data.frame(cbind(c(x,rep('NA',length(row.names(vars_list[[x]]))-1)),row.names(vars_list[[x]]),vars_list[[x]]))
	outdf<-data.frame(lapply(outdf,function(x) gsub("_","\\\\_",x)))
	write.table(outdf,args$out,row.names=F,col.names=F,quote=F,sep="\t",append=T)
}
