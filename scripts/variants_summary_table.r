library(argparse)

parser <- ArgumentParser()
parser$add_argument("--freq-in", dest="freq_in", type="character", help="Plink --freq file")
parser$add_argument("--indel-in", dest="indel_in", type="character", help="indel list")
parser$add_argument("--multi-in", dest="multi_in", type="character", help="multiallelic variants list")
parser$add_argument("--dupl-in", dest="dupl_in", type="character", help="duplicate variants list")
parser$add_argument("--out", dest="out", type="character", help="an output filename")
args<-parser$parse_args()

print(args)

indels<-list()
for(f in unlist(strsplit(args$indel_in,','))) {
	x<-unlist(strsplit(f,"___"))[1]
	y<-unlist(strsplit(f,"___"))[2]
	indels[[x]]<-scan(file=y, what="character")
}

multis<-list()
for(f in unlist(strsplit(args$multi_in,','))) {
	x<-unlist(strsplit(f,"___"))[1]
	y<-unlist(strsplit(f,"___"))[2]
	multis[[x]]<-scan(file=y, what="character")
}

dupls<-list()
for(f in unlist(strsplit(args$dupl_in,','))) {
	x<-unlist(strsplit(f,"___"))[1]
	y<-unlist(strsplit(f,"___"))[2]
	dupls[[x]]<-scan(file=y, what="character")
}

dfs<-list()
vars_list<-list()
for(f in unlist(strsplit(args$freq_in,','))) {
	x<-unlist(strsplit(f,"___"))[1]
	y<-unlist(strsplit(f,"___"))[2]

	dfs[[x]]<-read.table(y,header=T,as.is=T,stringsAsFactors=F)

	dfs[[x]]$freq_group<-factor(NA,levels=c("[0]","(0,0.01)","[0.01,0.03)","[0.03,0.05)","[0.05,0.10)","[0.10,0.50]"))
	dfs[[x]]$indel_group<-factor(NA,levels=c("YES","NO"))
	dfs[[x]]$multi_group<-factor(NA,levels=c("YES","NO"))
	dfs[[x]]$dupl_group<-factor(NA,levels=c("YES","NO"))
	dfs[[x]]$chr_class<-factor(NA,levels=c("Unpl","Auto","X","Y","X(PAR)","Mito"))

	dfs[[x]]$chr_class[dfs[[x]]$CHR == 0]<-"Unpl"
	dfs[[x]]$chr_class[dfs[[x]]$CHR >= 1 & dfs[[x]]$CHR <= 22]<-"Auto"
	dfs[[x]]$chr_class[dfs[[x]]$CHR == 23]<-"X"
	dfs[[x]]$chr_class[dfs[[x]]$CHR == 24]<-"Y"
	dfs[[x]]$chr_class[dfs[[x]]$CHR == 25]<-"X(PAR)"
	dfs[[x]]$chr_class[dfs[[x]]$CHR == 26]<-"Mito"
	dfs[[x]]$freq_group[dfs[[x]]$MAF == 0]<-"[0]"
	dfs[[x]]$freq_group[dfs[[x]]$MAF > 0 & dfs[[x]]$MAF < 0.01]<-"(0,0.01)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.01 & dfs[[x]]$MAF < 0.03]<-"[0.01,0.03)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.03 & dfs[[x]]$MAF < 0.05]<-"[0.03,0.05)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.05 & dfs[[x]]$MAF < 0.10]<-"[0.05,0.10)"
	dfs[[x]]$freq_group[dfs[[x]]$MAF >= 0.1 & dfs[[x]]$MAF <= 0.50]<-"[0.10,0.50]"
	dfs[[x]]$indel_group[dfs[[x]]$SNP %in% indels[[x]]]<-"YES"
	dfs[[x]]$indel_group[! dfs[[x]]$SNP %in% indels[[x]]]<-"NO"
	dfs[[x]]$multi_group[dfs[[x]]$SNP %in% multis[[x]]]<-"YES"
	dfs[[x]]$multi_group[! dfs[[x]]$SNP %in% multis[[x]]]<-"NO"
	dfs[[x]]$dupl_group[dfs[[x]]$SNP %in% dupls[[x]]]<-"YES"
	dfs[[x]]$dupl_group[! dfs[[x]]$SNP %in% dupls[[x]]]<-"NO"
	
	vars_df<-as.data.frame.matrix(table(dfs[[x]][,c("freq_group","chr_class")]))
	indel_df<-as.data.frame.matrix(table(dfs[[x]][,c("freq_group","indel_group")]))
	names(indel_df)[1]<-"InDel"
	indel_df$NO<-NULL
	multi_df<-as.data.frame.matrix(table(dfs[[x]][,c("freq_group","multi_group")]))
	names(multi_df)[1]<-"Multi"
	multi_df$NO<-NULL
	dupl_df<-as.data.frame.matrix(table(dfs[[x]][,c("freq_group","dupl_group")]))
	names(dupl_df)[1]<-"Dup"
	dupl_df$NO<-NULL
	vars_df<-cbind(vars_df,indel_df,multi_df,dupl_df)

	vars_df$Total<-rowSums(vars_df[,c("Unpl","Auto","X","Y","X(PAR)","Mito")])
	vars_df<-rbind(vars_df,colSums(vars_df))
	row.names(vars_df)[7]<-"Total"
	vars_list[[x]]<-vars_df
}

cat("Array\tFreq\tUnpl\tAuto\tX\tY\tX(PAR)\tMito\tInDel\tMulti\tDup\tTotal\n",file=args$out)
for(x in names(vars_list)) {
	cat(paste(x,"\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",sep=""),file=args$out,append=T)
	write.table(cbind('{}',row.names(vars_list[[x]]),vars_list[[x]]),args$out,row.names=F,col.names=F,quote=F,sep="\t",append=T)
	if(x != names(vars_list[[x]])[length(names(vars_list[[x]]))]) {
		cat("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",file=args$out,append=T)
	}
}
