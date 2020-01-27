library(argparse)

parser <- ArgumentParser()
parser$add_argument("--bim-in", dest="bim_in", type="character", help="Plink bim file")
parser$add_argument("--freq-in", dest="freq_in", type="character", help="Plink frq file")
parser$add_argument("--miss-in", dest="miss_in", type="character", help="Plink lmiss file")
parser$add_argument("--out", dest="out", type="character", help="an output file name")
args<-parser$parse_args()

print(args)

print("read in bim file")
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

dat<-try(read.table(args$bim_in,header=F,as.is=T,stringsAsFactors=F), silent=TRUE)
if(! inherits(dat, "try-error")) {
	names(dat)[1]<-"CHR"
	names(dat)[2]<-"SNP"
	names(dat)[3]<-"CM"
	names(dat)[4]<-"POS"
	names(dat)[5]<-"A1"
	names(dat)[6]<-"A2"
	dat$CHRPOS<-paste(dat$CHR,":",dat$POS,sep="")
	dups<-dat$CHRPOS[duplicated(dat$CHRPOS)]
	print("extract duplicate CHR:POS pairs")
	dat<-dat[dat$CHRPOS %in% dups,]
	dat$remove<-0
	dat$check<-0
	print(paste0("data contains ",nrow(dat)," rows and ",length(unique(dat$CHRPOS))," CHR:POS pairs that are duplicated"))
	
	print("read in allele frequency file and extract matching duplicate ids")
	tbl<-read.table(args$freq_in,header=T,as.is=T,stringsAsFactors=F)
	tbl<-tbl[,c("SNP","MAF")]
	tbl<-tbl[tbl$SNP %in% dat$SNP,]
	print(paste0("frequency file contains ",nrow(tbl)," rows matching data"))
	
	print("merge allele frequency into duplicates")
	dat<-merge(dat,tbl,all.x=T)
	print(paste0("merged data contains ",nrow(dat)," rows"))
	
	print("read in callrate file and extract matching duplicate ids")
	tbl<-read.table(args$miss_in,header=T,as.is=T,stringsAsFactors=F)
	tbl<-tbl[,c("SNP","F_MISS")]
	tbl<-tbl[tbl$SNP %in% dat$SNP,]
	print(paste0("callrate file contains ",nrow(tbl)," values matching duplicate bim file variants"))
	
	print("merge callrate into duplicates")
	dat<-merge(dat,tbl,all.x=T)
	print(paste0("merged data contains ",nrow(dat)," rows"))
	
	if(nrow(dat) > 0) {
		complement <- function(x) {
			comp<-c()
			for(d in unlist(strsplit(toupper(x),split=""))) {
				if(d == "A") comp <- c(comp,"T")
				if(d == "T") comp <- c(comp,"A")
				if(d == "G") comp <- c(comp,"C")
				if(d == "C") comp <- c(comp,"G")
				if(d == "0") comp <- c(comp,"0")
				if(d == "-") comp <- c(comp,"-")
			}
			return(paste(comp,collapse=""))
		}
		print("convert alleles to uppercase and define all allele combinations")
		dat$A1<-toupper(dat$A1)
		dat$A2<-toupper(dat$A2)
		dat$ALLELES<-paste(dat$A1,dat$A2,sep="")
		dat$ALLELES_R<-paste(dat$A2,dat$A1,sep="")
		dat$ALLELES_C<-NA
		dat$ALLELES_RC<-NA
		dat$ALLELES_MONOA1<-paste(dat$A1,"0",sep="")
		dat$ALLELES_MONOA2<-paste("0",dat$A2,sep="")
		dat$ALLELES_MONOCA1<-NA
		dat$ALLELES_MONOCA2<-NA
	
		print("calculate complements of alleles")
		dat$ALLELES_C<-lapply(dat$ALLELES, FUN=complement)
		dat$ALLELES_RC<-lapply(dat$ALLELES_R, FUN=complement)
		dat$ALLELES_MONOCA1<-paste0(lapply(dat$A1, FUN=complement),"0")
		dat$ALLELES_MONOCA2<-paste0("0",lapply(dat$A2, FUN=complement))
	
		print("identify duplicates to remove from each CHR:POS pair")
		dat$non_unique<-0
		dat$ref<-NA
		dat$flip<-0
		i<-0
		for(pos in unique(dat$CHRPOS[dat$CHRPOS %in% dups])) {
			i<-i+1
			print(paste(i," of ",length(unique(dat$CHRPOS[dat$CHRPOS %in% dups])),sep=""))
			for(snp in dat$SNP[dat$CHRPOS == pos]) {
				if(
					dat$ALLELES[dat$SNP == snp] %in% dat$ALLELES[dat$CHRPOS == pos & dat$SNP != snp] || 
					dat$ALLELES[dat$SNP == snp] %in% dat$ALLELES_R[dat$CHRPOS == pos & dat$SNP != snp] || 
					dat$ALLELES[dat$SNP == snp] %in% dat$ALLELES_C[dat$CHRPOS == pos & dat$SNP != snp] || 
					dat$ALLELES[dat$SNP == snp] %in% dat$ALLELES_RC[dat$CHRPOS == pos & dat$SNP != snp] || 
					dat$ALLELES_MONOA1[dat$SNP == snp] %in% dat$ALLELES_MONOA1[dat$CHRPOS == pos & dat$SNP != snp] || 
					dat$ALLELES_MONOA2[dat$SNP == snp] %in% dat$ALLELES_MONOA2[dat$CHRPOS == pos & dat$SNP != snp] || 
					dat$ALLELES_MONOA1[dat$SNP == snp] %in% dat$ALLELES_MONOCA1[dat$CHRPOS == pos & dat$SNP != snp] || 
					dat$ALLELES_MONOA2[dat$SNP == snp] %in% dat$ALLELES_MONOCA2[dat$CHRPOS == pos & dat$SNP != snp]
				) {
					dat$non_unique[dat$SNP == snp]<-1
				}
			}
			if(nrow(dat[dat$CHRPOS == pos & dat$non_unique == 1,]) > 0) {
				maf_max<-max(dat$MAF[dat$CHRPOS == pos & dat$non_unique == 1])
				maf_min<-min(dat$MAF[dat$CHRPOS == pos & dat$non_unique == 1])
				maf_diff<-maf_max-maf_min
				miss_max<-max(dat$F_MISS[dat$CHRPOS == pos & dat$non_unique == 1])
				miss_min<-min(dat$F_MISS[dat$CHRPOS == pos & dat$non_unique == 1])
				miss_diff<-miss_max-miss_min
				highcall_vars<-dat$SNP[dat$CHRPOS == pos & dat$non_unique == 1 & dat$F_MISS <= 0.02]
				if(length(highcall_vars) <= 1) {
					min_miss_var<-dat$SNP[dat$CHRPOS == pos & dat$non_unique == 1 & dat$F_MISS == miss_min]
					if(length(min_miss_var) == 1) {
						dat$remove[dat$CHRPOS == pos & dat$non_unique == 1 & dat$SNP != min_miss_var]<-1
					} else {
						dat$remove[dat$CHRPOS == pos & dat$non_unique == 1 & dat$SNP != min_miss_var[1]]<-1
					}
				} else {
					max_maf_var<-dat$SNP[dat$SNP %in% highcall_vars & dat$MAF == maf_max]
					if(length(max_maf_var) == 1) {
						dat$remove[dat$SNP %in% highcall_vars & dat$SNP != max_maf_var]<-1
					} else {
						dat$remove[dat$SNP %in% highcall_vars & dat$SNP != max_maf_var[1]]<-1
					}
				}
			} else {
				ref<-names(sort(table(c(dat$A1[dat$CHRPOS == pos],dat$A2[dat$CHRPOS == pos])),decreasing=T)[1])[1]
				dat$ref[dat$CHRPOS == pos]<-ref
				dat$flip[dat$CHRPOS == pos & dat$A2 == ref & dat$A1 != "-" & dat$A2 != "-"]<-1
			}
		}
		write.table(dat$SNP[dat$remove == 1],args$out,row.names=F,col.names=F,sep="\t",quote=F,append=F)
	} else {
		cat("",file=args$out,append=F)
	}
} else {
	cat("",file=args$out,append=F)
}
