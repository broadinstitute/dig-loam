library(argparse)

parser <- ArgumentParser()
parser$add_argument("--bim-in", dest="bim_in", type="character", help="Plink bim file")
parser$add_argument("--freq-in", dest="freq_in", type="character", help="Plink frq file")
parser$add_argument("--miss-in", dest="miss_in", type="character", help="Plink lmiss file")
parser$add_argument("--out", dest="out", type="character", help="an output file name")
args<-parser$parse_args()

print(args)

dat<-read.table(args$bim_in,header=F,as.is=T,stringsAsFactors=F)
freq<-read.table(args$freq_in,header=T,as.is=T,stringsAsFactors=F)
miss<-read.table(args$miss_in,header=T,as.is=T,stringsAsFactors=F)
freq<-freq[,c("SNP","MAF")]
miss<-miss[,c("SNP","F_MISS")]
names(dat)[2]<-"SNP"
dat<-merge(dat,freq,all.x=T)
dat<-merge(dat,miss,all.x=T)
dat$chrpos<-paste(dat$V1,":",dat$V4,sep="")
dups<-dat$chrpos[duplicated(dat$chrpos)]
dat$remove<-0
dat$check<-0
dat_nondups<-dat[! dat$chrpos %in% dups,]
dat_dups<-dat[dat$chrpos %in% dups,]
if(nrow(dat_dups) > 0) {
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
	names(dat_dups)[2]<-"CHR"
	names(dat_dups)[4]<-"POS"
	names(dat_dups)[5]<-"A1"
	names(dat_dups)[6]<-"A2"
	dat_dups$A1<-toupper(dat_dups$A1)
	dat_dups$A2<-toupper(dat_dups$A2)
	dat_dups$ALLELES<-paste(dat_dups$A1,dat_dups$A2,sep="")
	dat_dups$ALLELES_R<-paste(dat_dups$A2,dat_dups$A1,sep="")
	dat_dups$ALLELES_C<-NA
	dat_dups$ALLELES_RC<-NA
	dat_dups$ALLELES_MONOA1<-paste(dat_dups$A1,"0",sep="")
	dat_dups$ALLELES_MONOA2<-paste(dat_dups$A2,"0",sep="")
	dat_dups$ALLELES_MONORA1<-paste("0",dat_dups$A1,sep="")
	dat_dups$ALLELES_MONORA2<-paste("0",dat_dups$A2,sep="")
	dat_dups$ALLELES_MONOCA1<-NA
	dat_dups$ALLELES_MONOCA2<-NA
	dat_dups$ALLELES_MONORCA1<-NA
	dat_dups$ALLELES_MONORCA2<-NA
	print("calculating complements")
	dat_dups$ALLELES_C<-sapply(dat_dups$ALLELES, complement)
	print("calculating reverse complements")
	dat_dups$ALLELES_RC<-sapply(dat_dups$ALLELES_R, complement)
	print("calculating monomorphic A1 complements")
	dat_dups$ALLELES_MONOCA1<-sapply(dat_dups$A1, function(x) paste0(complement(x),"0"))
	print("calculating monomorphic A2 complements")
	dat_dups$ALLELES_MONOCA2<-sapply(dat_dups$A2, function(x) paste0(complement(x),"0"))
	print("calculating monomorphic A1 reverse complements")
	dat_dups$ALLELES_MONORCA1<-sapply(dat_dups$A1, function(x) paste0("0",complement(x)))
	print("calculating monomorphic A2 reverse complements")
	dat_dups$ALLELES_MONORCA2<-sapply(dat_dups$A2, function(x) paste0("0",complement(x)))
	dat_dups$uid<-apply(dat_dups[,c("CHR","POS","ALLELES","ALLELES_R","ALLELES_C","ALLELES_RC","ALLELES_MONOA1","ALLELES_MONOA2","ALLELES_MONORA1","ALLELES_MONORA2","ALLELES_MONOCA1","ALLELES_MONOCA2","ALLELES_MONORCA1","ALLELES_MONORCA2")], 1, function(x) paste(c(x[1],x[2],sort(x[3:length(x)])), collapse=":"))
	print(dat_dups)
	dat_dups_dups<-dat_dups$uid[duplicated(dat_dups$uid)]
	dat_dups<-dat_dups[dat_dups$uid %in% dat_dups_dups,]
	dat_dups$non_unique<-0
	dat_dups$ref<-NA
	dat_dups$flip<-0
	i<-0
	chrpos_unique<-unique(dat_dups$chrpos[dat_dups$chrpos %in% dups])
	chrpos_unique_length<-length(chrpos_unique)
	for(pos in chrpos_unique) {
		i<-i+1
		print(paste(i," of ",chrpos_unique_length,sep=""))
		for(snp in dat_dups$SNP[dat_dups$chrpos == pos]) {
			if(
				dat_dups$ALLELES[dat_dups$SNP == snp] %in% dat_dups$ALLELES[dat_dups$chrpos == pos & dat_dups$SNP != snp] || 
				dat_dups$ALLELES[dat_dups$SNP == snp] %in% dat_dups$ALLELES_R[dat_dups$chrpos == pos & dat_dups$SNP != snp] || 
				dat_dups$ALLELES[dat_dups$SNP == snp] %in% dat_dups$ALLELES_C[dat_dups$chrpos == pos & dat_dups$SNP != snp] || 
				dat_dups$ALLELES[dat_dups$SNP == snp] %in% dat_dups$ALLELES_RC[dat_dups$chrpos == pos & dat_dups$SNP != snp] || 
				dat_dups$ALLELES_MONOA1[dat_dups$SNP == snp] %in% dat_dups$ALLELES_MONOA1[dat_dups$chrpos == pos & dat_dups$SNP != snp] || 
				dat_dups$ALLELES_MONOA2[dat_dups$SNP == snp] %in% dat_dups$ALLELES_MONOA2[dat_dups$chrpos == pos & dat_dups$SNP != snp] || 
				dat_dups$ALLELES_MONOA1[dat_dups$SNP == snp] %in% dat_dups$ALLELES_MONOCA1[dat_dups$chrpos == pos & dat_dups$SNP != snp] || 
				dat_dups$ALLELES_MONOA2[dat_dups$SNP == snp] %in% dat_dups$ALLELES_MONOCA2[dat_dups$chrpos == pos & dat_dups$SNP != snp]
			) {
				dat_dups$non_unique[dat_dups$SNP == snp]<-1
			}
		}
		if(nrow(dat_dups[dat_dups$chrpos == pos & dat_dups$non_unique == 1,]) > 0) {
			maf_max<-max(dat_dups$MAF[dat_dups$chrpos == pos & dat_dups$non_unique == 1])
			maf_min<-min(dat_dups$MAF[dat_dups$chrpos == pos & dat_dups$non_unique == 1])
			maf_diff<-maf_max-maf_min
			miss_max<-max(dat_dups$F_MISS[dat_dups$chrpos == pos & dat_dups$non_unique == 1])
			miss_min<-min(dat_dups$F_MISS[dat_dups$chrpos == pos & dat_dups$non_unique == 1])
			miss_diff<-miss_max-miss_min
			highcall_vars<-dat_dups$SNP[dat_dups$chrpos == pos & dat_dups$non_unique == 1 & dat_dups$F_MISS <= 0.02]
			if(length(highcall_vars) <= 1) {
				min_miss_var<-dat_dups$SNP[dat_dups$chrpos == pos & dat_dups$non_unique == 1 & dat_dups$F_MISS == miss_min]
				if(length(min_miss_var) == 1) {
					dat_dups$remove[dat_dups$chrpos == pos & dat_dups$non_unique == 1 & dat_dups$SNP != min_miss_var]<-1
				} else {
					dat_dups$remove[dat_dups$chrpos == pos & dat_dups$non_unique == 1 & dat_dups$SNP != min_miss_var[1]]<-1
				}
			} else {
				max_maf_var<-dat_dups$SNP[dat_dups$SNP %in% highcall_vars & dat_dups$MAF == maf_max]
				if(length(max_maf_var) == 1) {
					dat_dups$remove[dat_dups$SNP %in% highcall_vars & dat_dups$SNP != max_maf_var]<-1
				} else {
					dat_dups$remove[dat_dups$SNP %in% highcall_vars & dat_dups$SNP != max_maf_var[1]]<-1
				}
			}
		} else {
			ref<-names(sort(table(c(dat_dups$A1[dat_dups$chrpos == pos],dat_dups$A2[dat_dups$chrpos == pos])),decreasing=T)[1])[1]
			dat_dups$ref[dat_dups$chrpos == pos]<-ref
			dat_dups$flip[dat_dups$chrpos == pos & dat_dups$A2 == ref & dat_dups$A1 != "-" & dat_dups$A2 != "-"]<-1
		}	
	}
	write.table(dat_dups$SNP[dat_dups$remove == 1],args$out,row.names=F,col.names=F,sep="\t",quote=F,append=F)
} else {
	cat("",file=args$out,append=F)
}
