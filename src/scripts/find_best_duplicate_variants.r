library(argparse)

parser <- ArgumentParser()
parser$add_argument("--bim-in", dest="bim_in", type="character", help="Plink bim file")
parser$add_argument("--freq-in", dest="freq_in", type="character", help="Plink frq file")
parser$add_argument("--miss-in", dest="miss_in", type="character", help="Plink lmiss file")
parser$add_argument("--out", dest="out", type="character", help="an output file name")
args<-parser$parse_args()

print(args)

print("read in bim file")
dat<-read.table(args$bim_in,header=F,as.is=T,stringsAsFactors=F)
dat$chrpos<-paste(dat$V1,":",dat$V4,sep="")
names(dat)[2]<-"SNP"
dups<-dat$chrpos[duplicated(dat$chrpos)]
print("extract duplicate chr:pos pairs")
dat<-dat[dat$chrpos %in% dups,]
dat$remove<-0
dat$check<-0

print("read in allele frequency file")
tbl<-read.table(args$freq_in,header=T,as.is=T,stringsAsFactors=F)
tbl<-tbl[,c("SNP","MAF")]

print("merge allele frequency into duplicates")
dat<-merge(dat,tbl,all.x=T)

print("read in callrate file")
tbl<-read.table(args$miss_in,header=T,as.is=T,stringsAsFactors=F)
tbl<-tbl[,c("SNP","F_MISS")]

print("merge callrate into duplicates")
dat<-merge(dat,tbl,all.x=T)

if(nrow(dat) > 0) {
	compliment <- function(x) {
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
	names(dat)[2]<-"CHR"
	names(dat)[4]<-"POS"
	names(dat)[5]<-"A1"
	names(dat)[6]<-"A2"
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
	for(i in 1:nrow(dat)) {
		print(paste("   ... dup ",i," of ",nrow(dat),sep=""))
		dat$ALLELES_C[i]<-compliment(dat$ALLELES[i])
		dat$ALLELES_RC[i]<-compliment(dat$ALLELES_R[i])
		dat$ALLELES_MONOCA1[i]<-paste(compliment(dat$A1[i]),"0",sep="")
		dat$ALLELES_MONOCA2[i]<-paste("0",compliment(dat$A2[i]),sep="")
	}
	dat$non_unique<-0
	dat$ref<-NA
	dat$flip<-0
	i<-0
	for(pos in unique(dat$chrpos[dat$chrpos %in% dups])) {
		i<-i+1
		print(paste(i," of ",length(unique(dat$chrpos[dat$chrpos %in% dups])),sep=""))
		for(snp in dat$SNP[dat$chrpos == pos]) {
			if(dat$ALLELES[dat$SNP == snp] %in% dat$ALLELES[dat$chrpos == pos & dat$SNP != snp] || dat$ALLELES[dat$SNP == snp] %in% dat$ALLELES_R[dat$chrpos == pos & dat$SNP != snp] || dat$ALLELES[dat$SNP == snp] %in% dat$ALLELES_C[dat$chrpos == pos & dat$SNP != snp] || dat$ALLELES[dat$SNP == snp] %in% dat$ALLELES_RC[dat$chrpos == pos & dat$SNP != snp] || dat$ALLELES_MONOA1[dat$SNP == snp] %in% dat$ALLELES_MONOA1[dat$chrpos == pos & dat$SNP != snp] || dat$ALLELES_MONOA2[dat$SNP == snp] %in% dat$ALLELES_MONOA2[dat$chrpos == pos & dat$SNP != snp] || dat$ALLELES_MONOA1[dat$SNP == snp] %in% dat$ALLELES_MONOCA1[dat$chrpos == pos & dat$SNP != snp] || dat$ALLELES_MONOA2[dat$SNP == snp] %in% dat$ALLELES_MONOCA2[dat$chrpos == pos & dat$SNP != snp]) {
				dat$non_unique[dat$SNP == snp]<-1
			}
		}
		if(nrow(dat[dat$chrpos == pos & dat$non_unique == 1,]) > 0) {
			maf_max<-max(dat$MAF[dat$chrpos == pos & dat$non_unique == 1])
			maf_min<-min(dat$MAF[dat$chrpos == pos & dat$non_unique == 1])
			maf_diff<-maf_max-maf_min
			miss_max<-max(dat$F_MISS[dat$chrpos == pos & dat$non_unique == 1])
			miss_min<-min(dat$F_MISS[dat$chrpos == pos & dat$non_unique == 1])
			miss_diff<-miss_max-miss_min
			highcall_vars<-dat$SNP[dat$chrpos == pos & dat$non_unique == 1 & dat$F_MISS <= 0.02]
			if(length(highcall_vars) <= 1) {
				min_miss_var<-dat$SNP[dat$chrpos == pos & dat$non_unique == 1 & dat$F_MISS == miss_min]
				if(length(min_miss_var) == 1) {
					dat$remove[dat$chrpos == pos & dat$non_unique == 1 & dat$SNP != min_miss_var]<-1
				} else {
					dat$remove[dat$chrpos == pos & dat$non_unique == 1 & dat$SNP != min_miss_var[1]]<-1
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
			ref<-names(sort(table(c(dat$A1[dat$chrpos == pos],dat$A2[dat$chrpos == pos])),decreasing=T)[1])[1]
			dat$ref[dat$chrpos == pos]<-ref
			dat$flip[dat$chrpos == pos & dat$A2 == ref & dat$A1 != "-" & dat$A2 != "-"]<-1
		}
	}
	write.table(dat$SNP[dat$remove == 1],args$out,row.names=F,col.names=F,sep="\t",quote=F,append=F)
} else {
	cat("",file=args$out,append=F)
}
