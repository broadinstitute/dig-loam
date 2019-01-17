library(argparse)

parser <- ArgumentParser()
parser$add_argument("--results", dest="results", type="character", help="a results file")
parser$add_argument("--known-loci", dest="known_loci", help='a known gwas loci results file annotated with gene')
parser$add_argument("--known-ld", dest="known_ld", help='an ld pair file')
parser$add_argument("--known-loci-n", dest="known_loci_n", help='sample size for known gwas loci results file')
parser$add_argument("--known-loci-case", dest="known_loci_case", help='number of cases for known gwas loci results file')
parser$add_argument("--known-loci-ctrl", dest="known_loci_ctrl", help='number of controls for known gwas loci results file')
parser$add_argument("--test", dest="test", type="character", help="a statistical test")
parser$add_argument("--out", dest="out", type="character", help="an output filename")
args<-parser$parse_args()

print(args)

complement <- function(a) {
	if(a != "NA") {
		s <- unlist(strsplit(a,""))
		comp <-c()
		for(l in s) {
			if(l == 'T') { 
				c <- 'A'
			} else if(l == 'A') {
				c <- 'T'
			} else if(l == 'G') {
				c <- 'C'
			} else if(l == 'C') {
				c <- 'G'
			} else if(l == '0') {
				c <- '0'
			} else if(l == ',') {
				c <- ','
			} else if(l == 'NA') {
				c <- 'NA'
			} else if(l == '-') {
				c <- '-'
			} else if(l == 'I') {
				c <- 'D'
			} else if(l == 'D') {
				c <- 'I'
			} else if(l %in% c('1','2','3','4','5','6','7','8','9','0')) {
				c <- l
			} else {
				c <- 'X'
			}
			comp <- c(comp,c)
		}
	} else {
		comp <- c('NA')
	}
	paste(comp,collapse="")
}

x<-read.table(args$results,header=T,as.is=T,sep="\t",comment.char="")
names(x)[1]<-gsub("X.","",names(x)[1])
cols_keep<-c("chr","pos","id","ref","alt","n")
if(args$test %in% c("wald","lrt","firth")) {
	cols_keep<-c(cols_keep,"case","ctrl")
}
if("af" %in% names(x)) {
	cols_keep<-c(cols_keep,"af")
}
if("afavg" %in% names(x)) {
	cols_keep<-c(cols_keep,"afavg")
}
if("afmin" %in% names(x)) {
	cols_keep<-c(cols_keep,"afmin")
}
if("afmax" %in% names(x)) {
	cols_keep<-c(cols_keep,"afmax")
}
if(args$test %in% c("wald","lrt","firth")) {
	x$or <- exp(x$beta)
	cols_keep<-c(cols_keep,"or")
} else {
	cols_keep<-c(cols_keep,"beta","se")
}
cols_keep<-c(cols_keep,"pval")
if("dir" %in% names(x)) {
	cols_keep<-c(cols_keep,"dir")
}
if("cohort" %in% names(x)) {
	cols_keep<-c(cols_keep,"cohort")
}
x<-x[,cols_keep]
x<-x[! is.na(x['pval']),]

for(i in 1:nrow(x)) {
	if("beta" %in% names(x)) {
		if(x$beta[i] < 0) {
			ref<-x$ref[i]
			alt<-x$alt[i]
			x$ref[i]<-alt
			x$alt[i]<-ref
			x$beta[i] <- -1 * x$beta[i]
			if("dir" %in% names(x)) { x$dir[i] = gsub("b","\\+",gsub("a","-",gsub("-","b",gsub("\\+","a",x$dir[i])))) }
			if("af" %in% names(x)) { x$af = 1 - x$af }
		}	
	} else {
		if(x$or[i] < 1) {
			ref<-x$ref[i]
			alt<-x$alt[i]
			x$ref[i]<-alt
			x$alt[i]<-ref
			x$or[i] <- 1 / x$or[i]
			if("dir" %in% names(x)) { x$dir[i] = gsub("b","\\+",gsub("a","-",gsub("-","b",gsub("\\+","a",x$dir[i])))) }
			if("af" %in% names(x)) { x$af = 1 - x$af }
		}
	}
}

known <- read.table(args$known_loci, header=T, as.is=T, stringsAsFactors=F)
known$chr<-NULL
known$pos<-NULL
names(known)[names(known) == "effect_allele"]<-"alt"
names(known)[names(known) == "other_allele"]<-"ref"
names(known)[names(known) == "effect"]<-"beta"
names(known)[names(known) == "stderr"]<-"se"
names(known)[names(known) == "p"]<-"pval"
for(c in names(known)) {
	if(! c %in% c("CLOSEST_GENE")) {
		names(known)[names(known) == c]<-paste(names(known)[names(known) == c],"known",sep="_")
	}
}

ld <- read.table(args$known_ld, header=T, as.is=T, stringsAsFactors=F)
known$id <- NA
known$ident <- 0
known$r2 <- NA
for(i in 1:nrow(known)) {
	if(known$id_known[i] %in% x$id) {
		known$id[i] <- known$id_known[i]
		known$ident[i] <- 1
		known$r2[i] <- 1
	} else {
		ld_temp <- ld[ld$SNP_A == known$id_known[i],]
		ld_temp <- ld_temp[ld_temp$SNP_B %in% x$id,]
		if(nrow(ld_temp) > 0) {
			ld_temp <- ld_temp[order(-ld_temp$R2),]
			known$id[i] <- ld_temp$SNP_B[1]
			known$r2[i] <- ld_temp$R2[1]
		}
	}
}
known <- known[! is.na(known$id),]
known$ref_known <- toupper(known$ref_known)
known$alt_known <- toupper(known$alt_known)

x<-merge(x,known,all=F)

x$status<-"remove"
for(i in 1:nrow(x)) {
	alleles <- paste(x$ref[i], x$alt[i], sep="")
	alleles_comp <- complement(paste(x$ref[i], x$alt[i], sep=""))
	alleles_rev <- paste(x$alt[i], x$ref[i], sep="")
	alleles_rev_comp <- complement(paste(x$alt[i], x$ref[i], sep=""))
	known_alleles <- paste(x$ref_known[i], x$alt_known[i], sep="")
	if(known_alleles == alleles) x$status[i] <- "match"
	if(known_alleles == alleles_comp) x$status[i] <- "comp"
	if(known_alleles == alleles_rev) x$status[i] <- "rev"
	if(known_alleles == alleles_rev_comp) x$status[i] <- "rev_comp"
}

x <- x[x$status != "remove",]

for(i in 1:nrow(x)) {
	if(x$status[i] == "rev") {
		ref <- x$ref_known[i]
		alt <- x$alt_known[i]
		x$ref_known[i] <- alt
		x$alt_known[i] <- ref
		if("beta_known" %in% names(x)) x$beta_known[i] <- -1 * x$beta_known[i]
		if("or_known" %in% names(x)) x$or_known[i] <- 1 / x$or_known[i]
	} else if(x$status[i] == "comp") {
		x$ref_known[i] <- complement(x$ref_known[i])
		x$alt_known[i] <- complement(x$alt_known[i])
	} else if(x$status[i] == "rev_comp") {
		ref <- x$ref_known[i]
		alt <- x$alt_known[i]
		x$ref_known[i] <- alt
		x$alt_known[i] <- ref
		x$ref_known[i] <- complement(x$ref_known[i])
		x$alt_known[i] <- complement(x$alt_known[i])
		if("beta_known" %in% names(x)) x$beta_known[i] <- -1 * x$beta_known[i]
		if("or_known" %in% names(x)) x$or_known[i] <- 1 / x$or_known[i]
	}
}

x <- x[order(-x$ident, -x$r2, x$pval_known),]
x <- x[! duplicated(x$CLOSEST_GENE),]

x$status<-NULL
x$ref_known<-NULL
x$alt_known<-NULL
x$ident<-NULL

cols_out <- c(cols_keep,"CLOSEST_GENE","r2")
cols_out_post <- names(x)[! names(x) %in% c(cols_keep,"CLOSEST_GENE","r2")]
if(args$known_loci_n != "") {
	x$n_known <- args$known_loci_n
	cols_out <- c(cols_out, "n_known")
}
if(args$known_loci_case != "") {
	x$case_known <- args$known_loci_case
	cols_out <- c(cols_out, "case_known")
}
if(args$known_loci_ctrl != "") {
	x$ctrl_known <- args$known_loci_ctrl
	cols_out <- c(cols_out, "ctrl_known")
}
cols_out <- c(cols_out, cols_out_post)

x <- x[,cols_out]
names(x)[names(x) == "CLOSEST_GENE"] <- "gene"

x <- head(x, n=50)

for(i in 1:nrow(x)) {
	if(args$test %in% c("wald","lrt","firth")) {
		if((x$or[i] <= 1 & x$or_known[i] <= 1) || (x$or[i] >= 1 & x$or_known[i] >= 1)){
			x$id[i]<-paste("\\large{\\textbf{",x$id[i],"}}",sep="")
		}
	} else {
		if(sign(x$beta[i]) == 0 || sign(x$beta[i]) == 0 || (sign(x$beta[i]) == sign(x$beta_known[i]))) x$id[i]<-paste("\\large{\\textbf{",x$id[i],"}}",sep="")
	}
}

# replace _ with \_ to make compatible with pgfplotstabletypeset
x <- data.frame(lapply(x, FUN=function(z) gsub("_","\\\\_",z)))

# replace any columns with all missing values
x <- Filter(function(a) !(all(is.na(a))), x)

write.table(x, args$out, row.names=F, col.names=T, quote=F, append=F, sep="\t")
