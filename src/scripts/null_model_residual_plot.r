library(argparse)
library(ggplot2)

set.seed(1)

parser <- ArgumentParser()
parser$add_argument("--pheno-in", dest="pheno_in", type="character", help="a prepared phenotype file")
parser$add_argument("--pheno-col", dest="pheno_col", type="character", help="a column name for phenotype")
parser$add_argument("--covars", dest="covars", type="character", help="a '+' separated list of covariates")
parser$add_argument("--pcs-include", dest="pcs_include", type="character", help="a pc inclusion filename")
parser$add_argument("--out", dest="out", type="character", help="an output filename base (ie no extension)")
args<-parser$parse_args()

print(args)

cat("removing factor indicators from covariates\n")
covars <- gsub("\\]","",gsub("\\[","",unlist(strsplit(args$covars,split="\\+"))))

cat("read in prepared phenotype file\n")
pheno<-read.table(args$pheno_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")

pcs<-scan(args$pcs_include,what="character")

covars_factors <- unlist(strsplit(args$covars,split="\\+"))
for(cv in covars_factors) {
	cvv <- unlist(strsplit(cv,split=""))
	if(cvv[1] == "[" && cvv[length(cvv)] == "]") {
		cvb<-paste(cvv[2:(length(cvv)-1)],collapse="")
		if(length(unique(pheno[,cvb])) == 1) {
			cat(paste0("covariate ",cvb," has zero variance\n"))
			failed <- TRUE
		} else {
			for(val in sort(unique(pheno[,cvb]))[2:length(sort(unique(pheno[,cvb])))]) {
				pheno[,paste0(cvb,val)] <- 0
				pheno[,paste0(cvb,val)][which(pheno[,cvb] == val)] <- 1
				covars_factors <- c(covars_factors,paste0(cvb,val))
			}
		}
		covars_factors <- covars_factors[covars_factors != cv]
	} else {
		if(length(unique(pheno[,cv])) == 1) {
			cat(paste0("covariate ",cv," has zero variance\n"))
			failed <- TRUE
		}
	}
}

covars_analysis<-paste(c(covars_factors,paste(pcs,collapse="+"),"1"),collapse="+")

cat("convert all model vars to numeric\n")
for(cv in c(args$pheno_col,unlist(strsplit(covars_analysis,split="\\+")))) {
	if(cv %in% names(pheno)) {
		pheno[,cv]<-as.numeric(as.character(pheno[,cv]))
	}
}

print(head(pheno))
print(covars_analysis)

model<-lm(as.formula(paste(args$pheno_col,"~",covars_analysis,sep="")),data=pheno)
res<-resid(model)

out<-paste0(args$out,".%03d.png")
png(out,width=7, height=7, units='in', res=300)
plot(model, ask=FALSE)
dev.off()
Sys.sleep(3)
file.copy(from=paste0(args$out,".001.png"), to=paste0(args$out,".res_vs_fit.png"))
file.copy(from=paste0(args$out,".002.png"), to=paste0(args$out,".qq.png"))
file.copy(from=paste0(args$out,".003.png"), to=paste0(args$out,".sqrtres_vs_fit.png"))
file.copy(from=paste0(args$out,".004.png"), to=paste0(args$out,".res_vs_lev.png"))
Sys.sleep(1)
file.remove(paste0(args$out,".001.png"))
file.remove(paste0(args$out,".002.png"))
file.remove(paste0(args$out,".003.png"))
file.remove(paste0(args$out,".004.png"))
