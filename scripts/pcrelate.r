library(argparse)

parser <- ArgumentParser()
parser$add_argument("--gds-in", dest="gds_in", type="character", help="Bioconductor gds file name", required=TRUE)
parser$add_argument("--rdata-in", dest="rdata_in", type="character", help="RData file containing pcair results", required=TRUE)
parser$add_argument("--ibd-out", dest="ibd_out", type="character", help="An output filename for pcrelate based ibd results", required=TRUE)
args<-parser$parse_args()

library(GENESIS)
library(SNPRelate)
library(GWASTools)
library(gdsfmt)

print("extracting pruned list of snps")
genofile <- snpgdsOpen(args$gds_in)
set.seed(1000)
snpset <- snpgdsLDpruning(genofile, sample.id = NULL, snp.id = NULL, autosome.only = TRUE, remove.monosnp = TRUE, maf = NaN, missing.rate = NaN, method = "corr", slide.max.bp = 500000, slide.max.n = NA, ld.threshold = 0.2, num.thread = 1, verbose = TRUE)
snps_include <- unlist(snpset)
snpgdsClose(genofile)

print("read in GDS file")
geno <- GdsGenotypeReader(filename = args$gds_in)
genoData <- GenotypeData(geno)

print("read in rdata file from previous pcair run")
load(args$rdata_in)

print("running pcrelate")
pcrel <- pcrelate(genoData = genoData, pcMat = mypcair$vectors[,1:3], training.set = mypcair$unrels, snp.include=snps_include)

kinship<-pcrelateReadKinship(pcrel, scan.include = NULL, ibd.probs = TRUE,  kin.thresh = NULL)

print("writing pcrelate results to file")
write.table(kinship,args$ibd_out,row.names=F,col.names=T,quote=F,sep="\t",append=F)
