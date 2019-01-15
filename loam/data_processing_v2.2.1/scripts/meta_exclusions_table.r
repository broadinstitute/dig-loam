library(argparse)

parser <- ArgumentParser()
parser$add_argument("--meta-exclusions", dest="meta_exclusions", type="character", help="a comma separated list of metas, cohorts, and exclusions files each separated by 3 underscores")
parser$add_argument("--meta-cohorts", dest="meta_cohorts", type="character", help="a comma separated list of meta and + separated cohorts each separated by 3 underscores")
parser$add_argument("--out", dest="out", type="character", help="an output filename")
args<-parser$parse_args()

print(args)
