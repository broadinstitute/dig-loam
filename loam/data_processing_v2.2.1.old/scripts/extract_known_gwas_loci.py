import pandas as pd
import numpy as np
import argparse
from biomart import BiomartServer
import itertools

def group(list, count):
    for i in range(0, len(list), count):
		yield list[i:i+count]

def main(args=None):

	known = pd.read_table(args.known)

	cols_keep = [args.var_id, args.effect_allele, args.other_allele]
	cols_rename = {args.var_id: 'id', args.effect_allele: 'effect_allele', args.other_allele: 'other_allele'}
	if args.odds_ratio:
		cols_keep = cols_keep + [args.odds_ratio]
		cols_rename[args.odds_ratio] = 'or'
	if args.effect:
		cols_keep = cols_keep + [args.effect]
		cols_rename[args.effect] = 'effect'
	if args.stderr:
		cols_keep = cols_keep + [args.stderr]
		cols_rename[args.stderr] = 'stderr'
	cols_keep = cols_keep + [args.pval]
	cols_rename[args.pval] = 'p'
	known = known[cols_keep]
	known.rename(columns = cols_rename, inplace = True)

	known = known[known['p'] < 5.4e-8]

	server = BiomartServer(args.biomart_server)
	server.verbose = True
	hsap = server.datasets['hsapiens_snp']
	if known.shape[0] > 100:
		n = 100
	elif known.shape[0] > 10:
		n = 10
	else:
		n = 1
	groups = group(known['id'].tolist(), n)
	biomart_results = []
	for g in groups:
		response = hsap.search({ 'filters': { 'snp_filter': g }, 'attributes': ['chr_name','chrom_start','refsnp_id'] })
		biomart_results = biomart_results + [r for r in response.iter_lines()]
	df = pd.DataFrame([l.split("\t") for l in biomart_results])
	df.columns = ['chr','pos','id']
	df = df.astype(dtype = {"chr": "int64", "pos": "int64"})

	df = df.merge(known)
	print df.dtypes
	df.sort_values(by=['chr','pos'], inplace=True)
	df.to_csv(args.out, header=True, index=False, sep="\t")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--odds-ratio', help='a column name for odds ratio')
	parser.add_argument('--effect', help='a column name for effect size (beta)')
	parser.add_argument('--stderr', help='a column name for standard error')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--biomart-server', help='a url for biomart server to use', required=True)
	requiredArgs.add_argument('--known', help='a known loci file', required=True)
	requiredArgs.add_argument('--var-id', help='a column name for variant ID (rsID)', required=True)
	requiredArgs.add_argument('--effect-allele', help='a column name for effect allele', required=True)
	requiredArgs.add_argument('--other-allele', help='a column name for other allele', required=True)
	requiredArgs.add_argument('--pval', help='a column name for p-value', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
