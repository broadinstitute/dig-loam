import pandas as pd
import numpy as np
import argparse
from biomart import BiomartServer
import itertools
import csv
import sys
csv.field_size_limit(sys.maxsize)

def group(list, count):
	for i in range(0, len(list), count):
		yield list[i:i+count]

def main(args=None):

	print("reading results from file")
	df=pd.read_table(args.results, low_memory=False)

	df.dropna(subset=[args.p], inplace=True)
	df.reset_index(drop=True, inplace=True)

	print("sorting by p value")
	df.sort_values(by=[args.p],inplace=True)

	print("extracting top " + str(args.n) + " variants")
	df = df.head(n=args.n)

	df['chromosomal_region'] = df['marker'].apply(lambda x: x.split("_")[0] + ':' + x.split("_")[1] + ':' + x.split("_")[1] + ':1')

	server = BiomartServer("http://grch37.ensembl.org/biomart")
	server.verbose = True
	hsap = server.datasets['hsapiens_snp']
	if df.shape[0] > 100:
		n = 100
	elif df.shape[0] > 10:
		n = 10
	else:
		n = 1
	groups = group(df['chromosomal_region'].tolist(), n)
	biomart_results = []
for g in df['chromosomal_region']:
	print(g)
	response = hsap.search({ 'filters': { 'chromosomal_region': g }, 'attributes': ['chr_name','chrom_start','refsnp_id','allele','allele_1'] })
	biomart_results = biomart_results + [r for r in response.iter_lines()]
	bm = pd.DataFrame([l.split("\t") for l in biomart_results])
	bm.columns = ['chr','pos','id']
	bm['chr_int'] = bm['chr'].map({v:str(i+1) for i,v in enumerate([str(i) for i in range(1,23)] + ['X','Y','MT'])})
	bm.dropna(subset=['chr_int'], inplace=True)
	bm.reset_index(drop=True, inplace=True)
	bm = bm.astype(dtype = {"chr_int": "int64", "pos": "int64"})
	bm = bm.merge(df)
	df.sort_values(by=['chr_int','pos'], inplace=True)
	df.drop(columns=['chr_int'], inplace=True)

	print("resorting by p value")
	df.sort_values(by=[args.p],inplace=True)

	df.to_csv(args.out, header=True, index=False, sep="\t", na_rep="NA")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--n', type=int, default=1000, help='number of top variants to report')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
