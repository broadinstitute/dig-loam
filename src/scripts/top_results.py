import pandas as pd
import numpy as np
import argparse

def main(args=None):

	print "reading results from file"
	df=pd.read_table(args.results, low_memory=False, compression="gzip")

	if args.exclude:
		print "reading in variant exclusions from file"
		excl=pd.read_table(args.exclude, header=None, low_memory=False)
		excl.columns=["locus","alleles"]
		excl_list=excl['locus'] + ':' + excl['alleles'].str.replace('\[|\]|\"','').str.replace(',',':')
		df['excl_id']=df['#chr'].map(str) + ':' + df['pos'].map(str) + ':' + df['ref'].map(str) + ':' + df['alt'].map(str)
		df=df[~ df['excl_id'].isin(excl_list)]
		df.drop(columns=['excl_id'], inplace=True)

	df.dropna(subset=[args.p], inplace=True)

	if len([col for col in df if col.startswith('case_') or col.startswith('ctrl_')]) > 0:
		print "removing {0:d} variants with mac < 20".format(df[df[args.mac] < 20].shape[0])
		df = df[df[args.mac] >= 20]
	else:
		print "removing {0:d} variants with mac < 3".format(df[df[args.mac] < 3].shape[0])
		df = df[df[args.mac] >= 3]

	df.reset_index(drop=True, inplace=True)

	print "sorting by p value"
	df.sort_values(by=[args.p],inplace=True)

	print "extracting top " + str(args.n) + " variants"
	df = df.head(n=args.n)
	df.to_csv(args.out, header=True, index=False, sep="\t", na_rep="NA")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--exclude', help='a variant exclusion file')
	parser.add_argument('--n', type=int, default=1000, help='number of top variants to report')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--mac', help='a minor allele count column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
