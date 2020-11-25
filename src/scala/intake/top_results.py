import pandas as pd
import numpy as np
import argparse

def main(args=None):

	print "reading results from file"
	df=pd.read_table(args.results, low_memory=False)

	df.dropna(subset=[args.p], inplace=True)

	df.reset_index(drop=True, inplace=True)

	print "sorting by p value"
	df.sort_values(by=[args.p],inplace=True)

	print "extracting mhtplot variants"
	df[df[args.p] <= 1e-4].to_csv(args.out_mht, header=True, index=False, sep="\t", na_rep="NA")

	print "extracting top " + str(args.n) + " variants"
	df.head(n=args.n).to_csv(args.out, header=True, index=False, sep="\t", na_rep="NA")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--n', type=int, default=1000, help='number of top variants to report')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename', required=True)
	requiredArgs.add_argument('--out-mht', help='an output filename for mht input file', required=True)
	args = parser.parse_args()
	main(args)
