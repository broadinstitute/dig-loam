import pandas as pd
import numpy as np
import argparse

def main(args=None):

	print "reading results from file"
	df = pd.read_table(args.results, low_memory=False, compression="gzip")
	h1 = list(df.columns)[0:3]
	h2 = list(df.columns)[4:len(list(df.columns))]

	print "reading group id map"
	ids = pd.read_table(args.group_id_map)

	df.dropna(subset=[args.p], inplace=True)
	df.reset_index(drop=True, inplace=True)

	df['ENSG'] = df['MARKER_ID'].str.split("_").str[-1]
	ids = ids.rename(columns = {'Gene stable ID': 'ENSG', 'Gene name': 'HGNC'})
	df = df.merge(ids)
	df.drop(columns = ['MARKER_ID'], inplace=True)
	df = df[h1 + ['ENSG','HGNC'] + h2]

	print "sorting by p value"
	df.sort_values(by=[args.p],inplace=True)

	print "extracting top " + str(args.n) + " groups"
	df = df.head(n=args.n)
	df.to_csv(args.out, header=True, index=False, sep="\t")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--n', type=int, default=20, help='number of top variants to report')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--group-id-map', help='a group map file', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
