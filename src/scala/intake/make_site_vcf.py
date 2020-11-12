import pandas as pd
import argparse

def main(args=None):

	print("reading results from file")
	df = pd.read_table(args.results, low_memory=False, usecols = ['marker'])
	
	df['#CHROM'] = df['marker'].apply(lambda x: x.split("_")[0])
	df['POS'] = df['marker'].apply(lambda x: x.split("_")[1])
	df['ID'] = df['marker']
	df['REF'] = df['marker'].apply(lambda x: x.split("_")[2])
	df['ALT'] = df['marker'].apply(lambda x: x.split("_")[3])
	df['QUAL'] = "."
	df['FILTER'] = "."
	df['INFO'] = "."

	print("sort by chr, pos, ref, alt")
	df.sort_values(by=['#CHROM','POS','REF','ALT'],inplace=True)

	df[['#CHROM','POS','ID','REF','ALT','QUAL','FILTER','INFO']].to_csv(args.out, header=True, index=False, sep="\t", na_rep="NA")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--out', help='an output filename', required=True)
	args = parser.parse_args()
	main(args)
