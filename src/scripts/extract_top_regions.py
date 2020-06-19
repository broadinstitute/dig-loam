import pandas as pd
import numpy as np
import argparse

def main(args=None):

	print "reading results from file"
	df=pd.read_table(args.results, low_memory=False, compression="gzip")

	#if args.exclude:
	#	print "reading in variant exclusions from file"
	#	excl=pd.read_table(args.exclude, header=None, low_memory=False)
	#	excl.columns=["locus","alleles"]
	#	excl_list=excl['locus'] + ':' + excl['alleles'].str.replace('\[|\]|\"','').str.replace(',',':')
	#	df['excl_id']=df['#chr'].map(str) + ':' + df['pos'].map(str) + ':' + df['ref'].map(str) + ':' + df['alt'].map(str)
	#	df=df[~ df['excl_id'].isin(excl_list)]
	#	df.drop(columns=['excl_id'], inplace=True)

	df.dropna(subset=[args.p], inplace=True)
	df.reset_index(drop=True, inplace=True)

	if df.shape[0] > 0:

		if df.shape[0] >= 1000000:
			sig = 5.4e-8
		else:
			sig = 0.05 / df.shape[0]
		print "significance level set to p-value = {0:.3g} (-1*log10(p-value) = {1:.3g})".format(sig, -1 * np.log10(sig))
	
		df_sig = df[df[args.p] <= sig]
		if df_sig.shape[0] > 0:
			df_sig.reset_index(drop=True, inplace=True)
			print "{0:d} genome wide significant variants".format(df_sig.shape[0])
		
			df_sig = df_sig.assign(start=df_sig[args.pos].values - 100000)
			df_sig = df_sig.assign(end=df_sig[args.pos].values + 100000)
		
			df_sig = df_sig[[args.chr,'start','end','id']]
			df_sig.sort_values([args.chr,'start'], inplace=True)
			df_sig.reset_index(drop=True, inplace=True)
		
			print "extracting variants in significant regions"
			for index, row in df_sig.iterrows():
				if index == 0:
					message = "initialize first variant " + row['id'] + " = " + str(row[args.chr]) + ":" + str(row['start']) + "-" + str(row['end'])
					out = df_sig.loc[[index]]
				else:
					if row[args.chr] != df_sig.loc[index-1,args.chr]:
						message = "initialize first variant " + row['id'] + " = " + str(row[args.chr]) + ":" + str(row['start']) + "-" + str(row['end'])
						out = out.append(row, ignore_index=True)
					elif df_sig.loc[index-1,'start'] <= row['start'] and row['start'] <= df_sig.loc[index-1,'end'] and row['end'] > df_sig.loc[index-1,'end']:
						message = "merge variant " + row['id'] + " = " + str(row[args.chr]) + ":" + str(row['start']) + "-" + str(row['end']) + " with previous region "  + str(df_sig.loc[index-1,args.chr]) + ":" + str(df_sig.loc[index-1,'start']) + "-" + str(df_sig.loc[index-1,'end'])
						out.loc[out.shape[0]-1,'end'] = row['end']
					elif row['start'] > df_sig.loc[index-1,'end']:
						message = "initialize first variant " + row['id'] + " = " + str(row[args.chr]) + ":" + str(row['start']) + "-" + str(row['end'])
						out = out.append(row, ignore_index=True)
				print message
		
			out['top_variant'] = ""
			out['top_pos'] = 0
			out['top_pval'] = 0
			for index, row in out.iterrows():
				df_region = df.loc[(df[args.chr] == row[args.chr]) & (df[args.pos] >= row['start']) & (df[args.pos] <= row['end'])].reset_index(drop=True)
				out.loc[index,'top_variant'] = df_region.loc[df_region[args.p].idxmin(), 'id']
				out.loc[index,'top_pos'] = df_region.loc[df_region[args.p].idxmin(), args.pos]
				out.loc[index,'top_pval'] = df_region.loc[df_region[args.p].idxmin(), args.p]
			out.sort_values(['top_pval'], inplace=True)
		else:
			out = pd.DataFrame({args.chr: [], 'start': [], 'end': [], 'top_variant': [], 'top_pos': [], 'top_pval': []})

	else:
		out = pd.DataFrame({args.chr: [], 'start': [], 'end': [], 'top_variant': [], 'top_pos': [], 'top_pval': []})

	if args.max_regions and out.shape[0] > args.max_regions:
		print "limiting maximum number of regions to " + str(args.max_regions)
		out = out.head(n = args.max_regions)

	out[[args.chr,'start','end','top_variant','top_pos','top_pval']].to_csv(args.out, header=False, index=False, sep="\t")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--max-regions', type=int, help='maximum number of top regions to report')
	#parser.add_argument('--exclude', help='a variant exclusion file')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--chr', help='a chromosome column name in --results', required=True)
	requiredArgs.add_argument('--pos', help='a position column name in --results', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename', required=True)
	args = parser.parse_args()
	main(args)
