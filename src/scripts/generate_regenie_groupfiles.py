import pandas as pd
import numpy as np
import argparse
from pathlib import Path
import time
import re
import gzip
import datatable as dt

def main(args=None):

	with gzip.open(args.filters, 'rb') as f:
		line=f.readline().rstrip().decode("utf-8").split('\t')
	filter_dtypes=dict(zip(line,['uint8' for x in line]))
	filter_dtypes['locus']='str'
	filter_dtypes['alleles']='str'
	filter_dtypes['rsid']='str'
	filter_dtypes['annotation.Gene']='str'

	df = pd.read_table(args.filters, low_memory=True, compression="gzip", dtype=filter_dtypes)
	# 1m53.954s
	#with gzip.open(args.filters, 'rb') as f:
	#	df = dt.fread(f)
	#	print(df.shape)
	#	return

	n_all = df.shape[0]
	n_exclude = df[df['ls_global_exclude'] == 1].shape[0]
	print(str(n_exclude) + " variants flagged for removal via standard filters")
	print(str(n_all - n_exclude) + " variants remaining for analysis")

	if n_all - n_exclude > 0:

		df = df[(~df['annotation.Gene'].isnull()) & (df['ls_global_exclude'] == 0)]

		if df.shape[0] > 0:

			df[['chr','pos']]=df.locus.str.split(":",expand=True)
			df['chr_num']=df['chr']
			df.replace({'chr_num': {'X': '23', 'Y': '24', 'XY': '25', 'MT': '26'}}, inplace=True)
			df.chr_num=df.chr_num.astype(int)
			df.pos=df.pos.astype(int)
			df.sort_values(by=['chr_num','pos'],inplace=True)
			df.reset_index(drop=True, inplace=True)
			print("reduced to " + str(df.shape[0]) + " variants passing global filters")
		
			genes=df['annotation.Gene'].unique()
			print("found " + str(len(genes)) + " genes")
		
			print("extracting minimum positions for genes")
			df_first_pos=df[['annotation.Gene','chr','chr_num','pos']].drop_duplicates(subset=['annotation.Gene'], keep='first')
		
			with open(args.setlist_out, 'w') as setlist:
				print("grouping variants into genes")
				setlist_df=df[['annotation.Gene','rsid']]
				print(setlist_df.head())
				setlist_df=setlist_df.groupby('annotation.Gene', as_index=False, sort=False).agg(','.join)
				print(setlist_df.head())
				setlist_df=df_first_pos.merge(setlist_df)
				setlist_df.sort_values(by=['chr_num','pos'],inplace=True)
				setlist_df[['annotation.Gene','chr','pos','rsid']].to_csv(setlist, header=False, index=False, sep="\t", na_rep="NA")
		
			i=0
			for mask in args.masks.split(','):
				i=i+1
				print("adding annotations for mask " + str(i) + " / " + str(len(args.masks)))
				mask_df=df[df['ls_mask_' + mask + '.exclude'] == 0][['rsid','annotation.Gene']]
				mask_df['mask']=mask
				if i == 1:
					annot_df=mask_df.copy()
				else:
					annot_df=pd.concat([annot_df,mask_df])
			print("grouping masks into variants")
			annot_df=annot_df.groupby(['rsid','annotation.Gene'], as_index=False, sort=False)['mask'].agg(';'.join)
		
			with open(args.annotations_out, 'w') as annots:
				print("writing annotations to file")
				annot_df.to_csv(annots, header=False, index=False, sep="\t", na_rep="NA")

			mask_sets=annot_df['mask'].unique()
			with open(args.masks_out, 'w') as mask_file:
				for mask in args.masks.split(','):
					mask_sets_used=[x for x in mask_sets if mask in x.split(";")]
					if len(mask_sets_used) > 0:
						print("writing mask annotations used for mask " + mask)
						mask_file.write(mask + "\t" + ",".join(mask_sets_used) + "\n")
					else:
						print("no annotations exist for mask " + mask)

		else:

			print("no variants with non-missing gene annotations remaining ... writing empty files")
			Path(args.annotations_out).touch()
			Path(args.setlist_out).touch()
			Path(args.masks_out).touch()

	else:

		print("no variants remaining after standard filters ... writing empty files")
		Path(args.annotations_out).touch()
		Path(args.setlist_out).touch()
		Path(args.masks_out).touch()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--masks', help='a list of mask ids separated by a comma')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--filters', help='a filter file', required=True)
	requiredArgs.add_argument('--setlist-out', help='an output file basename for regenie setlist file', required=True)
	requiredArgs.add_argument('--masks-out', help='an output file basename for regenie masks file', required=True)
	requiredArgs.add_argument('--annotations-out', help='an output file basename for regenie annotations file', required=True)
	args = parser.parse_args()
	main(args)
