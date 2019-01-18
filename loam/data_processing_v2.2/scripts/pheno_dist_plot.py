import pandas as pd
import numpy as np
from math import log, isnan, ceil
import argparse
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as scipy

sns.set(context='notebook', style='darkgrid', palette='deep', font='sans-serif', font_scale=1, color_codes=False, rc=None)
sns.set_style("ticks",{'axes.grid': True, 'grid.color': '.9'})

def dist_boxplot(v, **kwargs):
	if len(v) >= 1:
		ax = sns.distplot(v, kde=True, hist_kws=dict(alpha=0.5), color="grey",vertical=True, rug=True)
		ax2 = ax.twiny()
		sns.boxplot(y=v, ax=ax2, flierprops=dict(marker='o',markersize=5), width=3) #fliersize=3
		ax2.set(xlim=(-5, 5))
		ax2.set_xticks([])
	else:
		ax = None
		ax2 = None

def main(args=None):

	print "reading phenotypes from file"
	df = pd.read_table(args.pheno, sep="\t", dtype = {'ID': np.str})
	pops = ['AFR','AMR','EAS','EUR','SAS']
	if args.strat:
		anc = pd.read_table(args.ancestry, sep="\t")
		anc.rename(columns = {'IID': 'ID', 'FINAL': 'POP'}, inplace = True)
		anc['ID'] = anc['ID'].astype(str)
		df = df.merge(anc)
	else:
		df['POP'] = "ALL"

	print "extracting samples in clean fam file"
	samples_df = pd.read_table(args.fam, header=None, sep=" ")
	samples = samples_df[1].astype(str).tolist()
	df = df[df['ID'].isin(samples)]
	print "excluding samples in samples exclude file"
	if args.samples_exclude != "":
		with open(args.samples_exclude) as f:
			exclude = [line.strip() for line in f]
		df = df[~df['ID'].isin(exclude)]

	df.dropna(subset = [args.pheno_name], inplace=True)

	if args.strat:
		df['POP'] = df['POP'].astype('category')
		if len(df['POP'].unique()) < len(pops):
			df['POP'] = df['POP'].cat.add_categories([p for p in pops if p not in df['POP'].unique()])
		df['POP'] = df['POP'].cat.reorder_categories(pops, ordered=True)

	if len(df[args.pheno_name].unique()) > 2:

		if args.strat:

			print "generating distribution plots stratified by ancestry"
			g = sns.FacetGrid(df, col="POP", col_order=sort(df['POP'].unique()), sharex=False, sharey=True, size=6, aspect=0.3)
			g.map(dist_boxplot, args.pheno_name)

			## format facets
			for k, axes_row in enumerate(g.axes):
				for l, axes_col in enumerate(axes_row):
					col = axes_col.get_title().replace('POP = ','')
					axes_col.set_title(col, size=16)
					axes_col.set_xlabel('')
					axes_col.minorticks_on()
		
			g.fig.tight_layout(w_pad=1)
		
		else:
		
			print "generating distribution plot"
			fig, ax1 = plt.subplots()
			ax1 = sns.distplot(df[args.pheno_name], kde=True, hist_kws=dict(alpha=0.5), color="grey",vertical=False, rug=True)
			ax2 = ax1.twinx()
			sns.boxplot(x=args.pheno_name, data=df, ax=ax2, flierprops=dict(marker='o',markersize=5), width=3) #fliersize=3
			ax2.set(ylim=(-5, 5))
			ax2.set_yticks([])
			ax1.set_xlabel("")
			fig.tight_layout(w_pad=1)

	else:

		if args.strat:
			print "generating distribution plots stratified by ancestry"
			vals = np.sort(df[args.pheno_name].unique())
			df.replace({args.pheno_name: {vals[1]: 'Case', vals[0]: 'Control'}}, inplace=True)
			print "generating count plot"
			fig, ax = plt.subplots()
			ax = sns.countplot(x="POP", hue=args.pheno_name, data=df, order=sort(df['POP'].unique()), hue_order=['Case','Control'])
			for p in ax.patches:
				height = p.get_height() if not np.isnan(p.get_height()) else 0
				ax.text(p.get_x() + p.get_width()/2., height, '%d' % int(height), ha="center", va="bottom")
			ax.set_xlabel("")
			ax.set_ylabel("")
			l = ax.legend(loc = "upper right")
			l.set_title("")
			fig.tight_layout(w_pad=1)

		else:

			print "generating distribution plot"
			vals = np.sort(df[args.pheno_name].unique())
			df.replace({args.pheno_name: {vals[1]: 'Case', vals[0]: 'Control'}}, inplace=True)
			print "generating count plot"
			fig, ax = plt.subplots()
			ax = sns.countplot(x=args.pheno_name, data=df, order=['Case','Control'])
			for p in ax.patches:
				height = p.get_height() if not np.isnan(p.get_height()) else 0
				ax.text(p.get_x() + p.get_width()/2., height, '%d' % int(height), ha="center", va="bottom")
			ax.set_xlabel("")
			ax.set_ylabel("")
			fig.tight_layout(w_pad=1)

	plt.savefig(args.out,dpi=300)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--strat', action='store_true', help='make a plot stratified by ancestry')
	parser.add_argument('--ancestry', help='an ancestry file name')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--pheno', help='a phenotype file name', required=True)
	requiredArgs.add_argument('--pheno-name', help='a phenotype name', required=True)
	requiredArgs.add_argument('--fam', help='a fam file with clean samples', required=True)
	requiredArgs.add_argument('--samples-exclude', help='a sample exclusions file ("" = ignored)', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
