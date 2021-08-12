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
	df = pd.read_table(args.pheno, sep="\t", dtype = {args.iid_col: np.str})

	covars_factors = args.covars.split("+")

	num_covars = []
	cat_covars = []
	for cv in covars_factors:
		cvv = [char for char in cv]
		if cvv[0] == "[" and cvv[len(cvv)-1] == "]":
			cat_covars.append("".join(cvv[1:(len(cvv)-1)]))
		else:
			num_covars.append(cv)

	for cc in cat_covars:
		df[cc] = df[cc].astype('category')

	summary_all = df[[args.pheno_name]+cat_covars+num_covars].describe(include='all')
	summary_all['COHORT'] = "-"
	summary_all['STAT'] = summary_all.index
	summary_all.reset_index(drop=True)
	summary_all = summary_all[['COHORT','STAT'] + [c for c in summary_all.columns if c not in ['COHORT','STAT']]]

	if args.cohorts_map:
		cohorts = pd.read_table(args.cohorts_map, header=None, sep="\t", names=[args.iid_col,"COHORT"])
		cohorts[args.iid_col] = cohorts[args.iid_col].astype(str)
		df = df.merge(cohorts)
		df['COHORT'] = df['COHORT'].astype('category')

		for cohort in df['COHORT'].unique():
			summary_cohort = df[[args.pheno_name]+cat_covars+num_covars][df['COHORT'] == cohort].describe(include='all')
			summary_cohort['COHORT'] = cohort
			summary_cohort['STAT'] = summary_cohort.index
			summary_cohort.reset_index(drop=True)
			summary_cohort = summary_cohort[['COHORT','STAT'] + [c for c in summary_cohort.columns if c not in ['COHORT','STAT']]]
			summary_all = pd.concat([summary_all, summary_cohort])

	summary_all.to_csv(args.out_vars_summary, header=True, index=False, sep="\t", na_rep="NA")

	df.dropna(subset = [args.pheno_name], inplace=True)

	if len(df[args.pheno_name].unique()) > 2:

		if args.cohorts_map:

			print "generating distribution plots stratified by cohort"
			g = sns.FacetGrid(df, col="COHORT", col_order=np.sort(df['COHORT'].unique()), sharex=False, sharey=True, size=6, aspect=0.3)
			g.map(dist_boxplot, args.pheno_name)

			## format facets
			for k, axes_row in enumerate(g.axes):
				for l, axes_col in enumerate(axes_row):
					col = axes_col.get_title().replace('COHORT = ','')
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

		if args.cohorts_map:
			print "generating distribution plots stratified by cohort"
			vals = np.sort(df[args.pheno_name].unique())
			df.replace({args.pheno_name: {vals[1]: 'Case', vals[0]: 'Control'}}, inplace=True)
			print "generating count plot"
			fig, ax = plt.subplots()
			ax = sns.countplot(x="COHORT", hue=args.pheno_name, data=df, order=np.sort(df['COHORT'].unique()), hue_order=['Case','Control'])
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

	plt.savefig(args.out_plot,dpi=300)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--cohorts-map', help='a model cohort map')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--pheno', help='a phenotype file name', required=True)
	requiredArgs.add_argument('--pheno-name', help='a phenotype name', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--covars', help='a + delimited list of covariates, indicating categorical covars with []', required=True)
	requiredArgs.add_argument('--out-plot', help='a plot output filename ending in .png or .pdf', required=True)
	requiredArgs.add_argument('--out-vars-summary', help='a model vars summary output filename', required=True)
	args = parser.parse_args()
	main(args)
