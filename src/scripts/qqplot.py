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

def qqplot(pvals, file, gc = False, mafstring = None):

	print "num variants: " + str(len(pvals))
	print "minimum p-value: {0:.3g}".format(np.min(pvals))

	lmda = np.median(scipy.chi2.ppf([1-x for x in pvals.tolist()], df=1))/scipy.chi2.ppf(0.5,1)
	print "genomic inflation rate: " + str(lmda)

	if gc and lmda > 1:
		print "applying genomic control to p-values"
		pvals=2 * scipy.norm.cdf(-1 * np.abs(scipy.norm.ppf(0.5*pvals) / math.sqrt(lmda)))
		print "minimum post-gc adjustment p-value: {0:.3g}".format(np.min(pvals))

	logpvals = -1 * np.log10(pvals) + 0.0
	print "maximum -1*log10(p-value): {0:.3g}".format(np.max(logpvals))

	spvals = sorted(filter(lambda x: x and not(isnan(x)), pvals))
	exp = sorted([-log(float(i) / len(spvals), 10) for i in np.arange(1, len(spvals) + 1, 1)])
	obs = sorted(logpvals.tolist())
	expMax = int(ceil(max(exp)))
	obsMax = int(ceil(max(obs)))
	ci_upper = sorted((-1 * np.log10(scipy.beta.ppf(0.95, range(1,len(obs) + 1), range(len(obs),0,-1)))).tolist())
	ci_lower = sorted((-1 * np.log10(scipy.beta.ppf(0.05, range(1,len(obs) + 1), range(len(obs),0,-1)))).tolist())
	plt.clf()
	plt.figure(figsize=(6,6))
	plt.scatter(exp, obs, c="#1F76B4", s=12)
	plt.plot((0, max(exp)),(0,max(exp)), linewidth=0.75, c="#B8860B")
	plt.fill_between(exp, ci_lower, ci_upper, color="#646464", alpha=0.15)
	plt.xlabel(r"Expected $- log_{10} (p)$")
	plt.ylabel(r"Observed $- log_{10} (p)$")
	plt.xlim(0, expMax)
	plt.ylim(0, max(obsMax, int(ceil(max(ci_upper))+1)))
	if mafstring is not None:
		plt.annotate(r"$MAF \in {0}$".format(mafstring), xy=(0, 1), xycoords='axes fraction', horizontalalignment='left', verticalalignment='bottom', size='small', weight='bold', annotation_clip = False)
	plt.annotate(r"$N = {0:,}$".format(len(obs)), xy=(0.5, 1), xycoords='axes fraction', horizontalalignment='center', verticalalignment='bottom', size='small', weight='bold', annotation_clip = False)
	if lmda is not None:
		plt.annotate(r"$\lambda \approx {0:.3f}$".format(lmda), xy=(1, 1), xycoords='axes fraction', horizontalalignment='right', verticalalignment='bottom', size='small', weight='bold', annotation_clip = False)
	plt.savefig(file, bbox_inches='tight', dpi=300)

def empty_qqplot(file):

	plt.clf()
	plt.figure(figsize=(6,6))
	plt.scatter([0], [0], c="#1F76B4", s=12)
	plt.plot((0, 1),(0, 1), linewidth=0.75, c="#B8860B")
	plt.xlabel(r"Expected $- log_{10} (p)$")
	plt.ylabel(r"Observed $- log_{10} (p)$")
	plt.xlim(0, 1)
	plt.ylim(0, 1)
	plt.annotate(r"$N = {0:,}$".format(0), xy=(0.5, 1), xycoords='axes fraction', horizontalalignment='center', verticalalignment='bottom', size='small', weight='bold', annotation_clip = False)
	plt.savefig(file, bbox_inches='tight', dpi=300)

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

	if args.eaf:
		df['__MAF__'] = np.where(df[args.eaf] > 0.5, 1 - df[args.eaf], df[args.eaf])

	if args.mac:
		if len([col for col in df if col.startswith('case_') or col.startswith('ctrl_')]) > 0:
			print "removing {0:d} variants with mac < 20".format(df[df[args.mac] < 20].shape[0])
			df = df[df[args.mac] >= 20]
		else:
			print "removing {0:d} variants with mac < 3".format(df[df[args.mac] < 3].shape[0])
			df = df[df[args.mac] >= 3]
	elif args.n and args.eaf:
		df['__MAC__'] = np.floor(df[args.n] * df['__MAF__'])
		if len([col for col in df if col.startswith('case_') or col.startswith('ctrl_')]) > 0:
			print "removing {0:d} variants with mac < 20".format(df[df['__MAC__'] < 20].shape[0])
			df = df[df['__MAC__'] >= 20]
		else:
			print "removing {0:d} variants with mac < 3".format(df[df['__MAC__'] < 3].shape[0])
			df = df[df['__MAC__'] >= 3]

	df.loc[df[args.p] == 0, args.p] = 1e-300
	df.reset_index(drop=True, inplace=True)

	print "generating qq plot"
	qqplot(df[args.p], args.out, gc = args.gc)

	if args.eaf:
		if args.out_low_maf:
			if len(df[args.p][(0.005 <= df['__MAF__']) & (df['__MAF__'] < 0.01)]) > 0:
				print "generating low maf qq plot"
				qqplot(df[args.p][(0.005 <= df['__MAF__']) & (df['__MAF__'] < 0.01)], args.out_low_maf, gc = args.gc, mafstring = "[0.005, 0.01)")
			else:
				print "no low maf variants found... generating empty qq plot"
				empty_qqplot(args.out_low_maf)
		if args.out_mid_maf:
			if len(df[args.p][(0.01 <= df['__MAF__']) & (df['__MAF__'] < 0.05)]) > 0:
				print "generating mid maf qq plot"
				qqplot(df[args.p][(0.01 <= df['__MAF__']) & (df['__MAF__'] < 0.05)], args.out_mid_maf, gc = args.gc, mafstring = "[0.01, 0.05)")
			else:
				print "no mid maf variants found... generating empty qq plot"
				empty_qqplot(args.out_mid_maf)
		if args.out_high_maf:
			if len(df[args.p][0.05 <= df['__MAF__']]) > 0:
				print "generating high maf qq plot"
				qqplot(df[args.p][0.05 <= df['__MAF__']], args.out_high_maf, gc = args.gc, mafstring = "[0.05, 0.5]")
			else:
				print "no high maf variants found... generating empty qq plot"
				empty_qqplot(args.out_high_maf)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--gc', action='store_true', help='flag indicates that genomic control should be applied to results before plotting')
	parser.add_argument('--eaf', help='a minor allele frequency column name in --results')
	parser.add_argument('--n', help='a sample count column name in --results')
	parser.add_argument('--out-low-maf', help='an output filename for low maf plot ending in .png or .pdf')
	parser.add_argument('--out-mid-maf', help='an output filename for mid maf plot ending in .png or .pdf')
	parser.add_argument('--out-high-maf', help='an output filename for high maf plot ending in .png or .pdf')
	parser.add_argument('--exclude', help='a variant exclusion file')
	parser.add_argument('--mac', help='a minor allele count column name in --results')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
