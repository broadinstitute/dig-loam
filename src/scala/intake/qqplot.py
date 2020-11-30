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

def qqplot(pvals, file, mafstring = None):

	print "num variants: " + str(len(pvals))
	print "minimum p-value: {0:.3g}".format(np.min(pvals))

	lmda = np.median(scipy.chi2.ppf([1-x for x in pvals.tolist()], df=1))/scipy.chi2.ppf(0.5,1)
	print "genomic inflation rate: " + str(lmda)

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
	df=pd.read_table(args.results, low_memory=False)

	df.dropna(subset=[args.p], inplace=True)

	df.loc[df[args.p] < 1e-300, args.p] = 1e-300
	df.reset_index(drop=True, inplace=True)

	print "generating qq plot"
	qqplot(df[args.p], args.out)

	if args.eaf:
		if args.out_common:
			if len(df[args.p][(0.03 <= df[args.eaf]) & (0.97 >= df[args.eaf])]) > 0:
				print "generating common variants qq plot"
				qqplot(df[args.p][(0.03 <= df[args.eaf]) & (0.97 >= df[args.eaf])], args.out_common, mafstring = "[0.03, 0.5]")
			else:
				print "no common variants found... generating empty qq plot"
				empty_qqplot(args.out_common)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--eaf', help='an effect allele frequency column name in --results')
	parser.add_argument('--out-common', help='an output filename for common variants plot ending in .png or .pdf')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
