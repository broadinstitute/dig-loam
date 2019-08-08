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

def qqplot(pvals, file, gc = False):

	print "minimum p-value: {0:.3g}".format(np.min(pvals))

	print "calculating genomic inflation rate"
	lmda = np.median(scipy.chi2.ppf([1-x for x in pvals.tolist()], df=1))/scipy.chi2.ppf(0.5,1)

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
	if lmda is not None:
		plt.annotate(r"$\lambda \approx {0:.3f}$".format(lmda), xy=(1, 1), xycoords='axes fraction', horizontalalignment='right', verticalalignment='bottom', size='small', weight='bold', annotation_clip = False)
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
	df.reset_index(drop=True, inplace=True)

	print "generating qq plot"
	qqplot(df[args.p], args.out, gc = args.gc)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--gc', action='store_true', help='flag indicates that genomic control should be applied to results before plotting')
	parser.add_argument('--exclude', help='a variant exclusion file')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
