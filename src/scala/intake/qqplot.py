import pandas as pd
import numpy as np
from math import log, isnan, ceil
import argparse
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as scipy
import sys

sns.set(context='notebook', style='darkgrid', palette='deep', font='sans-serif', font_scale=1, color_codes=False, rc=None)

def qqplot(pvals, file, mafstring = None):

	print "num variants: " + str(len(pvals))
	print "minimum p-value: {0:.3g}".format(np.min(pvals))

	lmda = np.median(scipy.chi2.ppf([1-x for x in pvals], df=1))/scipy.chi2.ppf(0.5,1)
	print "genomic inflation rate: " + str(lmda)

	obs = np.sort(-1 * np.log10(pvals) + 0.0)
	print "maximum -1*log10(p-value): {0:.3g}".format(np.max(obs))

	exp = np.array([-log(float(i) / len(pvals), 10) for i in np.arange(1, len(pvals) + 1, 1)], dtype=np.dtype(float))

	expMax = int(ceil(max(exp)))
	obsMax = int(ceil(max(obs)))

	ci_upper = np.sort(np.array((-1 * np.log10(scipy.beta.ppf(0.95, np.arange(1,len(obs) + 1), np.arange(len(obs),0,-1)))).tolist(), dtype=np.dtype(float)))
	ci_lower = np.sort(np.array((-1 * np.log10(scipy.beta.ppf(0.05, np.arange(1,len(obs) + 1), np.arange(len(obs),0,-1)))).tolist(), dtype=np.dtype(float)))

	objects=[]
	for name,obj in locals().items():
		objects.append([name,sys.getsizeof(obj)])
	print(sorted(objects,key=lambda x: x[1],reverse=True))

	#plt.clf()
	#plt.figure(figsize=(6,6))
	#plt.scatter(exp, obs, c="#1F76B4", s=12)
	#plt.plot((0, max(exp)),(0,max(exp)), linewidth=0.75, c="#B8860B")
	#plt.fill_between(exp, ci_lower, ci_upper, color="#646464", alpha=0.15)
	#plt.xlabel(r"Expected $- log_{10} (p)$")
	#plt.ylabel(r"Observed $- log_{10} (p)$")
	#plt.xlim(0, expMax)
	#plt.ylim(0, max(obsMax, int(ceil(max(ci_upper))+1)))
	#plt.annotate(r"$N = {0:,}$".format(len(obs)), xy=(0.5, 1), xycoords='axes fraction', horizontalalignment='center', verticalalignment='bottom', size='small', weight='bold', annotation_clip = False)
	#if lmda is not None:
	#	plt.annotate(r"$\lambda \approx {0:.3f}$".format(lmda), xy=(1, 1), xycoords='axes fraction', horizontalalignment='right', verticalalignment='bottom', size='small', weight='bold', annotation_clip = False)
	#plt.savefig(file, bbox_inches='tight', dpi=300)

def main(args=None):

	print "reading results from file"
	a=pd.read_table(args.results, usecols=[args.p], dtype=np.dtype(float)).values

	a[a < 1e-300] = 1e-300
	a = np.sort(a)

	objects=[]
	for name,obj in locals().items():
		objects.append([name,sys.getsizeof(obj)])
	print(sorted(objects,key=lambda x: x[1],reverse=True))

	print "generating qq plot"
	qqplot(a, args.out)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
