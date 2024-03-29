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

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def mhtplot(df, chr, pos, p, nVars, file, bicolor = False):

	logp = 'log' + p
	gpos = 'g' + pos
	chrInt = chr + 'Int'
	
	print "minimum p-value: {0:.3g}".format(np.min(df[p]))
	
	df[logp] = -1 * np.log10(df[p]) + 0.0
	print "maximum -1*log10(p-value): {0:.3g}".format(np.max(df[logp]))
	
	print "calculating genomic positions"
	df[chr] = df[chr].astype(str)
	df[chrInt] = df[chr].replace({'X': '23', 'Y': '24', 'MT': '25'}).astype(int)
	df.sort_values(by=[chrInt,pos], inplace=True)
	ticks = []
	lastbase = 0
	df[gpos] = 0
	nchr = len(list(np.unique(df[chr].values)))
	chrs = list(np.sort(np.unique(df[chr][~df[chr].isin(['X','Y','MT'])].values.astype(int))).astype(str))
	if 'X' in df[chr].values:
		chrs.extend(['X'])
	if 'Y' in df[chr].values:
		chrs.extend(['Y'])
	if 'MT' in df[chr].values:
		chrs.extend(['MT'])

	if not bicolor:
		chr_hex = ["#08306B","#41AB5D","#000000","#F16913","#3F007D","#EF3B2C","#08519C","#238B45","#252525","#D94801","#54278F","#CB181D","#2171B5","#006D2C","#525252","#A63603","#6A51A3","#A50F15","#4292C6","#00441B","#737373","#7F2704","#807DBA","#67000D","#6BAED6"]
	else:
		chr_hex = ["#08589e","#4eb3d3"]*12 + ["#08589e"]

	if nchr == 1:
		df[gpos] = df[pos]
		df['chr_hex'] = "#08589e"
		if df[gpos].max() - df[gpos].min() <= 1000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 100 == 0]
		elif df[gpos].max() - df[gpos].min() <= 10000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 1000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 100000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 10000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 200000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 20000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 300000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 30000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 400000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 40000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 500000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 50000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 600000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 60000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 700000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 70000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 800000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 80000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 900000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 90000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 1000000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 100000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 10000000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 1000000 == 0]
		elif df[gpos].max() - df[gpos].min() <= 100000000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 10000000 == 0]
		elif df[gpos].max() - df[gpos].min() > 100000000:
			ticks = [x for x in range(df[gpos].min(),df[gpos].max()) if x % 25000000 == 0]
		x_labels = ["{:,}".format(x/1000) for x in ticks]
	else:
		df['chr_hex'] = "#000000"
		for i in range(len(chrs)):
			print "processed chromosome {}".format(chrs[i])
			if i == 0:
				df.loc[df[chr] == chrs[i],gpos] = df.loc[df[chr] == chrs[i],pos]
			else:
				lastbase = lastbase + df.loc[df[chr] == chrs[i-1],pos].iloc[-1]
				df.loc[df[chr] == chrs[i],gpos] = (df.loc[df[chr] == chrs[i],pos]) + lastbase
			if df.loc[df[chr] == chrs[i]].shape[0] > 1:
				ticks.append(df.loc[df[chr] == chrs[i],gpos].iloc[0] + (df.loc[df[chr] == chrs[i],gpos].iloc[-1] - df.loc[df[chr] == chrs[i],gpos].iloc[0])/2)
			else:
				ticks.append(df.loc[df[chr] == chrs[i],gpos].iloc[0])
			df.loc[df[chr] == chrs[i],'chr_hex'] = chr_hex[i]
		x_labels = [a.replace('MT','\nMT') if a == 'MT' else a for a in chrs]

	if nVars >= 1000000:
		sig = 5.4e-8
	else:
		sig = 0.05 / nVars
	print "significance level set to p-value = {0:.3g} (-1*log10(p-value) = {1:.3g})".format(sig, -1 * np.log10(sig))
	print "{0:d} genome wide significant variants".format(len(df[p][df[p] <= sig]))

	yMin = 0
	yMax=int(max(np.ceil(-1 * np.log10(sig)),np.ceil(df[logp].max())))
	if yMax > 20:
		y_breaks = range(0,yMax,5)
		y_labels = range(0,yMax,5)
	else:
		y_breaks = range(0,yMax+1)
		y_labels = range(0,yMax+1)

	plt.clf()
	plt.figure(figsize=(16,4))
	for i in range(len(chrs)):
		plt.scatter(df.loc[df[chr] == chrs[i], gpos], df.loc[df[chr] == chrs[i], logp], c=chr_hex[i], s=8, clip_on=False)
	plt.axhline(y = -1 * np.log10(sig), linewidth=0.75, color="#B8860B")
	plt.xlabel("Chromosome")
	plt.xticks(ticks, x_labels)
	plt.ylabel(r"$- log_{10} (p)$")
	plt.yticks(y_breaks, y_labels)
	plt.xlim(min(df[gpos]), max(df[gpos]))
	plt.ylim(yMin, yMax)
	plt.annotate(r"$gws \approx {0:.3g}$".format(sig), xy=(max(df[gpos]), yMax), horizontalalignment='right', verticalalignment='bottom', color="#B8860B", size='small', weight='bold', annotation_clip = False)
	ax = plt.gca()
	for xtick, color in zip(ax.get_xticklabels(), chr_hex[0:len(chrs)]):
		xtick.set_color(color)
	plt.savefig(file, bbox_inches='tight', dpi=300)

def main(args=None):

	print "reading plottable results from file"
	df=pd.read_table(args.results, usecols=['marker',args.p], dtype={'marker': np.dtype(str), args.p: np.dtype(float)})

	df['chr'] = df['marker'].apply(lambda x: x.split("_")[0])
	df['pos'] = df['marker'].apply(lambda x: int(x.split("_")[1]))

	df.loc[df[args.p] < 1e-300, args.p] = 1e-300
	df.reset_index(drop=True, inplace=True)

	var_count = file_len(args.full_results)
	print "total variant count in full results: {0:d}".format(var_count)

	print "generating manhattan plot for " + str(df.shape[0]) + " variants"
	mhtplot(df, chr = 'chr', pos = 'pos', p = args.p, nVars = var_count, file = args.out, bicolor = args.bicolor)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--bicolor', action='store_true', help='flag indicates that plot should be bicolor')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--full-results', help='a full results file name', required=True)
	requiredArgs.add_argument('--results', help='a plottable results file name', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
