import argparse
import numpy as np
import pandas as pd
import collections

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing variantqc section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Variant QC}"); f.write("\n")

		filters_dict = collections.OrderedDict()
		for x in args.postqc_variant_filters:
			with open(x.split(",")[1]) as mo:
				filters_dict[x.split(",")[0]] = mo.readlines()

		text_dict = collections.OrderedDict()
		for x in args.variant_exclusions:
			with open(x.split(",")[1]) as mo:
				failed = [l.split("\t")[2] for l in mo.read().splitlines()]
				text_dict[x.split(",")[0]] = failed

		#if len(text_dict) == 1:
		#	text1 = str("{0:,d}".format(len(text_dict[text_dict.keys()[0]]))) + " variants"
		#if len(text_dict) == 2:
		#	text1 = " and ".join(["{0:,d}".format(len(text_dict[x])) + " " + x.replace("_","\_") for x in text_dict.keys()[0:len(text_dict.keys())]]) + " variants"
		#elif len(text_dict) > 2:
		#	text1 = ", ".join(["{0:,d}".format(len(text_dict[x])) + " " + x.replace("_","\_") for x in text_dict.keys()[0:(len(text_dict.keys())-1)]]) + " and " + "{0:,d}".format(len(text_dict[text_dict.keys()[len(text_dict.keys())-1]])) + " " + text_dict.keys()[len(text_dict.keys())-1].replace("_","\_") + " variants"
		#
		#text=r"Prior to association resulted in flagging {0} for removal.".format(text1)

		n = 0
		if args.variants_upset_diagram is None:
			for x in args.bim:
				print "processing bim file " + x
				bim_tmp=pd.read_table(x.split(",")[1], low_memory=False, header=None)
				n = n + bim_tmp[~(bim_tmp[1].isin(text_dict[x.split(",")[0]]))].shape[0]

		if len(filters_dict.values()) == 0:
			text=r"In order to allow for downstream fine tuning, no filters were applied to the data prior to generating analysis ready files, leaving {1} variants remaining for analysis.".format(str(n))
		else:
			if len(filters_dict.keys()) == 1:
				text=r"In order to remove problematic variants, {0} filters were applied prior to generating analysis ready files, leaving {1} variants remaining for analysis. The results of each filter are summarized in Table \ref{{table:variantsExcluded}}.".format(str(len(filters_dict.values())), str(n))
			else:
				x=[k for k in filters_dict.keys() if len(filters_dict[k]) > 0]
				if len(x) == 1:
					x_f = x[0]
				elif len(x) ==2:
					x_f = " and ".join(x)
				else:
					x_f = ", ".join([k for k in x[0:(len(x)-1)]]) + " and " + x[len(x)-1]
				text=r"In order to remove problematic variants, {0} filters were applied to {1} prior to generating analysis ready files. The results of each filter are summarized in Table \ref{{table:variantsExcluded}} and the final variant counts are described in Figure \ref{{fig:variantsRemaining}}.".format(str(len(x)), x_f)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		with open(args.variants_exclude_table) as vexcl:

			rows = 0
			for i, row in enumerate(vexcl):
				rows = rows + 1
				if rows == 1:
					header=row.rstrip().split("\t")

			if rows > 0:

				filters=[x for x in header if x not in ["Batch","MAF","Total"]]
				filters_test=set([x.strip() for y in filters_dict.values() for x in y])
	
				text=[
					r"\needlines{25}",
					r"\begin{ThreePartTable}",
					r"	\begin{TableNotes}",
					r"		\footnotesize"]
				for v in filters_test:
					text = text + [r"		\item \textbf{" + v.split("\t")[0].replace("_","\_") + r"}: " + v.split("\t")[2].replace("_","\_")]
				text = text + [
					r"	\end{TableNotes}",
					r"	\pgfplotstabletypeset[",
					r"		begin table=\begin{longtable},",
					r"		end table=\end{longtable},",
					r"		font=\tiny\sffamily,",
					r"		string replace={NA}{},",
					r"		col sep=tab,",
					r"		columns={Batch,MAF," + 	','.join(filters) + r",Total},",
					r"		column type={>{\fontseries{bx}\selectfont}c},",
					r"		columns/Batch/.style={column name=, string type},",
					r"		columns/MAF/.style={column name=MAF, string type, column type={>{\fontseries{bx}\selectfont}r}},"]
				for filt in filters:
					text = text + [r"		columns/" + filt + r"/.style={column name=" + filt + r", string type},"]
				text = text + [r"		columns/Total/.style={column name=Total, string type, column type={>{\fontseries{bx}\selectfont}r}},"]
				text = text + [
					r"		postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
					r"		every head row/.append style={",
					r"			before row=",
					r"				\caption{Summary of variants excluded by frequency and filter}\label{table:variantsExcluded}\\",
					r"				\insertTableNotes\\\\",
					r"				\toprule,",
					r"			after row=",
					r"				\midrule",
					r"				\endfirsthead",
					r"		},",
					r"		every first row/.append style={",
					r"			before row=",
					r"				\multicolumn{" + str(len(filters_test)) + r"}{c}{}\\ \caption[]{... Continued from previous page}\\",
					r"				\toprule,",
					r"			after row=",
					r"				\midrule",
					r"				\endhead",
					r"				\midrule",
					r"				\multicolumn{" + str(len(filters_test)) + r"}{r}{Continued on next page ...}\\",
					r"				\bottomrule",
					r"				\endfoot",
					r"				\endlastfoot",
					r"		},"]
				text.extend([r"		every row no " + str(row) + r"/.append style={after row=*}," for row in xrange(1,rows-2) if row % 9 != 0])
				for i in xrange(2,rows-1,18):
					text.extend([r"		grayrow=" + str(row) + r"," for row in xrange(i,i+9)])
				text.extend([
						r"		every last row/.style={",
						r"			after row=",
						r"				\bottomrule",
						r"		},",
						r"		empty cells with={}",
						r"	]{" + args.variants_exclude_table + r"}",
						r"\end{ThreePartTable}"])
				f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		if args.variants_upset_diagram is not None:
			text=[
				r"\begin{figure}[H]",
				r"	\centering",
				r"	\includegraphics[width=0.75\linewidth,page=1]{" + args.variants_upset_diagram + r"}",
				r"	\caption{Variants remaining for analysis}",
				r"	\label{fig:variantsRemaining}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--variants-upset-diagram', help='an upset diagram for variants remaining')
	requiredArgs.add_argument('--bim', nargs='+', help='a list of labels and bim files, each separated by a comma')
	requiredArgs.add_argument('--variant-exclusions', nargs='+', help='a list of labels and variant exclusion files, each separated by comma', required=True)
	requiredArgs.add_argument('--postqc-variant-filters', nargs='+', help='a list of labels and post qc variant filter files, each separated by comma', required=True)
	requiredArgs.add_argument('--variants-exclude-table', help='a variant exclusion summary table', required=True)
	args = parser.parse_args()
	main(args)
