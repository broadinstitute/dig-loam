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

		text_dict = collections.OrderedDict()
		for x in args.variant_exclusions:
			with open(x.split(",")[1]) as mo:
				failed = mo.read().splitlines()
			if len(failed) > 0:
				text_dict[x.split(",")[0]] = "{0:,d}".format(len(failed))

		if len(text_dict) == 0:
			text1 = "no variants"
		if len(text_dict) == 1:
			text1 = text_dict[text_dict.keys()[0]] + " variants"
		if len(text_dict) == 2:
			text1 = " and ".join([str(text_dict[x]) + " " + x.replace("_","\_") for x in text_dict.keys()[0:len(text_dict.keys())]]) + " variants"
		elif len(text_dict) > 2:
			text1 = ", ".join([str(text_dict[x]) + " " + x.replace("_","\_") for x in text_dict.keys()[0:(len(text_dict.keys())-1)]]) + " and " + str(text_dict[text_dict.keys()[len(text_dict.keys())-1]]) + " " + text_dict.keys()[len(text_dict.keys())-1].replace("_","\_") + " variants"

		text=r"Variant quality was assessed using call rate and Hardy Weinberg equilibrium (HWE). We calculate HWE p-values within any of 4 major ancestral populations; EUR, AFR, SAS and EAS. There must have been at least 100 samples in a population to trigger a filter. This conservative approach minimizes the influence from admixture in other population groups. This procedure resulted in flagging {0} for removal.".format(text1)

		if args.variants_upset_diagram is not None:
			text = text + r" Figure \ref{fig:variantsRemaining} shows the number of variants remaining for analysis after applying filters."

		else:
			bim=pd.read_table(args.bim.split(",")[1], low_memory=False, header=None)
			text = text + r" After applying variant filters, there were {0:,d} variants remaining for analysis.".format(bim.shape[0])

		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

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
	requiredArgs.add_argument('--bim', help='a bim file')
	requiredArgs.add_argument('--variant-exclusions', nargs='+', help='a list of labels and variant exclusion files, each separated by comma', required=True)
	args = parser.parse_args()
	main(args)
