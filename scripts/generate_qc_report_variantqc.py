import argparse
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing variantqc section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Variant QC}"); f.write("\n")

		var_list = args.variant_exclusions.split(",")
		if len(var_list) > 1:
			i = 0
			for a in var_list:
				i = i + 1
				l = a.split("___")[0]
				m = a.split("___")[1]
				with open(m) as mo:
					failed = mo.read().splitlines()
				if i == 1:
					text1 = "{0:,d}".format(len(failed)) + " " + l.replace("_","\_") + " variants"
				elif i < len(var_list)-1:
					text1 = text1 + ", " + "{0:,d}".format(len(failed)) + " " + l.replace("_","\_") + " variants"
				else:
					if len(var_list) == 2:
						text1 = text1 + " and " + "{0:,d}".format(len(failed)) + " " + l.replace("_","\_") + " variants"
					else:
						text1 = text1 + ", and " + "{0:,d}".format(len(failed)) + " " + l.replace("_","\_") + " variants"
		else:
			l = args.variant_exclusions.split("___")[0]
			m = args.variant_exclusions.split("___")[1]
			with open(m) as mo:
				failed = mo.read().splitlines()
			text1 = "{0:,d}".format(len(failed)) + " variants"

		text=r"""Variant quality was assessed using call rate and Hardy Weinberg equilibrium (HWE). We calculate HWE using controls only within any of 4 major ancestral populations; EUR, AFR, SAS and EAS. There must have been at least 100 samples in a population to trigger a filter. This conservative approach minimizes the influence from admixture in other population groups. This procedure resulted in flagging {0} for removal.""".format(text1)

		if args.variants_upset_diagram is not None:
			text = text + """ Figure \ref{{fig:variantsRemaining}} shows the number of variants remaining for analysis after applying filters."""

		else:
			bim=pd.read_table(args.bim.split("___")[1], low_memory=False, header=None)
			text = text + """ After applying variant filters, there were {0:,d} variants remaining for analysis.""".format(bim.shape[0])

		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		if args.variants_upset_diagram is not None:
			text=r"""\begin{figure}[H]
					\centering
					\includegraphics[width=0.75\linewidth,page=1]{""" + args.variants_upset_diagram + r"""}
					\caption{Variants remaining for analysis}
					\label{fig:variantsRemaining}
				\end{figure}"""
			f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--variants-upset-diagram', help='an upset diagram for variants remaining')
	requiredArgs.add_argument('--bim', help='a bim file')
	requiredArgs.add_argument('--variant-exclusions', help='a comma separated list of labels and variant exclusion files, each separated by 3 underscores', required=True)
	args = parser.parse_args()
	main(args)
