import argparse
import numpy as np
import pandas as pd
import re

def main(args=None):

	print args

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Data}"); f.write("\n")

		array_tbl=[
			r"\begin{table}[H]",
			r"	\caption{Genotype array information}",
			r"	\begin{center}",
			r"	\begin{tabular}{rlclc}",
			r"		\toprule",
			r"		\textbf{ID} & \textbf{Filename} & \textbf{Format} & \textbf{LiftOver} & \textbf{Report}\\",
			r"		\midrule"]
		for a in args.arrays:
			array_tbl.extend([
			r"		" + " & ".join([re.sub(r"\bNA\b", "N/A", s).replace("_","\_") for s in a.split(',')]) + r" \\"])
		array_tbl.extend([
			r"		\bottomrule",
			r"	\end{tabular}",
			r"	\end{center}",
			r"	\label{table:genotypeArrayInformation}",
			r"\end{table}"])
		f.write("\n"); f.write("\n".join(array_tbl).encode('utf-8')); f.write("\n")

		text=[
			r"\begin{figure}[H]",
			r"\centering",
			r"\includegraphics[width=0.75\linewidth,page=1]{" + args.samples_upset_diagram + r"}",
			r"\caption{Samples remaining for analysis after quality control}",
			r"\label{fig:samplesUpsetDiagram}",
			r"\end{figure}"]
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text=[
			r"\begin{figure}[H]",
			r"\centering",
			r"\includegraphics[width=0.75\linewidth,page=1]{" + args.variants_upset_diagram + r"}",
			r"\caption{Variants remaining for analysis after quality control}",
			r"\label{fig:variantsUpsetDiagram}",
			r"\end{figure}"]
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text = r"\ExecuteMetaData[\currfilebase.input]{data}"
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		text = ["",r"%<*data>","%</data>"]
		f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--samples-upset-diagram', help='an upset diagram for samples', required=True)
	requiredArgs.add_argument('--variants-upset-diagram', help='an upset diagram for harmonized variants', required=True)
	requiredArgs.add_argument('--arrays', nargs='+', help='a list of each arrays comma delimited attributes from the config file', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
