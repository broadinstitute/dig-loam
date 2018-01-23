import argparse
import numpy as np
import pandas as pd

def main(args=None):

	qq_plots = {}
	for s in args.qq_plots.split(","):
		qq_plots[s.split("___")[0]] = s.split("___")[1]

	mht_plots = {}
	for s in args.mht_plots.split(","):
		mht_plots[s.split("___")[0]] = s.split("___")[1]

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing calibration section"
		f.write("\n"); f.write(r"\subsection{Calibration}"); f.write("\n")

		n = 0
		text = [r"\begin{figure}[H]",
				r"   \centering"]
		for a in qq_plots:
			n = n + 1
			delim = r"\\" if n % 2 == 0 else r"%"
			text.extend([r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth]{" + qq_plots[a] + r"}",
				r"      \caption{" + a.replace("_","\_") + r"}",
				r"      \label{fig:qqPlot" + a + r"}",
				r"   \end{subfigure}" + delim])
		text.extend([r"   \caption{QQ plots for " + args.pheno_name.replace("_","\_") + r"}",
			r"   \label{fig:qqPlots" + args.pheno_name + r"}",
			r"\end{figure}"])
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		n = 0
		text = [r"\begin{figure}[H]",
				r"   \centering"]
		for a in mht_plots:
			n = n + 1
			delim = r"\\"
			text.extend([r"   \begin{subfigure}{\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth]{" + mht_plots[a] + r"}",
				r"      \caption{" + a.replace("_","\_") + r"}",
				r"      \label{fig:mhtPlot" + a + r"}",
				r"   \end{subfigure}" + delim])
		text.extend([r"   \caption{Manhattan plots for " + args.pheno_name.replace("_","\_") + r"}",
			r"   \label{fig:mhtPlots" + args.pheno_name + r"}",
			r"\end{figure}"])
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text = r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_long_name.replace(" ","-") + r"-calibration}"
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		text = ["",r"%<*"  + args.pheno_long_name.replace(" ","-") + r"-calibration>","%</"  + args.pheno_long_name.replace(" ","-") + r"-calibration>"]
		f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--qq-plots', help='a comma separated list of array labels, model names, and qq plots, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--mht-plots', help='a comma separated list of array labels, model names, and manhattan plots, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pheno-name', help='a column name for phenotype', required=True)
	requiredArgs.add_argument('--pheno-long-name', help='a full phenotype name', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
