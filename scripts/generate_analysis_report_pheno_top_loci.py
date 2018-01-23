import argparse
import numpy as np
import pandas as pd
import collections

def main(args=None):

	top = collections.OrderedDict()
	for s in args.top_results.split(","):
		top[s.split("___")[0]] = s.split("___")[1]

	reg = collections.OrderedDict()
	if args.regionals != "":
		for s in args.regionals.split(","):
			reg[s.split("___")[0]] = s.split("___")[1].split(";")

	result_cols = ['chr', 'pos', 'id', 'alt', 'ref', 'gene', 'cohort', 'dir', 'n', 'case', 'ctrl', 'mac', 'af', 'beta', 'se', 'sigmaG2', 'or', 'tstat', 'zstat', 'chi2', 'pval']
	report_cols = ['CHR', 'POS', 'ID', 'EA', 'OA', r"GENE\textsubscript{CLOSEST}", 'COHORT', 'DIR', 'N', 'CASE', 'CTRL', 'MAC', 'FREQ', 'EFFECT', 'STDERR', 'SIGMAG2', 'OR', 'T', 'Z', 'CHI2', 'P']
	cols = dict(zip(result_cols,report_cols))
	types = {'chr': 'string type', 'pos': 'string type', 'id': 'string type', 'alt': 'verb string type', 'ref': 'verb string type', 'gene': 'string type', 'cohort': 'verb string type', 'dir': 'verb string type'}

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing top associations section"
		f.write("\n"); f.write(r"\subsection{Top associations}"); f.write("\n")

		for model in top:

			# read in top results
			df = pd.read_table(top[model],sep="\t")
			df = df[[c for c in result_cols if c in df.columns]]

			text = []
			text.extend([r"\begin{table}[H]",
				r"   \begin{center}",
				r"   \caption{Top variants in " + model.replace(r'_',r'\_') + r" (\textbf{bold} variants indicate previously identified associations)}",
				r"   \resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%",
				r"      \pgfplotstabletypeset[",
				r"         font=\footnotesize,",
				r"         col sep=tab,",
				r"         columns={" + ",".join(df.columns.tolist()) + r"},",
				r"         column type={>{\fontseries{bx}\selectfont}c},"])
			for c in df.columns.tolist():
				if c in types:
					text.extend([r"         columns/" + c + r"/.style={column name=" + cols[c] + r", " + types[c] + r"},"])
				else:
					text.extend([r"         columns/" + c + r"/.style={column name=" + cols[c] + r"},"])
			text.extend([r"         postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
				r"         every head row/.style={before row={\toprule}, after row={\midrule}},",
				r"         every last row/.style={after row=\bottomrule}",
				r"         ]{" + top[model] + r"}}",
				r"   \label{table:topLoci" + args.pheno_name + r"}",
				r"   \end{center}",
				r"\end{table}"])
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

			if model in reg:
				text = []
				nplots = 0
				if len(reg[model]) > 1:
					text.extend([r"\begin{figure}[h!]",
							r"   \centering"])
					for r in reg[model]:
						v = r.split("regplot_")[1].split(".")[0].split("_")[0]
						nplots = nplots + 1
						delim = r"\\" if nplots % 2 == 0 else r"%"
						text.extend([r"   \begin{subfigure}{.5\textwidth}",
							r"      \centering",
							r"      \includegraphics[width=\linewidth]{" + r + r"}",
							r"      \caption{" + v + r" $\pm 100 kb$}",
							r"      \label{fig:regPlot" + model + "_" + v + r"}",
							r"   \end{subfigure}" + delim])
					text.extend([r"   \caption{Regional plots for model " + model.replace(r'_',r'\_') + r"}",r"   \label{fig:regPlots" + model + r"}",r"\end{figure}"])
				else:
					v = reg[model][0].split("regplot_")[1].split(".")[0].split("_")[0]
					text.extend([r"\begin{figure}[h!]",
							r"   \centering",
							r"   \includegraphics[width=.5\linewidth]{" + reg[model][0] + "}",
							r"   \caption{Regional plot for model " + model.replace(r'_',r'\_') + r": " + v + r" $\pm 100 kb$}",
							r"   \label{fig:regPlot" + model + "_" + v + "}",
							r"\end{figure}"])
				f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text = r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_long_name.replace(" ","-") + r"-top-associations}"
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		text = ["",r"%<*"  + args.pheno_long_name.replace(" ","-") + r"-top-associations>","%</"  + args.pheno_long_name.replace(" ","-") + r"-top-associations>"]
		f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--top-results', help='a comma separated list of models and top results files (aligned to risk allele and gene annotated), each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--regionals', help='a comma separated list of models and regional mht plots (list of mht plots separated by semicolon), each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pheno-name', help='a column name for phenotype', required=True)
	requiredArgs.add_argument('--pheno-long-name', help='a full phenotype name', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
