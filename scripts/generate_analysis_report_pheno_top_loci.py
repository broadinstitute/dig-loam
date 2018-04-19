import argparse
import numpy as np
import pandas as pd
import collections

def main(args=None):

	top = collections.OrderedDict()
	for s in args.top_results.split(","):
		ss = s.split("___")
		if len(ss) > 4:
			ss[1] = " ".join([ss[0],ss[1]])
			del ss[0]
		if ss[2] == "":
			ss[2] = "unadj"
		else:
			ss[2] = "adj " + ss[2]
		if ss[1] != "":
			id = ss[0] + " " + ss[1] + " " + ss[2]
		else:
			id = ss[0] + " " + ss[2]
		top[id] = ss[3]

	reg = collections.OrderedDict()
	if args.regionals != "":
		for s in args.regionals.split(","):
			ss = s.split("___")
			if len(ss) > 5:
				ss[1] = " ".join([ss[0],ss[1]])
				del ss[0]
			if ss[2] == "":
				ss[2] = "unadj"
			else:
				ss[2] = "adj " + ss[2]
			if ss[1] != "":
				id = ss[0] + " " + ss[1] + " " + ss[2]
			else:
				id = ss[0] + " " + ss[2]
			reg[id] = {}
			reg[id]['sigregions'] = ss[3]
			reg[id]['regplots'] = ss[4]

	result_cols = ['chr', 'pos', 'id', 'alt', 'ref', 'gene', 'cohort', 'dir', 'n', 'male', 'female', 'case', 'ctrl', 'mac', 'af', 'afavg', 'afmin', 'afmax', 'beta', 'se', 'sigmaG2', 'or', 'tstat', 'zstat', 'chi2', 'zscore', 'pval']
	report_cols = ['CHR', 'POS', 'ID', 'EA', 'OA', r"GENE\textsubscript{CLOSEST}", 'COHORT', 'DIR', 'N', 'MALE', 'FEMALE', 'CASE', 'CTRL', 'MAC', 'FREQ', 'FREQ\textsubscript{AVG}', 'FREQ\textsubscript{MIN}', 'FREQ\textsubscript{MAX}','EFFECT', 'STDERR', 'SIGMAG2', 'OR', 'T', 'Z', 'CHI2', 'ZSCORE', 'P']
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
			text.extend([
						r"\begin{table}[H]",
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
					text.extend([
						r"         columns/" + c + r"/.style={column name=" + cols[c] + r", " + types[c] + r"},"])
				else:
					text.extend([
						r"         columns/" + c + r"/.style={column name=" + cols[c] + r"},"])
			text.extend([
						r"         postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
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
				try:
					sigdf = pd.read_table(reg[model]['sigregions'], header=None, sep="\t")
				except pd.errors.EmptyDataError:
					pass
				else:
					if sigdf.shape[0] > 0:
						if sigdf.shape[0] > 1:
							text.extend([
									r"\begin{figure}[h!]",
									r"   \centering"])
							for idx, row in sigdf.iterrows():
								nplots = nplots + 1
								delim = r"\\" if nplots % 2 == 0 else r"%"
								text.extend([
									r"   \begin{subfigure}{.5\textwidth}",
									r"      \centering",
									r"      \includegraphics[width=\linewidth,page=" + nplots + r"]{" + reg[model]['regplots'] + r"}",
									r"      \caption{" + row[3] + r" $\pm 100 kb$}",
									r"      \label{fig:regPlot" + model.replace(" ","") + "_" + row[3] + r"}",
									r"   \end{subfigure}" + delim])
							text.extend([
									r"   \caption{Regional plots for model " + model.replace(r'_',r'\_') + r"}",
									r"   \label{fig:regPlots" + model.replace(" ","") + r"}",
									r"\end{figure}"])
						else:
							text.extend([
									r"\begin{figure}[h!]",
									r"   \centering",
									r"   \includegraphics[width=.5\linewidth,page=1]{" + reg[model]['regplots'] + "}",
									r"   \caption{Regional plot for model " + model.replace(r'_',r'\_') + r": " + sigdf[3][0] + r" $\pm 100 kb$}",
									r"   \label{fig:regPlot" + model.replace(" ","") + "_" + sigdf[3][0] + "}",
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
