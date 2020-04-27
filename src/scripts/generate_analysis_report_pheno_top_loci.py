import argparse
import numpy as np
import pandas as pd
import collections

def main(args=None):

	top = collections.OrderedDict()
	for s in args.top_results:
		ss = s.split(",")
		if len(ss) > 4:
			ss[1] = " ".join([ss[0],ss[1]])
			del ss[0]
		if ss[2] == "":
			ss[2] = "Unadjusted"
		else:
			ss[2] = "Adjusted " + ss[2]
		if ss[1] != "":
			id = ss[1] + " " + ss[2]
		else:
			id = ss[2]
		if not ss[0] in top:
			top[ss[0]] = collections.OrderedDict()
		top[ss[0]][id] = ss[3]

	reg = collections.OrderedDict()
	if args.regionals != "":
		for s in args.regionals:
			ss = s.split(",")
			if len(ss) > 5:
				ss[1] = " ".join([ss[0],ss[1]])
				del ss[0]
			if ss[2] == "":
				ss[2] = "Unadjusted"
			else:
				ss[2] = "Adjusted " + ss[2]
			if ss[1] != "":
				id = ss[1] + " " + ss[2]
			else:
				id = ss[2]
			if not ss[0] in reg:
				reg[ss[0]] = collections.OrderedDict()
			reg[ss[0]][id] = {}
			reg[ss[0]][id]['sigregions'] = ss[3]
			reg[ss[0]][id]['regplots'] = ss[4]

	result_cols = ['chr', 'pos', 'id', 'alt', 'ref', 'gene', 'cohort', 'dir', 'n', 'male', 'female', 'case', 'ctrl', 'mac', 'af', 'afavg', 'afmin', 'afmax', 'beta', 'se', 'sigmaG2', 'or', 'tstat', 'zstat', 'chi2', 'zscore', 'pval']
	report_cols = ['CHR', 'POS', 'ID', 'EA', 'OA', 'GENE\\textsubscript{CLOSEST}', 'COHORT', 'DIR', 'N', 'MALE', 'FEMALE', 'CASE', 'CTRL', 'MAC', 'FREQ', 'FREQ\\textsubscript{AVG}', 'FREQ\\textsubscript{MIN}', 'FREQ\\textsubscript{MAX}','EFFECT', 'STDERR', 'SIGMAG2', 'OR', 'T', 'Z', 'CHI2', 'ZSCORE', 'P']
	cols = dict(zip(result_cols,report_cols))
	types = {
		'chr': 'string type',
		'pos': 'string type',
		'id': 'verb string type',
		'alt': 'verb string type',
		'ref': 'verb string type',
		'gene': 'verb string type',
		'cohort': 'verb string type',
		'dir': 'verb string type',
		'mac': 'precision = 3, sci precision = 2',
		'af': 'precision = 3, sci precision = 2',
		'afavg': 'precision = 3, sci precision = 2',
		'afmin': 'precision = 3, sci precision = 2',
		'afmax': 'precision = 3, sci precision = 2',
		'beta': 'precision = 3, sci precision = 2',
		'se': 'precision = 3, sci precision = 2',
		'sigmaG2': 'precision = 3, sci precision = 2',
		'or': 'precision = 3, sci precision = 2',
		'tstat': 'precision = 3, sci precision = 2',
		'zstat': 'precision = 3, sci precision = 2',
		'chi2': 'precision = 3, sci precision = 2',
		'zscore': 'precision = 3, sci precision = 2',
		'pval': 'precision = 3, sci precision = 2'
	}

	## open latex file for writing
	with open(args.out_input,'w') as fin:
		with open(args.out_tex,'w') as f:
	
			print "writing top associations section"
			f.write("\n"); f.write(r"\subsection{Top associations}"); f.write("\n")
			f.write(r"\label{" + args.pheno_name.replace("_","-") + r"-Top-associations}"); f.write("\n")
	
			f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Top-Associations}".encode('utf-8')); f.write("\n")

			fin.write("\n"); fin.write("\n".join([r"%<*"  + args.pheno_name.replace("_","-") + r"-Top-Associations>","%</"  + args.pheno_name.replace("_","-") + r"-Top-Associations>"]).encode('utf-8')); fin.write("\n")
	
			for cohort in top:
	
				f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Top-Associations-Table-" + cohort.replace("_","-") + r"}".encode('utf-8')); f.write("\n")

				fin.write("\n"); fin.write("\n".join([r"%<*"  + args.pheno_name.replace("_","-") + r"-Top-Associations-Table-" + cohort.replace("_","-") + r">","%</"  + args.pheno_name.replace("_","-") + r"-Top-Associations-Table-" + cohort.replace("_","-") + r">"]).encode('utf-8')); fin.write("\n")
	
				for model in top[cohort]:
	
					f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Top-Associations-Table-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r"}".encode('utf-8')); f.write("\n")

					fin.write("\n"); fin.write("\n".join([r"%<*"  + args.pheno_name.replace("_","-") + r"-Top-Associations-Table-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r">", "%</"  + args.pheno_name.replace("_","-") + r"-Top-Associations-Table-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r">"]).encode('utf-8')); fin.write("\n")
	
					# read in top results
					df = pd.read_table(top[cohort][model],sep="\t")
					df = df[[c for c in result_cols if c in df.columns]]
	
					text = []
					text.extend([
								r"\begin{table}[H]",
								r"	\begin{center}",
								r"	\caption{Top variants in the " + cohort.replace("_","\_") + " " + model.replace("_","\_") + " model" + r" (\textbf{bold} variants indicate previously identified associations)}",
								r"	\label{table:" + args.pheno_name.replace("_","-") + r"-Top-Associations-Table-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r"}",
								r"	\resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%",
								r"	\pgfplotstabletypeset[",
								r"		font=\footnotesize,",
								r"		col sep=tab,",
								r"		columns={" + ",".join(df.columns.tolist()) + r"},",
								r"		column type={>{\fontseries{bx}\selectfont}c},"])
					for c in df.columns.tolist():
						if c in types:
							text.extend([
								r"		columns/" + c + r"/.style={column name=" + cols[c] + r", " + types[c] + r"},"])
						else:
							text.extend([
								r"		columns/" + c + r"/.style={column name=" + cols[c] + r"},"])
					text.extend([
								r"		postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
								r"		every head row/.append style={",
								r"			before row=",
								r"				\toprule,",
								r"			after row=",
								r"				\midrule",
								r"		},",
								r"		every last row/.style={",
								r"			after row=",
								r"				\bottomrule",
								r"		},",
								r"		empty cells with={}",
								r"         ]{" + top[cohort][model] + r"}}",
								r"	\end{center}",
								r"\end{table}"])
					f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")
	
					if cohort in reg:
						if model in reg[cohort]:
							text = []
							nplots = 0
							try:
								sigdf = pd.read_table(reg[cohort][model]['sigregions'], header=None, sep="\t")
							except pd.errors.EmptyDataError:
								pass
							else:
								if sigdf.shape[0] > 0:
	
									f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Top-Associations-Regional-Plots-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r"}".encode('utf-8')); f.write("\n")

									fin.write("\n"); fin.write("\n".join([r"%<*"  + args.pheno_name.replace("_","-") + r"-Top-Associations-Regional-Plots-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r">", "%</"  + args.pheno_name.replace("_","-") + r"-Top-Associations-Regional-Plots-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r">"]).encode('utf-8')); fin.write("\n")
	
									if sigdf.shape[0] > 1:
										text.extend([
												r"\begin{figure}[H]",
												r"   \centering"])
										for idx, row in sigdf.iterrows():
											nplots = nplots + 1
											delim = r"\\" if nplots % 2 == 0 else r"%"
											if nplots % 7 == 0:
												text.extend([
													r"\begin{figure}[H]\ContinuedFloat",
													r"   \centering"])
											text.extend([
												r"   \begin{subfigure}{.5\textwidth}",
												r"      \centering",
												r"      \includegraphics[width=\linewidth,page=" + str(nplots) + r"]{" + reg[cohort][model]['regplots'] + r"}",
												r"      \caption{" + row[3].replace("_","\_") + r" $\pm 100 kb$}",
												r"      \label{fig:" + args.pheno_name.replace("_","-") + r"-Top-Associations-Regional-Plots-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + "-" + row[3].replace("_","-") + r"}",
												r"   \end{subfigure}" + delim])
											if nplots % 6 == 0 and idx < sigdf.shape[0] - 1:
												if nplots == 6:
													text.extend([
														r"   \caption{Regional plots for cohort " + cohort.replace("_","\_") + " model " + model.replace("_","\_") + r" (Continued on next page)}",
														r"\end{figure}"])
												else:
													text.extend([
														r"   \caption{Regional plots for cohort " + cohort.replace("_","\_") + " model " + model.replace("_","\_") + r" (Continued)}",
														r"\end{figure}"])
										if nplots < 7:
											text.extend([
												r"   \caption{Regional plots for cohort " + cohort.replace("_","\_") + " model " + model.replace("_","\_") + r"}"])
										else:
											text.extend([
												r"   \caption{Regional plots for cohort " + cohort.replace("_","\_") + " model " + model.replace("_","\_") + r" (Continued)}"])
										text.extend([
											r"   \label{fig:" + args.pheno_name.replace("_","-") + r"-Top-Associations-Regional-Plots-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r"}",
											r"\end{figure}"])
									else:
										text.extend([
												r"\begin{figure}[H]",
												r"   \centering",
												r"   \includegraphics[width=.5\linewidth,page=1]{" + reg[cohort][model]['regplots'] + "}",
												r"   \caption{Regional plot for cohort " + cohort.replace("_","\_") + " model " + model.replace("_","\_") + r": " + sigdf[3][0].replace("_","\_") + r" $\pm 100 kb$}",
												r"   \label{fig:" + args.pheno_name.replace("_","-") + r"-Top-Associations-Regional-Plots-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + "}",
												r"\end{figure}"])
									f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--top-results', nargs='+', help='a comma separated list of models and top results files (aligned to risk allele and gene annotated), each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--regionals', nargs='+', help='a comma separated list of models and regional mht plots (list of mht plots separated by semicolon), each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pheno-name', help='a column name for phenotype', required=True)
	requiredArgs.add_argument('--pheno-long-name', help='a full phenotype name', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
