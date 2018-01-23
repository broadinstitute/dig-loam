import argparse
import numpy as np
import pandas as pd

def main(args=None):

	ancestry = pd.read_table(args.ancestry, sep="\t")
	ancestry.columns = ['ID','POP']

	sex = pd.read_table(args.pheno_master, sep="\t")
	sex = sex[[args.id_col,args.sex_col]]

	pheno_files = {}
	for s in args.pheno.split(","):
		if not s.split("___")[0] in pheno_files:
			pheno_files[s.split("___")[0]] = {}
		pheno_files[s.split("___")[0]][s.split("___")[1]] = pd.read_table(s.split("___")[2],sep="\t")
		pheno_files[s.split("___")[0]][s.split("___")[1]] = pheno_files[s.split("___")[0]][s.split("___")[1]].merge(ancestry)
		if not args.sex_col in pheno_files[s.split("___")[0]][s.split("___")[1]].columns:
			pheno_files[s.split("___")[0]][s.split("___")[1]] = pheno_files[s.split("___")[0]][s.split("___")[1]].merge(sex)

	sample_files = {}
	for s in args.samples_include.split(","):
		if not s.split("___")[0] in sample_files:
			sample_files[s.split("___")[0]] = {}
		sample_files[s.split("___")[0]][s.split("___")[1]] = s.split("___")[2]

	pc_files = {}
	for s in args.samples_include.split(","):
		if not s.split("___")[0] in pc_files:
			pc_files[s.split("___")[0]] = {}
		pc_files[s.split("___")[0]][s.split("___")[1]] = s.split("___")[2]

	dist_plots = {}
	for s in args.dist_plot.split(","):
		dist_plots[s.split("___")[0]] = s.split("___")[1]

	strat_dist_plots = {}
	for s in args.strat_dist_plot.split(","):
		strat_dist_plots[s.split("___")[0]] = s.split("___")[1]

	## generate sample table header
	cols=['Array','Model','Ancestry','N','Male','Female']
	dichotomous = True if len(pheno_files[pheno_files.keys()[0]][pheno_files[pheno_files.keys()[0]].keys()[0]][args.pheno_name].value_counts()) == 2 else False
	if dichotomous:
		cols.extend(['Case','Ctrl'])
	else:
		cols.extend(['Max','Min','Mean','Median','StdDev'])

	sample_table = [r"\begin{table}[h!]",r"   \footnotesize",r"   \caption{" + args.pheno_long_name + " summarized by array, ancestry, and model}",r"   \centering",r"   \label{table:samplesTable" + args.pheno_name + r"}",r"   \begin{adjustbox}{max width=\textwidth}",r"      \begin{tabular}{" + 'c'*(len(cols)) + "}",r"         \toprule"]
	sample_table.extend([r"         " + ' & '.join([r"\textbf{" + x.replace('Mean','\\bm{$\\mu$}').replace('Median','\\bm{$\\tilde{x}$}').replace('StdDev','\\bm{$\\sigma$}') + r"}" for x in cols]) + r" \\"])
	sample_table.extend([r"         \midrule"])
	i = 0
	for arr in pheno_files:
		i = i + 1
		if i % 2 != 0:
			color = r"         \rowcolor{Gray}"
		else:
			color = r"         \rowcolor{white}"
		sample_table.extend([color,"         " + arr + ' & {} '*(len(cols)-1) + r" \\"])
		sample_table.extend([color,"         {} " + ' & {} '*(len(cols)-1) + r" \\"])
		j = 0
		for model in pheno_files[arr]:
			j = j + 1
			k = 0
			for pop in pheno_files[arr][model]['POP'].unique().tolist():
				k = k + 1
				df_temp = pheno_files[arr][model][pheno_files[arr][model]['POP'] == pop]
				row = []
				row.extend([df_temp.shape[0]])
				row.extend([df_temp[df_temp[args.sex_col].isin([1,"M","m","Male","male"])].shape[0]])
				row.extend([df_temp[df_temp[args.sex_col].isin([2,"F","f","Female","female"])].shape[0]])
				if dichotomous:
					row.extend([df_temp[df_temp[args.pheno_name] == 1].shape[0]])
					row.extend([df_temp[df_temp[args.pheno_name] == 0].shape[0]])
				else:
					row.extend([round(np.max(df_temp[args.pheno_name]),3)])
					row.extend([round(np.min(df_temp[args.pheno_name]),3)])
					row.extend([round(np.mean(df_temp[args.pheno_name]),3)])
					row.extend([round(np.median(df_temp[args.pheno_name]),3)])
					row.extend([round(np.std(df_temp[args.pheno_name]),3)])
				if k == 1:
					sample_table.extend([color,"         {} & " + model.replace("_","\_") + " & " + pop + " & " + " & ".join([str(r) for r in row]) + r" \\"])
				else:
					sample_table.extend([color,"         {} & {} & " + pop + " & " + " & ".join([str(r) for r in row]) + r" \\"])
			row = []
			row.extend([pheno_files[arr][model].shape[0]])
			row.extend([pheno_files[arr][model][pheno_files[arr][model][args.sex_col].isin([1,"M","m","Male","male"])].shape[0]])
			row.extend([pheno_files[arr][model][pheno_files[arr][model][args.sex_col].isin([2,"F","f","Female","female"])].shape[0]])
			if dichotomous:
				row.extend([pheno_files[arr][model][pheno_files[arr][model][args.pheno_name] == 1].shape[0]])
				row.extend([pheno_files[arr][model][pheno_files[arr][model][args.pheno_name] == 0].shape[0]])
			else:
				row.extend([round(np.max(pheno_files[arr][model][args.pheno_name]),3)])
				row.extend([round(np.min(pheno_files[arr][model][args.pheno_name]),3)])
				row.extend([round(np.mean(pheno_files[arr][model][args.pheno_name]),3)])
				row.extend([round(np.median(pheno_files[arr][model][args.pheno_name]),3)])
				row.extend([round(np.std(pheno_files[arr][model][args.pheno_name]),3)])
			sample_table.extend([color,r"         {} & {} & \textit{ALL} & " + " & ".join([r"\textit{" + str(r) + r"}" for r in row]) + r" \\"])
			if j < len(pheno_files[arr].keys()):
				sample_table.extend([color,"         {} " + ' & {} '*(len(cols)-1) + r" \\"])
	sample_table.extend([r"         \bottomrule",r"      \end{tabular}",r"   \end{adjustbox}",r"   \\[10pt]",r"   \caption*{\textit{italic} = all populations}",r"\end{table}"])

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{" + args.pheno_long_name + r"}"); f.write("\n")
		f.write("\n"); f.write(r"\subsection{Summary}"); f.write("\n")

		text = r"\ExecuteMetaData[\currfilebase.input]{" + args.pheno_long_name.replace(" ","-") + r"-summary}"
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		n = 0
		text = [r"\begin{figure}[H]",
				r"   \centering"]
		for a in dist_plots:
			n = n + 1
			delim = r"\\" if n % 2 == 0 else r"%"
			text.extend([r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth]{" + dist_plots[a] + r"}",
				r"      \caption{" + a + r"}",
				r"      \label{fig:distPlot" + args.pheno_name + a + r"}",
				r"   \end{subfigure}" + delim])
		text.extend([r"   \caption{Distribution of " + args.pheno_name.replace("_","\_") + r"}",
			r"   \label{fig:distPlots" + args.pheno_name + r"}",
			r"\end{figure}"])

		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		n = 0
		text = [r"\begin{figure}[H]",
				r"   \centering"]
		for a in strat_dist_plots:
			n = n + 1
			delim = r"\\" if n % 2 == 0 else r"%"
			text.extend([r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth]{" + strat_dist_plots[a] + r"}",
				r"      \caption{" + a + r"}",
				r"      \label{fig:stratDistPlot" + args.pheno_name + a + r"}",
				r"   \end{subfigure}" + delim])
		text.extend([r"   \caption{Distribution of " + args.pheno_name.replace("_","\_") + r" stratified by ancestry}",
			r"   \label{fig:stratDistPlots" + args.pheno_name + r"}",
			r"\end{figure}"])

		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		f.write("\n"); f.write("\n".join(sample_table).encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		text = ["",r"%<*" + args.pheno_long_name.replace(" ","-") + r"-summary>","%</" + args.pheno_long_name.replace(" ","-") + r"-summary>"]
		f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--dist-plot', help='a comma separated list of array labels and phenotype distribution plots, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--strat-dist-plot', help='a comma separated list of array labels and ancestry stratified phenotype distribution plots, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pheno-master', help='a master phenotype file', required=True)
	requiredArgs.add_argument('--id-col', help='a column name for sample id in phenotype master file', required=True)
	requiredArgs.add_argument('--sex-col', help='a column name for sample sex in phenotype master file', required=True)
	requiredArgs.add_argument('--pheno', help='a comma separated list of array labels, model names, and phenotype files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pheno-name', help='a column name for phenotype', required=True)
	requiredArgs.add_argument('--pheno-long-name', help='a full name for phenotype (used in section titles)', required=True)
	requiredArgs.add_argument('--ancestry', help='an ancestry file', required=True)
	requiredArgs.add_argument('--samples-include', help='a comma separated list of array labels, model names, and sample include files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pcs-include', help='a comma separated list of array labels, model names, and PC include files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
