import argparse
import numpy as np
import pandas as pd

def main(args=None):

	ancestry = pd.read_table(args.ancestry, sep="\t")
	ancestry.columns = ['ID','POP']

	sex = pd.read_table(args.pheno_master, sep="\t")
	sex = sex[[args.id_col,args.sex_col]]

	model_files = {}
	for s in args.model_files.split(","):
		ss = s.split("___")
		if len(ss) > 7:
			ss[1] = " ".join([ss[0],ss[1]])
			del ss[0]
		if not (ss[0],ss[1],ss[2]) in model_files:
			model_files[(ss[0],ss[1],ss[2])] = {}
		if not (ss[3],ss[4]) in model_files[(ss[0],ss[1],ss[2])]:
			model_files[(ss[0],ss[1],ss[2])][(ss[3],ss[4])] = {}
		with open(ss[5]) as f:
			model_files[(ss[0],ss[1],ss[2])][(ss[3],ss[4])]['pcs'] = len(f.read().splitlines())
		model_files[(ss[0],ss[1],ss[2])][(ss[3],ss[4])]['pheno'] = pd.read_table(ss[6],sep="\t")
		model_files[(ss[0],ss[1],ss[2])][(ss[3],ss[4])]['pheno'] = model_files[(ss[0],ss[1],ss[2])][(ss[3],ss[4])]['pheno'].merge(ancestry)
		if not args.sex_col in model_files[(ss[0],ss[1],ss[2])][(ss[3],ss[4])]['pheno'].columns:
			model_files[(ss[0],ss[1],ss[2])][(ss[3],ss[4])]['pheno'] = model_files[(ss[0],ss[1],ss[2])][(ss[3],ss[4])]['pheno'].merge(sex)

	# cohort.id + array.id + array.ancestry + model.trans + model.covars + model.pcs
	# META_EX EXBROAD EUR invn Age     3
	#                     invn Age+SEX 4
	# META_EX EXCIDR  EUR invn Age     2
	#                     invn Age+SEX 1

	## generate sample table header
	cols=['Cohort','Array','Ancestry','Trans','Covars','PCs','N','Male','Female']
	dichotomous = True if len(model_files[model_files.keys()[0]][model_files[model_files.keys()[0]].keys()[0]]['pheno'][args.pheno_name].value_counts()) == 2 else False
	if dichotomous:
		cols.extend(['Case','Ctrl'])
	else:
		cols.extend(['Max','Min','Mean','Median','StdDev'])

	sample_table = [
			r"\begin{ThreePartTable}[H]",
			r"	\footnotesize",
			r"	\caption{" + args.pheno_long_name + " summarized by cohort and adjustments}",
			r"	\centering",
			r"	\label{table:samplesTable" + args.pheno_name + r"}",
			r"	\begin{tabular}{rrrrr" + 'c'*(len(cols)-5) + "}",
			r"		\toprule"]
	sample_table.extend([
			r"		" + ' & '.join([r"\textbf{" + x.replace('Mean','\\bm{$\\mu$}').replace('Median','\\bm{$\\tilde{x}$}').replace('StdDev','\\bm{$\\sigma$}') + r"}" for x in cols]).replace("_","\_") + r" \\"])
	sample_table.extend([
			r"		\midrule"])
	i = 0
	for cohort in model_files.keys():
		i = i + 1
		if i % 2 != 0:
			color = r"		\rowcolor{Gray}"
		else:
			color = r"		\rowcolor{white}"
		sample_table.extend([color,"		" + cohort[0].replace("_","\_") + " & " + cohort[1].replace("_","\_") + " & " + cohort[2].replace("_","\_") + ' & {} '*(len(cols)-3) + r" \\"])
		sample_table.extend([color,"		{} " + ' & {} '*(len(cols)-1) + r" \\"])
		j = 0
		for model in model_files[cohort].keys():
			j = j + 1
			df_temp = model_files[cohort][model]['pheno'][model_files[cohort][model]['pheno']['POP'].isin(cohort[2].split("+"))]
			row = []
			row.extend([str(model_files[cohort][model]['pcs'])])
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
			if j == 1:
				sample_table.extend([color,"		" + cohort[0].replace("_","\_") + " & " + cohort[1].replace("_","\_") + " & " + cohort[2].replace("_","\_") + " & " + model[0].replace("_","\_") + " & " + model[1].replace("_","\_") + " & " + " & ".join([str(r) for r in row]) + r" \\"])
			else:
				sample_table.extend([color,"		{} & {} & {} & " + model[0].replace("_","\_") + " & " + model[1].replace("_","\_") + " & " + " & ".join([str(r) for r in row]) + r" \\"])
	sample_table.extend([
			r"		\bottomrule",
			r"	\end{tabular}",
			r"\end{ThreePartTable}"])


	dist_plots = {}
	for s in args.dist_plot.split(","):
		if len(s.split("___")) > 2:
			dist_plots[" ".join(s.split("___")[0].replace("_","\_"),s.split("___")[1].replace("_","\_"))] = s.split("___")[2]
		else:
			dist_plots[s.split("___")[0].replace("_","\_")] = s.split("___")[1]

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{" + args.pheno_long_name + r"}"); f.write("\n")
		f.write("\n"); f.write(r"\subsection{Summary}"); f.write("\n")

		text = r"\ExecuteMetaData[\currfilebase.input]{" + args.pheno_long_name.replace(" ","-") + r"-summary}"
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		n = 0
		text = [
				r"\begin{figure}[H]",
				r"   \centering"]
		for c in dist_plots:
			n = n + 1
			delim = r"\\" if n % 2 == 0 else r"%"
			text.extend([
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth,page=1]{" + dist_plots[c] + r"}",
				r"      \caption{" + " ".join(c) + r"}",
				r"      \label{fig:distPlot" + args.pheno_name + "".join(c) + r"}",
				r"   \end{subfigure}" + delim])
		text.extend([
				r"   \caption{Distribution of " + args.pheno_name.replace("_","\_") + r"}",
				r"   \label{fig:distPlots" + args.pheno_name + r"}",
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
	requiredArgs.add_argument('--dist-plot', help='a comma separated list of cohort labels and phenotype distribution plots, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pheno-master', help='a master phenotype file', required=True)
	requiredArgs.add_argument('--id-col', help='a column name for sample id in phenotype master file', required=True)
	requiredArgs.add_argument('--sex-col', help='a column name for sample sex in phenotype master file', required=True)
	requiredArgs.add_argument('--model-files', help='a comma separated list of cohort labels, model names, and phenotype files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pheno-name', help='a column name for phenotype', required=True)
	requiredArgs.add_argument('--pheno-long-name', help='a full name for phenotype (used in section titles)', required=True)
	requiredArgs.add_argument('--ancestry', help='an ancestry file', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
