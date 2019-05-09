import argparse
import numpy as np
import pandas as pd
import collections

def main(args=None):

	ancestry = pd.read_table(args.ancestry, sep="\t")
	ancestry.columns = [args.id_col,'POP']

	sex = pd.read_table(args.sample_file, sep="\t")
	sex = sex[[args.id_col,args.sex_col]]

	model_files = collections.OrderedDict()
	for s in args.model_files:
		ss = s.split(",")
		if len(ss) > 7:
			ss[1] = " ".join([ss[0],ss[1]])
			del ss[0]
		if not (ss[0],ss[1],ss[2]) in model_files:
			model_files[(ss[0],ss[1],ss[2])] = collections.OrderedDict()
		if not (ss[3],ss[4]) in model_files[(ss[0],ss[1],ss[2])]:
			model_files[(ss[0],ss[1],ss[2])][(ss[3],ss[4])] = collections.OrderedDict()
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
			r"\begin{table}[H]",
			r"	\footnotesize",
			r"	\caption{Samples with " + args.pheno_long_name.replace("_","\_") + " data summarized by cohort, transformation, and run-time adjustments}",
			r"	\begin{center}",
			r"	\resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%",
			r"	\begin{tabular}{rrrrr" + 'c'*(len(cols)-5) + "}",
			r"		\toprule"]
	sample_table.extend([
			r"		" + ' & '.join([r"\textbf{" + x.replace('Mean','\\bm{$\\mu$}').replace('Median','\\bm{$\\tilde{x}$}').replace('StdDev','\\bm{$\\sigma$}') + r"}" for x in cols]) + r"\\"])
	sample_table.extend([
			r"		\midrule"])
	i = 0
	for cohort in model_files.keys():
		i = i + 1
		if i % 2 != 0:
			color = r"		\rowcolor{Gray}"
		else:
			color = r"		\rowcolor{white}"
		j = 0
		for model in model_files[cohort].keys():
			j = j + 1
			df_temp = model_files[cohort][model]['pheno'][model_files[cohort][model]['pheno']['POP'].isin(cohort[2].split("+"))]
			row = []
			row.extend([str(model_files[cohort][model]['pcs'])])
			row.extend([df_temp.shape[0]])
			row.extend([df_temp[df_temp[args.sex_col].astype(str) == args.male_code].shape[0]])
			row.extend([df_temp[df_temp[args.sex_col].astype(str) == args.female_code].shape[0]])
			if dichotomous:
				row.extend([df_temp[df_temp[args.pheno_name] == 1].shape[0]])
				row.extend([df_temp[df_temp[args.pheno_name] == 0].shape[0]])
			else:
				row.extend([round(np.max(df_temp[args.pheno_name]),3)])
				row.extend([round(np.min(df_temp[args.pheno_name]),3)])
				row.extend([round(np.mean(df_temp[args.pheno_name]),3)])
				row.extend([round(np.median(df_temp[args.pheno_name]),3)])
				row.extend([round(np.std(df_temp[args.pheno_name]),3)])
			if j < len(model_files[cohort].keys()):
				lineEnd = r"\\*"
			else:
				lineEnd = r"\\"
			if j == 1:
				sample_table.extend([color + "		" + cohort[0].replace("_","\_") + " & " + cohort[1].replace("_","\_") + " & " + cohort[2].replace("_","\_") + " & " + model[0].replace("_","\_") + " & " + model[1].replace("_","\_") + " & " + " & ".join([str(r).replace("_","\_") for r in row]) + lineEnd])
			else:
				sample_table.extend([color + "		{} & {} & {} & " + model[0].replace("_","\_") + " & " + model[1].replace("_","\_") + " & " + " & ".join([str(r).replace("_","\_") for r in row]) + lineEnd])
	sample_table.extend([
			r"		\bottomrule",
			r"	\end{tabular}}",
			r"	\end{center}",
			r"	\label{table:" + args.pheno_name.replace("_","-") + r"-Summary-Table}",
			r"\end{table}"])


	dist_plots = collections.OrderedDict()
	for s in args.dist_plot:
		if not s.split(",")[0] in dist_plots:
			dist_plots[s.split(",")[0]]  = collections.OrderedDict()
		if len(s.split(",")) > 2:
			dist_plots[s.split(",")[0]][s.split(",")[1]] = s.split(",")[2]
		else:
			dist_plots[s.split(",")[0]][s.split(",")[0]] = s.split(",")[1]

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{" + args.pheno_long_name.replace("_","\_") + " (" + args.pheno_name.replace("_","\_") + r")}"); f.write("\n")
		f.write(r"\label{" + args.pheno_name.replace("_","-") + r"}"); f.write("\n")

		f.write("\n"); f.write(r"\subsection{Summary}"); f.write("\n")
		f.write(r"\label{" + args.pheno_name.replace("_","-") + r"-Summary}"); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{" + args.pheno_name.replace("_","-") + r"-Summary}".encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{" + args.pheno_name.replace("_","-") + r"-Summary-Distributions}".encode('utf-8')); f.write("\n")

		text = []
		j = 0
		for c in dist_plots:
			j = j + 1
			k = 0
			if c == "":
				figcap = r"Distribution of " + args.pheno_name.replace("_","\_") + r" in cohort-level analyses"
			else:
				figcap = r"Distribution of " + args.pheno_name.replace("_","\_") + r" in " + c.replace("_","\_") + r" by cohort"
			text.extend([
							r"\begin{figure}[H]",
							r"   \centering"])
			for d in dist_plots[c]:
				k = k + 1
				if k % 2 != 0:
					delim = r"%"
					if k > 1:
						text.extend([
							r"\end{figure}",
							r"\begin{figure}[H]\ContinuedFloat",
							r"   \centering"])
				else:
					delim = ""
				text.extend([
							r"   \begin{subfigure}{.5\textwidth}",
							r"      \centering",
							r"      \includegraphics[width=\linewidth,page=1]{" + dist_plots[c][d] + r"}",
							r"      \caption{" + d.replace("_","\_") + r"}",
							r"      \label{fig:" + args.pheno_name.replace("_","-") + c.replace("_","-") + d.replace("_","-") + r"-Distribution}",
							r"   \end{subfigure}" + delim])
				if k == len(dist_plots[c]):
					text.extend([
							r"   \caption{" + figcap + r"}",
							r"   \label{fig:" + args.pheno_name.replace("_","-") + c.replace("_","-") + r"-Distributions}",
							r"\end{figure}"])

		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{" + args.pheno_name.replace("_","-") + r"-Summary-Table}".encode('utf-8')); f.write("\n")

		f.write("\n"); f.write("\n".join(sample_table).encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		f.write("\n".join(["",r"%<*" + args.pheno_name.replace("_","-") + r"-Summary>","%</" + args.pheno_name.replace("_","-") + r"-Summary>"]).encode('utf-8')); f.write("\n")

		f.write("\n".join(["",r"%<*" + args.pheno_name.replace("_","-") + r"-Summary-Distributions>","%</" + args.pheno_name.replace("_","-") + r"-Summary-Distributions>"]).encode('utf-8')); f.write("\n")

		f.write("\n".join(["",r"%<*" + args.pheno_name.replace("_","-") + r"-Summary-Table>","%</" + args.pheno_name.replace("_","-") + r"-Summary-Table>"]).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--dist-plot', nargs='+', help='a list of cohort labels and phenotype distribution plots, each separated by comma', required=True)
	requiredArgs.add_argument('--sample-file', help='a master phenotype file', required=True)
	requiredArgs.add_argument('--id-col', help='a column name for sample id in phenotype master file', required=True)
	requiredArgs.add_argument('--sex-col', help='a column name for sample sex in phenotype master file', required=True)
	requiredArgs.add_argument('--male-code', help='a column name for sample sex male code in phenotype master file', required=True)
	requiredArgs.add_argument('--female-code', help='a column name for sample sex female code in phenotype master file', required=True)
	requiredArgs.add_argument('--model-files', nargs='+', help='a list of cohort labels, model names, and phenotype files, each separated by comma', required=True)
	requiredArgs.add_argument('--pheno-name', help='a column name for phenotype', required=True)
	requiredArgs.add_argument('--pheno-long-name', help='a full name for phenotype (used in section titles)', required=True)
	requiredArgs.add_argument('--ancestry', help='an ancestry file', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
