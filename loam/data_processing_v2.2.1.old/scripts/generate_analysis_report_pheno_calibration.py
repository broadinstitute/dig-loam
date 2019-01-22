import argparse
import numpy as np
import pandas as pd
import collections

def main(args=None):

	qq_plots = collections.OrderedDict()
	for s in args.qq_plots:
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
		if not ss[0] in qq_plots:
			qq_plots[ss[0]] = collections.OrderedDict()
		qq_plots[ss[0]][id] = ss[3]

	mht_plots = collections.OrderedDict()
	for s in args.mht_plots:
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
		if not ss[0] in mht_plots:
			mht_plots[ss[0]] = collections.OrderedDict()
		mht_plots[ss[0]][id] = ss[3]

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing calibration section"
		f.write("\n"); f.write(r"\subsection{Calibration}"); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Calibration}".encode('utf-8')); f.write("\n")

		for cohort in qq_plots:

			n = 0
			text = [
					r"\begin{figure}[H]",
					r"   \centering"]

			f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Calibration-QQ-" + cohort.replace("_","-") + r"}".encode('utf-8')); f.write("\n")

			for model in qq_plots[cohort]:

				f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Calibration-QQ-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r"}".encode('utf-8')); f.write("\n")

				n = n + 1
				delim = r"\\" if n % 2 == 0 else r"%"
				text.extend([
					r"   \begin{subfigure}{.5\textwidth}",
					r"      \centering",
					r"      \includegraphics[width=\linewidth]{" + qq_plots[cohort][model] + r"}",
					r"      \caption{" + model.replace("_","\_") + r"}",
					r"      \label{fig:" + args.pheno_name.replace("_","-") + r"-Calibration-QQ-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r"}",
					r"   \end{subfigure}" + delim])
			text.extend([
					r"   \caption{QQ plots for " + args.pheno_name.replace("_","\_") + r" in the " + cohort.replace("_","\_") + r" analysis}",
					r"   \label{fig:" + args.pheno_name.replace("_","-") + r"-Calibration-QQ-" + cohort.replace("_","-") + r"}",
					r"\end{figure}"])
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		for cohort in mht_plots:

			n = 0
			text = [
					r"\begin{figure}[H]",
					r"   \centering"]

			f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Calibration-Manhattan-" + cohort.replace("_","-") + r"}".encode('utf-8')); f.write("\n")

			for model in mht_plots[cohort]:

				text_input = r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Calibration-Manhattan-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r"}"
				f.write("\n"); f.write(text_input.encode('utf-8')); f.write("\n")

				n = n + 1
				delim = r"\\"
				text.extend([
					r"   \begin{subfigure}{\textwidth}",
					r"      \centering",
					r"      \includegraphics[width=\linewidth]{" + mht_plots[cohort][model] + r"}",
					r"      \caption{" + model.replace("_","\_") + r"}",
					r"      \label{fig:" + args.pheno_name.replace("_","-") + r"-Calibration-Manhattan-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r"}",
					r"   \end{subfigure}" + delim])
			text.extend([
					r"   \caption{Manhattan plots for " + args.pheno_name.replace("_","\_") + r" in the " + cohort.replace("_","\_") + r" analysis}",
					r"   \label{fig:" + args.pheno_name.replace("_","-") + r"-Calibration-Manhattan-" + cohort.replace("_","-") + r"}",
					r"\end{figure}"])
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		f.write("\n"); f.write("\n".join([r"%<*" + args.pheno_name.replace("_","-") + r"-Calibration>","%</"  + args.pheno_name.replace("_","-") + r"-Calibration>"]).encode('utf-8')); f.write("\n")

		for cohort in qq_plots:

			f.write("\n"); f.write("\n".join([r"%<*" + args.pheno_name.replace("_","-") + r"-Calibration-QQ-" + cohort.replace("_","-") + r">","%</"  + args.pheno_name.replace("_","-") + r"-Calibration-QQ-" + cohort.replace("_","-") + r">"]).encode('utf-8')); f.write("\n")

			for model in qq_plots[cohort]:

				f.write("\n"); f.write("\n".join([r"%<*" + args.pheno_name.replace("_","-") + r"-Calibration-QQ-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r">","%</"  + args.pheno_name.replace("_","-") + r"-Calibration-QQ-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r">"]).encode('utf-8')); f.write("\n")

		for cohort in mht_plots:

			f.write("\n"); f.write("\n".join([r"%<*" + args.pheno_name.replace("_","-") + r"-Calibration-Manhattan-" + cohort.replace("_","-") + r">","%</"  + args.pheno_name.replace("_","-") + r"-Calibration-Manhattan-" + cohort.replace("_","-") + r">"]).encode('utf-8')); f.write("\n")

			for model in mht_plots[cohort]:

				f.write("\n"); f.write("\n".join([r"%<*" + args.pheno_name.replace("_","-") + r"-Calibration-Manhattan-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r">","%</"  + args.pheno_name.replace("_","-") + r"-Calibration-Manhattan-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r">"]).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--qq-plots', nargs='+', help='a list of cohort labels, model names, and qq plots, each separated by commas', required=True)
	requiredArgs.add_argument('--mht-plots', nargs='+', help='a list of cohort labels, model names, and manhattan plots, each separated by commas', required=True)
	requiredArgs.add_argument('--pheno-name', help='a column name for phenotype', required=True)
	requiredArgs.add_argument('--pheno-long-name', help='a full phenotype name', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
