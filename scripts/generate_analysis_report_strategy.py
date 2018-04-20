import argparse
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Strategy}"); f.write("\n")

		cohort_tbl=[
				r"\begin{table}[H]",
				r"	\caption{Cohorts available for analysis}",
				r"	\begin{center}",
				r"	\begin{ThreePartTable}",
				r"		\begin{tabular}{rccc}",
				r"			\toprule",
				r"			\textbf{ID} & \textbf{Array} & \textbf{Ancestry} & \textbf{Report} \\",
				r"			\midrule"]
		for c in args.cohorts.split(","):
			cohort_tbl.extend([
				r"			" + " & ".join(c.split('___')).replace("_","\_") + r" \\"])
		cohort_tbl.extend([
				r"			\bottomrule",
				r"		\end{tabular}",
				r"	\end{ThreePartTable}",
				r"	\end{center}",
				r"	\label{table:cohortsAvailable}",
				r"\end{table}"])
		f.write("\n"); f.write("\n".join(cohort_tbl).encode('utf-8')); f.write("\n")

		if args.metas != "":
			metas = {}
			for m in args.metas.split(","):
				ms = m.split("___")
				if not (ms[0],ms[1]) in metas:
					metas[(ms[0],ms[1])] = {}
				if not ms[2] in metas[(ms[0],ms[1])]:
					metas[(ms[0],ms[1])][ms[2]] = ms[3]

			meta_tbl=[
							r"\begin{table}[H]",
							r"	\caption{Meta analyses}",
							r"	\begin{center}",
							r"	\begin{ThreePartTable}",
							r"		\begin{tabular}{rccc}",
							r"			\toprule",
							r"			\textbf{ID} & \textbf{Report} & \textbf{Cohort} & \textbf{KinshipRemove} \\",
							r"			\midrule"]
			for m in metas:
				i = 0
				for c in metas[m]:
					i = i + 1
					print metas[m][c]
					with open(metas[m][c]) as kin:
						kinshipRemoved = kin.read().splitlines()
					if i == 1:
						meta_tbl.extend([
							r"			" + m[0].replace("_","\_") + " & " + m[1].replace("_","\_") + " & " + c.replace("_","\_") + " & " + str(len(kinshipRemoved)) + r" \\"])
					else:
						meta_tbl.extend([
							r"			" + " & & " + c.replace("_","\_") + " & " + str(len(kinshipRemoved)) + r" \\"])
			meta_tbl.extend([
							r"			\bottomrule",
							r"		\end{tabular}",
							r"	\end{ThreePartTable}",
							r"	\end{center}",
							r"	\label{table:metaAnalyses}",
							r"\end{table}"])
			f.write("\n"); f.write("\n".join(meta_tbl).encode('utf-8')); f.write("\n")

		if args.merges != "":
			merge_tbl=[
					r"\begin{table}[H]",
					r"	\caption{Merged results}",
					r"	\begin{center}",
					r"	\begin{ThreePartTable}",
					r"		\begin{tabular}{rcc}",
					r"			\toprule",
					r"			\textbf{ID} & \textbf{Cohorts/Metas} & \textbf{Report} \\",
					r"			\midrule"]
			for m in args.merges.split(","):
				merge_tbl.extend([
					r"			" + " & ".join(m.split('___')).replace("_","\_") + r" \\"])
			merge_tbl.extend([
					r"			\bottomrule",
					r"		\end{tabular}",
					r"	\end{ThreePartTable}",
					r"	\end{center}",
					r"	\label{table:mergedResults}",
					r"\end{table}"])
			f.write("\n"); f.write("\n".join(merge_tbl).encode('utf-8')); f.write("\n")

		text = r"\ExecuteMetaData[\currfilebase.input]{strategy}"
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		text = ["",r"%<*strategy>","%</strategy>"]
		f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--cohorts', help='a comma delimited string representing all cohort attributes in the config each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--metas', help='a comma delimited string representing all meta attributes in the config and kinship removed files each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--merges', help='a comma delimited string representing all merge attributes in the config each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
