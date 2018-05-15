import argparse
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Strategy}"); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy}".encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy-Table-Cohorts}".encode('utf-8')); f.write("\n")

		cohort_tbl=[
				r"\begin{table}[H]",
				r"	\caption{Cohorts available for analysis}",
				r"	\begin{center}",
				r"	\begin{tabular}{rccc}",
				r"		\toprule",
				r"		\textbf{ID} & \textbf{Array} & \textbf{Ancestry} & \textbf{Report}\\",
				r"		\midrule"]
		i = 0
		for c in args.cohorts:
			i = i + 1
			if i != len(args.cohorts):
				lineEnd=r"\\*"
			else:
				lineEnd=r"\\"
			cohort_tbl.extend([
				r"			" + " & ".join(c.split(',')).replace("_","\_") + lineEnd])
		cohort_tbl.extend([
				r"		\bottomrule",
				r"	\end{tabular}",
				r"	\end{center}",
				r"	\label{table:Strategy-Table-Cohorts}",
				r"\end{table}"])
		f.write("\n"); f.write("\n".join(cohort_tbl).encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy-Table-Metas}".encode('utf-8')); f.write("\n")

		if len(args.metas) > 0:
			metas = {}
			for m in args.metas:
				ms = m.split(",")
				if not ms[0] in metas:
					metas[ms[0]] = {}
				if not ms[1] in metas[ms[0]]:
					metas[ms[0]][ms[1]] = {}
				metas[ms[0]][ms[1]]['kin'] = ms[2]
				metas[ms[0]][ms[1]]['report'] = ms[3]

			meta_tbl=[
							r"\begin{table}[H]",
							r"	\caption{Meta analyses}",
							r"	\begin{center}",
							r"	\begin{tabular}{rccc}",
							r"		\toprule",
							r"		\textbf{ID} & \textbf{Cohort} & \textbf{KinshipRemove} & \textbf{Report}\\",
							r"		\midrule"]

			for m in metas:
				i = 0
				for c in metas[m]:
					i = i + 1
					if i != len(metas[m].keys()):
						lineEnd=r"\\*"
					else:
						lineEnd=r"\\"
					with open(metas[m][c]['kin']) as kin:
						kinshipRemoved = kin.read().splitlines()
					if i == 1:
						meta_tbl.extend([
							r"			" + m.replace("_","\_") + " & " + c.replace("_","\_") + " & " + str(len(kinshipRemoved)) + " & " + metas[m][c]['report'] + lineEnd])
					else:
						meta_tbl.extend([
							r"			" + " & " + c.replace("_","\_") + " & " + str(len(kinshipRemoved)) + " & " + lineEnd])
			meta_tbl.extend([
							r"		\bottomrule",
							r"	\end{tabular}",
							r"	\end{center}",
							r"	\label{table:Strategy-Table-Metas}",
							r"\end{table}"])
			f.write("\n"); f.write("\n".join(meta_tbl).encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy-Table-Merges}".encode('utf-8')); f.write("\n")

		if len(args.merges) > 0:
			merge_tbl=[
					r"\begin{table}[H]",
					r"	\caption{Merged results}",
					r"	\begin{center}",
					r"	\begin{tabular}{rcc}",
					r"		\toprule",
					r"		\textbf{ID} & \textbf{Cohorts/Metas} & \textbf{Report}\\",
					r"		\midrule"]
			i = 0
			for m in args.merges:
				i = i + 1
				if i != len(args.merges):
					lineEnd=r"\\*"
				else:
					lineEnd=r"\\"
				merge_tbl.extend([
					r"			" + " & ".join(m.split(',')).replace("_","\_") + lineEnd])
			merge_tbl.extend([
					r"		\bottomrule",
					r"	\end{tabular}",
					r"	\end{center}",
					r"	\label{table:Strategy-Table-Merges}",
					r"\end{table}"])
			f.write("\n"); f.write("\n".join(merge_tbl).encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		f.write("\n".join(["",r"%<*Strategy>","%</Strategy>"]).encode('utf-8')); f.write("\n")
		f.write("\n".join(["",r"%<*Strategy-Table-Cohorts>","%</Strategy-Table-Cohorts>"]).encode('utf-8')); f.write("\n")
		f.write("\n".join(["",r"%<*Strategy-Table-Metas>","%</Strategy-Table-Metas>"]).encode('utf-8')); f.write("\n")
		f.write("\n".join(["",r"%<*Strategy-Table-Merges>","%</Strategy-Table-Merges>"]).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--cohorts', nargs='*', help='a list of cohort attributes in the config each separated by comma', required=True)
	requiredArgs.add_argument('--metas', nargs='*', help='a list of meta attributes in the config and kinship removed files each separated by comma', required=True)
	requiredArgs.add_argument('--merges', nargs='*', help='a list of merge attributes in the config each separated by comma', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
