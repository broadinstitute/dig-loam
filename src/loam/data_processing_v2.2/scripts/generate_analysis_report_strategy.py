import argparse
import numpy as np
import pandas as pd
import collections

def main(args=None):

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Strategy}"); f.write("\n")
		f.write(r"\label{Strategy}"); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy}".encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsection{Sample structure and pipeline}"); f.write("\n")
		f.write(r"\label{Sample-structure-and-pipeline}"); f.write("\n")

		text=r"The strategy we used to perform association testing can be found below. The 'ID' columns are the names used to identify each set of association test results in this document. The 'Report' columns indicate whether or not that particular set of association results will be presented in the tables and plots of the proceeding sections."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy-Sample-structure-and-pipeline}".encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Cohort-level analysis}"); f.write("\n")
		f.write(r"\label{Cohort-level-analysis}"); f.write("\n")

		text=r"In Table \ref{table:Strategy-Table-Cohorts}, all of the cohorts available for analysis are defined. Each cohort was defined by a single array and one or more ancestral populations."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy-Cohort-level-analysis}".encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy-Table-Cohorts}".encode('utf-8')); f.write("\n")

		cohort_tbl=[
				r"\begin{table}[H]",
				r"	\caption{Cohort-level analysis}",
				r"	\begin{center}",
				r"	\begin{tabular}{rccc}",
				r"		\toprule",
				r"		\textbf{ID} & \textbf{Array} & \textbf{Ancestry} & \textbf{Report}\\",
				r"		\midrule"]
		i = 0
		for c in args.cohorts.split("___"):
			i = i + 1
			if i != len(args.cohorts.split("___")):
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

		if len(args.metas.split("___")) > 0:

			f.write("\n"); f.write(r"\subsubsection{Meta-analysis}"); f.write("\n")
			f.write(r"\label{Meta-analysis}"); f.write("\n")

			text=r"Table \ref{table:Strategy-Table-Metas} defines any meta-analyses performed on the cohorts. Each cohort that was included is detailed along with the number of samples removed prior to cohort-level association testing. In order to identify samples that needed to be removed due to relatedness across cohorts, the cohorts genotypes were first merged on common variants. Then, autosomal variants with $MAF >= 0.01$ and $callrate >= 0.98$ were extracted and kinship values were calculated using King \cite{king} with the '--kinship' flag. The reference cohort, the first one listed, maintained all of its samples. Starting from the last listed cohort, any samples shown to have some relation ($kinship >= 0.0884$) to a sample from any preceeding cohort was removed. This was continued until all cohorts subsequent to the reference cohort had been processed."
			f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

			f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy-Meta-analysis}".encode('utf-8')); f.write("\n")

			f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy-Table-Metas}".encode('utf-8')); f.write("\n")

			metas = collections.OrderedDict()
			for m in args.metas.split("___"):
				ms = m.split(",")
				if not ms[0] in metas:
					metas[ms[0]] = collections.OrderedDict()
				if not ms[1] in metas[ms[0]]:
					metas[ms[0]][ms[1]] = collections.OrderedDict()
				metas[ms[0]][ms[1]]['kin'] = ms[2]
				metas[ms[0]][ms[1]]['report'] = ms[3]

			meta_tbl=[
							r"\begin{table}[H]",
							r"	\caption{Meta-analysis}",
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
							r"			" + m.replace("_","\_") + " & & & " + metas[m][c]['report'] + lineEnd])
					meta_tbl.extend([
						r"			" + " & " + c.replace("_","\_") + " & " + str(len(kinshipRemoved)) + " & " + lineEnd])
			meta_tbl.extend([
							r"		\bottomrule",
							r"	\end{tabular}",
							r"	\end{center}",
							r"	\label{table:Strategy-Table-Metas}",
							r"\end{table}"])
			f.write("\n"); f.write("\n".join(meta_tbl).encode('utf-8')); f.write("\n")

		if len(args.merges.split("___")) > 0:

			f.write("\n"); f.write(r"\subsubsection{Merged results}"); f.write("\n")
			f.write(r"\label{Merged-results}"); f.write("\n")

			text=r"In order to present results in a comprehensive way, we identified a single reference set of results as the default and merged in results from other arrays where either the variant failed to provide a $p$-value or did not exist in the reference set. Table \ref{table:Strategy-Table-Merges} describes the merges performed. The '>' symbol in the 'Cohorts/Metas' column implies the strategy used to combine the results. The left-most results set was kept as reference, while variants from the following set were merged in where applicable. This merge was repeated (ie. additively) for all sets listed from left to right."
			f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

			f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy-Merged-results}".encode('utf-8')); f.write("\n")

			f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy-Table-Merges}".encode('utf-8')); f.write("\n")

			merge_tbl=[
					r"\begin{table}[H]",
					r"	\caption{Merged results}",
					r"	\begin{center}",
					r"	\begin{tabular}{rcc}",
					r"		\toprule",
					r"		\textbf{ID} & \textbf{Cohorts/Metas} & \textbf{Report}\\",
					r"		\midrule"]
			i = 0
			for m in args.merges.split("___"):
				i = i + 1
				if i != len(args.merges.split("___")):
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

		f.write("\n"); f.write(r"\subsection{Ancestry Adjustment and Outlier Removal}"); f.write("\n")
		f.write(r"\label{Ancestry-Adjustment-and-Outlier-Removal}"); f.write("\n")

		text=r"Adjusting the statistical models for underlying ancestry is often crucial to reduce or eliminate Type 1 error. Often analysts include principal components of ancestry as covariates in their models as a matter of convention. In our case, we undertook a more nuanced approach. First, the top ten PC's were calculated for each cohort using the PC-AiR method \cite{pcair}. Then, the phenotype of interest was regressed on the covariates to be used in the model and all of the PC's. If the $N$th PC exhibited a statistically significant $p$-value ($p <= 0.05$), we selected PC's $1-N$ to be included in association testing. Once determined, any sample lying outside $6$ standard deviations from the mean on any of the $N$ PC's was marked as an outlier and removed from the sample set. This process was repeated up to a maximum of ten times until no outliers were found, resulting in more homogeneous sample sets for each particular analysis."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Strategy-Ancestry-Adjustment-and-Outlier-Removal}".encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		f.write("\n".join(["",r"%<*Strategy>","%</Strategy>"]).encode('utf-8')); f.write("\n")
		f.write("\n".join(["",r"%<*Strategy-Table-Cohorts>","%</Strategy-Table-Cohorts>"]).encode('utf-8')); f.write("\n")
		f.write("\n".join(["",r"%<*Strategy-Table-Metas>","%</Strategy-Table-Metas>"]).encode('utf-8')); f.write("\n")
		f.write("\n".join(["",r"%<*Strategy-Table-Merges>","%</Strategy-Table-Merges>"]).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--cohorts', help='a three underscore delimited list of cohort attributes in the config each separated by comma', required=True)
	requiredArgs.add_argument('--metas', help='a three underscore delimited list of meta attributes in the config and kinship removed files each separated by comma', required=True)
	requiredArgs.add_argument('--merges', help='a three underscore delimited list of merge attributes in the config each separated by comma', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
