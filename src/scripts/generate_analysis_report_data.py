import argparse
import numpy as np
import pandas as pd
import re

def list2text(l):
	text = ""
	i = 0
	for x in l:
		i = i + 1
		if i > 1:
			if i != len(l):
				text = text + ", " + x
			else:
				text = text + ", and " + x
		else:
			text = x
	return text

def main(args=None):

	print args

	df = pd.DataFrame([x.split(",") for x in args.arrays], columns = ["id","filename","format","liftover"])

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Data}"); f.write("\n")
		f.write(r"\label{Data}"); f.write("\n")

		text=r"In order to run the data we received through our analysis pipeline in an efficient manner, the genotype arrays were each given a short code name; {0}. In Table \ref{{table:Data-Table-Array-Information}}, we list the corresponding filename of the data set we received, the format of the file set (\textit{{note: 'bfile' refers to binary Plink format \cite{{plink}}}}), and a liftOver \cite{{liftover}} chain file if it was required to remap the variants to GRCh37 / hg19 coordinates.".format(list2text(df['id']).replace("_","\_"))

		if args.samples_upset_diagram is not None:
			text = text + r" See Figure \ref{fig:Data-Figure-Samples-Upset-Diagram} for intersection counts of samples remaining for analysis. The counts for each genotype array have been broken down by inferred ancestry as well."

		if args.fam is not None:
			fam=pd.read_table(args.fam.split(",")[1], low_memory=False, header=None)
			text = text + r" After applying sample quality control, there were {0:,d} samples remaining for analysis.".format(fam.shape[0])

		if args.variants_upset_diagram is not None:
			text = text + r" See Figure \ref{fig:Data-Figure-Variants-Upset-Diagram} for intersection counts of variants available for analysis. The counts for each genotype array have been broken down by inferred ancestry as well."

		if args.bim is not None:
			bim=pd.read_table(args.bim.split(",")[1], low_memory=False, header=None)
			text = text + r" After applying variant filters, there were {0:,d} variants remaining for analysis.".format(bim.shape[0])

		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		if len(df['format'].unique().tolist()) == 1:
			text2=r"Each array was received in the format of "


		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Data}".encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Data-Table-Array-Information}".encode('utf-8')); f.write("\n")

		array_tbl=[
			r"\begin{table}[H]",
			r"	\caption{Genotype array information}",
			r"  \footnotesize",
			r"	\begin{center}",
			r"	\begin{tabular}{rlcl}",
			r"		\toprule",
			r"		\textbf{ID} & \textbf{Filename} & \textbf{Format} & \textbf{LiftOver}\\",
			r"		\midrule"]
		for a in args.arrays:
			array_tbl.extend([
			r"		" + " & ".join([re.sub(r"\bNA\b", "N/A", s).replace("_","\_") for s in a.split(',')]) + r" \\"])
		array_tbl.extend([
			r"		\bottomrule",
			r"	\end{tabular}",
			r"	\end{center}",
			r"	\label{table:Data-Table-Array-Information}",
			r"\end{table}"])
		f.write("\n"); f.write("\n".join(array_tbl).encode('utf-8')); f.write("\n")

		if args.samples_upset_diagram is not None:

			f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Data-Figure-Samples-Upset-Diagram}".encode('utf-8')); f.write("\n")

			text=[
				r"\begin{figure}[H]",
				r"\centering",
				r"\includegraphics[width=0.75\linewidth,page=1]{" + args.samples_upset_diagram + r"}",
				r"\caption{Samples remaining for analysis after quality control}",
				r"\label{fig:Data-Figure-Samples-Upset-Diagram}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		if args.variants_upset_diagram is not None:

			f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{Data-Figure-Variants-Upset-Diagram}".encode('utf-8')); f.write("\n")

			text=[
				r"\begin{figure}[H]",
				r"\centering",
				r"\includegraphics[width=0.75\linewidth,page=1]{" + args.variants_upset_diagram + r"}",
				r"\caption{Variants remaining for analysis after quality control}",
				r"\label{fig:Data-Figure-Variants-Upset-Diagram}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		f.write("\n".join(["",r"%<*Data>","%</Data>"]).encode('utf-8')); f.write("\n")
		f.write("\n".join(["",r"%<*Data-Table-Array-Information>","%</Data-Table-Array-Information>"]).encode('utf-8')); f.write("\n")
		f.write("\n".join(["",r"%<*Data-Figure-Samples-Upset-Diagram>","%</Data-Figure-Samples-Upset-Diagram>"]).encode('utf-8')); f.write("\n")
		f.write("\n".join(["",r"%<*Data-Figure-Variants-Upset-Diagram>","%</Data-Figure-Variants-Upset-Diagram>"]).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--samples-upset-diagram', help='an upset diagram for samples')
	requiredArgs.add_argument('--fam', help='a fam file')
	requiredArgs.add_argument('--variants-upset-diagram', help='an upset diagram for harmonized variants')
	requiredArgs.add_argument('--bim', help='a bim file')
	requiredArgs.add_argument('--arrays', nargs='+', help='a list of each arrays comma delimited attributes from the config file', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
