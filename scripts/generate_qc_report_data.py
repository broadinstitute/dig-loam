import argparse
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Data}"); f.write("\n")
		f.write("\n"); f.write(r"\subsection{Samples}"); f.write("\n")

		if args.samples_upset_diagram is not None:
			text=r"The following diagram (Figure \ref{{fig:samplesUpsetDiagram}}) describes the sample distribution over the {0:d} genotype arrays, along with their intersection sizes.".format(args.narrays)
			f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")
			
			text=[
				r"\begin{figure}[H]",
				r"	\centering",
				r"	\includegraphics[width=0.75\linewidth,page=1]{" + args.samples_upset_diagram + "}",
				r"	\caption{Samples distributed by genotyping array}",
				r"	\label{fig:samplesUpsetDiagram}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		else:
			fam=pd.read_table(args.fam.split("___")[1], low_memory=False, header=None)
			text=r"This data consisted of a single genotype array ({0:s}) which contained {1:,d} samples.".format(args.fam.split("___")[0], fam.shape[0])
			f.write("\n"); f.write(text.replace("_","\_").encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsection{Variants}"); f.write("\n")

		text=r"Table \ref{table:variantsSummaryTable} gives an overview of the different variant classes and how they distributed across allele frequencies for each dataset. Note that the totals reflect the sum of the chromosomes only. A legend has been provided below the table for further inspection of the class definitions."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=[
				r"\needlines{25}",
				r"\begin{ThreePartTable}",
				r"	\begin{TableNotes}",
				r"		\footnotesize",
				r"		\item \textbf{Freq} = Minor allele frequency (MAF) range",
				r"		\item \textbf{Unpl} = Chromosome = 0",
				r"		\item \textbf{Auto} = Autosomal variants",
				r"		\item \textbf{X} = X chromosome non-pseudoautosomal region (non-PAR) variants",
				r"		\item \textbf{Y} = Y chromosome variants",
				r"		\item \textbf{X(PAR)} = X chromosome pseudoautosomal (PAR) region variants",
				r"		\item \textbf{Mito} = Mitochodrial variants",
				r"		\item \textbf{InDel} = Insertion/Deletion variants (I/D or D/I alleles)",
				r"		\item \textbf{Multi} = Multiallelic variants (2 or more alternate alleles)",
				r"		\item \textbf{Dup} = Duplicated variants with respect to position and alleles",
				r"	\end{TableNotes}",
				r"	\pgfplotstabletypeset[",
				r"		begin table=\begin{longtable},",
				r"		end table=\end{longtable},",
				r"		font=\footnotesize\sffamily,",
				r"		string replace={NA}{},",
				r"		col sep=tab,",
				r"		columns={Array,Freq,Unpl,Auto,X,Y,X(PAR),Mito,InDel,Multi,Dup,Total},",
				r"		column type={>{\fontseries{bx}\selectfont}c},",
				r"		columns/Array/.style={column name=, string type},",
				r"		columns/Freq/.style={column name=Freq, string type, column type={>{\fontseries{bx}\selectfont}r}},",
				r"		columns/Unpl/.style={column name=Unpl, string type},",
				r"		columns/Auto/.style={column name=Auto, string type},",
				r"		columns/X/.style={column name=X, string type},",
				r"		columns/Y/.style={column name=Y, string type},",
				r"		columns/X(PAR)/.style={column name=X(PAR), string type},",
				r"		columns/Mito/.style={column name=Mito, string type},",
				r"		columns/InDel/.style={column name=InDel, string type},",
				r"		columns/Multi/.style={column name=Multi, string type},",
				r"		columns/Dup/.style={column name=Dup, string type},",
				r"		columns/Total/.style={column name=Total, string type, column type={>{\fontseries{bx}\selectfont}l}},",
				r"		postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
				r"		every head row/.append style={",
				r"			before row=",
				r"				\caption{Summary of raw variants by frequency and classification}\label{table:variantsSummaryTable}\\",
				r"				\insertTableNotes\\\\",
				r"				\toprule,",
				r"			after row=",
				r"				\midrule",
				r"				\endfirsthead",
				r"		},",
				r"		every first row/.append style={",
				r"			before row=",
				r"				\multicolumn{12}{c}{}\\ \caption[]{... Continued from previous page}\\",
				r"				\toprule,",
				r"			after row=",
				r"				\midrule",
				r"				\endhead",
				r"				\midrule",
				r"				\multicolumn{12}{r}{Continued on next page ...}\\",
				r"				\bottomrule",
				r"				\endfoot",
				r"				\endlastfoot",
				r"		},"]

		rows = 0
		with open(args.variants_summary_table) as vsum:
			for i, row in enumerate(vsum):
				rows = rows + 1
		text.extend([r"		every row no " + str(row) + r"/.append style={after row=*}," for row in xrange(1,rows-2) if row % 7 != 0])
		for i in xrange(2,rows-1,14):
			text.extend([r"		grayrow=" + str(row) + r"," for row in xrange(i,i+7)])
		text.extend([
				r"		every last row/.style={",
				r"			after row=",
				r"				\bottomrule",
				r"		},",
				r"		empty cells with={}",
				r"	]{" + args.variants_summary_table + r"}",
				r"\end{ThreePartTable}"])
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text = r"To facilitate downstream operations on genotype data, such as merging and meta-analysis, each dataset gets harmonized with modern reference data. The harmonization process is performed in two steps. First, using Genotype Harmonizer \cite{genotypeHarmonizer}, the variants are strand-aligned with the 1000 Genomes Phase 3 Version 5 \cite{1KG} variants. While some variants (A/C or G/T variants) may be removed due to strand ambiguity, if enough information exists, Genotype Harmonizer uses linkage disequilibrium (LD) patterns with nearby variants to accurately determine strand. This step will remove variants that it is unable to reconcile and maintains variants that are unique to the input data. The second step manually reconciles non-1000 Genomes variants with the human reference assembly GRCh37 \cite{humref}. This step will flag variants for removal that do not match an allele to the reference and variants that have only a single allele in the data file (0 for the other). Note that some monomorphic variants may be maintained in this process if there are two alleles in the data file and one of them matches a reference allele."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"After harmonization, the data is loaded into a Hail \cite{hail} variant dataset (VDS) for downstream use."

		if args.variants_upset_diagram is not None:

			text = text + r" See Figure \ref{fig:variantsUpsetDiagram} for final variant counts by genotyping array."
			f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")
			
			text=[
				r"\begin{figure}[H]",
				r"	\centering",
				r"	\includegraphics[width=0.75\linewidth,page=1]{" + args.variants_upset_diagram + "}",
				r"	\caption{Variants remaining for analysis}",
				r"	\label{fig:variantsUpsetDiagram}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		else:
			bim=pd.read_table(args.bim.split("___")[1], low_memory=False, header=None)
			text = text + r"The resulting dataset consisted of {0:,d} total variants.".format(bim.shape[0])
			f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--narrays', type=int, help='an integer', required=True)
	requiredArgs.add_argument('--samples-upset-diagram', help='an upset diagram for samples')
	requiredArgs.add_argument('--fam', help='a fam file')
	requiredArgs.add_argument('--variants-summary-table', help='a variant summary table', required=True)
	requiredArgs.add_argument('--variants-upset-diagram', help='an upset diagram for harmonized variants')
	requiredArgs.add_argument('--bim', help='a bim file')
	args = parser.parse_args()
	main(args)
