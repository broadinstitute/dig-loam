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

		text=r"""The following diagram (Figure \ref{{fig:samplesUpsetDiagram}}) describes the sample distribution over the {0:d} genotype arrays, along with their intersection sizes.""".format(args.narrays)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")
		
		text=r"""\begin{figure}[H]
			     \centering
			     \includegraphics[width=0.75\linewidth]{""" + args.samples_upset_diagram + """}
			     \caption{Samples distributed by genotyping array}
			     \label{fig:samplesUpsetDiagram}
			  \end{figure}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsection{Variants}"); f.write("\n")

		text=r"""Table \ref{table:variantsSummaryTable} gives an overview of the different variant classes and how they distribute across certain frequencies for each dataset. Note that the totals reflect the sum of the chromosomes only. A legend has been provided below the table for further inspection of the class definitions."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{table}[H]
				\caption{Summary of raw variants by frequency and classification}
				\begin{center}
					\resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%
						\sffamily
						\pgfplotstabletypeset[
							font=\footnotesize,
							col sep=tab,
							columns={Array,Freq,Unpl,Auto,X,Y,X(PAR),Mito,InDel,Multi,Dup,Total},
							column type={>{\fontseries{bx}\selectfont}c},
							columns/Array/.style={column name=, string type},
							columns/Freq/.style={column name=Freq, string type, column type={>{\fontseries{bx}\selectfont}r}},
							columns/Unpl/.style={column name=Unpl, string type},
							columns/Auto/.style={column name=Auto, string type},
							columns/X/.style={column name=X, string type},
							columns/Y/.style={column name=Y, string type},
							columns/X(PAR)/.style={column name=X(PAR), string type},
							columns/Mito/.style={column name=Mito, string type},
							columns/InDel/.style={column name=InDel, string type},
							columns/Multi/.style={column name=Multi, string type},
							columns/Dup/.style={column name=Dup, string type},
							columns/Total/.style={column name=Total, string type, column type={>{\fontseries{bx}\selectfont}l}},
							postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},
							every head row/.style={before row={\toprule}, after row={\midrule}},
							every last row/.style={after row=\bottomrule},
							empty cells with={}
						]{""" + args.variants_summary_table + r"""}}
				\label{table:variantsSummaryTable}
				\end{center}
				\hfill
				\footnotesize
				\begin{tabular}{>{\bfseries}r l}
					Freq & Minor allele frequency (MAF) range \\
					Unpl & Chromosome = 0 \\
					Auto & Autosomal variants \\
					X & X chromosome non-pseudoautosomal region (non-PAR) variants \\
					Y & Y chromosome variants \\
					X(PAR) & X chromosome pseudoautosomal (PAR) region variants \\
					Mito & Mitochodrial variants \\
					InDel & Insertion/Deletion variants (I/D or D/I alleles) \\
					Multi & Multiallelic variants (2 or more alternate alleles) \\
					Dup & Duplicated variants with respect to position and alleles
				\end{tabular}
			\end{table}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text = r"""To facilitate downstream operations on genotype data, such as merging and meta-analysis, each dataset is harmonized with modern reference data. This harmonization process is performed in two steps. First, using Genotype Harmonizer \cite{genotypeHarmonizer}, the variants are strand-aligned with the 1000 Genomes Phase 3 Version 5 \cite{1KG} variants. While some variants (A/C or G/T variants) are removed due to strand ambiguity, if enough information exists, Genotype Harmonizer uses linkage disequilibrium (LD) patterns with nearby variants to accurately determine strand. This step removes variants that it is unable to reconcile and maintains variants that are unique to the input data. The second step manually reconciles non-1000 Genomes variants with the human reference assembly GRCh37 \cite{humref}. This step flags variants for removal that do not match an allele to the reference and variants that have only a single allele in the data file (0 for the other). Note that some monomorphic variants are maintained in this process if there are two alleles in the data file and one of them matches a reference allele."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""After harmonization, the data is loaded into a Hail \cite{hail} variant dataset (VDS) for downstream use. See Figure \ref{fig:variantsUpsetDiagram} for final variant counts by genotyping array."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{figure}[H]
			     \centering
			     \includegraphics[width=0.75\linewidth]{""" + args.variants_upset_diagram + """}
			     \caption{Variants remaining for analysis}
			     \label{fig:variantsUpsetDiagram}
			  \end{figure}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--narrays', type=int, help='an integer', required=True)
	requiredArgs.add_argument('--samples-upset-diagram', help='an upset diagram for samples', required=True)
	requiredArgs.add_argument('--variants-summary-table', help='a variant summary table', required=True)
	requiredArgs.add_argument('--variants-upset-diagram', help='an upset diagram for harmonized variants', required=True)
	args = parser.parse_args()
	main(args)
