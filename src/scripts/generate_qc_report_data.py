import argparse
import numpy as np
import pandas as pd
import collections

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Data}"); f.write("\n")
		f.write("\n"); f.write(r"\subsection{Samples}"); f.write("\n")

		nImiss = collections.OrderedDict()
		if args.imiss:
			for x in args.imiss:
				with open(x.split(",")[1]) as f_imiss:
					nLines = len(f_imiss.readlines())
					if nLines > 0:
						nImiss[x.split(",")[0]] = nLines

			text1 = "each microarray" if len(nImiss) > 1 else "the microarray"
			text2 = "There were " if len(nImiss) != 1 or nImiss[nImiss.keys()[0]] != 1 else "There was "
			if len(nImiss) == 0:
				text2 = text2 + "no"
			if len(nImiss) == 1:
				text2 = text2 + str(nImiss[nImiss.keys()[0]]) 
			if len(nImiss) == 2:
				text2 = text2 + " and ".join([str(nImiss[x]) + " " + x.replace("_","\_") for x in nImiss.keys()[0:(len(nImiss.keys()))]])
			elif len(nImiss) > 2:
				text2 = text2 + ", ".join([str(nImiss[x]) + " " + x.replace("_","\_") for x in nImiss.keys()[0:(len(nImiss.keys())-1)]]) + " and " + str(nImiss[nImiss.keys()[len(nImiss.keys())-1]]) + " " + nImiss.keys()[len(nImiss.keys())-1].replace("_","\_")
			text2 = text2 + " samples " if len(nImiss) != 1 or nImiss[nImiss.keys()[0]] != 1 else text2 + " sample"
	
			text=r"Initially, {0} was assessed for sample genotype missingness. Prior to our standard quality control procedures, we removed any samples exhibiting extreme genotype missingness (ie. $> 0.5$). The intention of this threshold is to remove any samples that appear to suffer from obvious poor genotyping, which would likely add noise to our analyses. {1} removed from downstream analyses at this stage.".format(text1, text2)
			f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		if args.samples_upset_diagram is not None:
			text=r"The following diagram (Figure \ref{fig:samplesUpsetDiagram}) describes the distribution of the remaining samples that were included in our full quality control analysis."
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
			sample_list=pd.read_table(args.sample_list.split(",")[1], low_memory=False, header=None)
			text=r"This data consisted of a single batch of data ({0:s}), from which {1:,d} remaining samples were included in our full quality control analysis.".format(args.sample_list.split(",")[0], sample_list.shape[0])
			f.write("\n"); f.write(text.replace("_","\_").encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsection{Variants}"); f.write("\n")

		with open(args.geno_variants_summary_table) as vsum:

			rows = 0
			for i, row in enumerate(vsum):
				rows = rows + 1

			if rows > 0:

				text=r"Table \ref{table:genoVariantsSummaryTable} gives an overview of the different variant classes and how they distributed across allele frequencies in the raw microarray data. Note that the totals reflect the sum of the chromosomes only. A legend has been provided below the table for further inspection of the class definitions."
				f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")
	
				text=[
					r"\needlines{25}",
					r"\begin{ThreePartTable}",
					r"	\begin{TableNotes}",
					r"		\footnotesize",
					r"		\item \textbf{Freq}: Minor allele frequency (MAF) range",
					r"		\item \textbf{Unpl}: Chromosome = 0",
					r"		\item \textbf{Auto}: Autosomal variants",
					r"		\item \textbf{X}: X chromosome non-pseudoautosomal region (non-PAR) variants",
					r"		\item \textbf{Y}: Y chromosome variants",
					r"		\item \textbf{X(PAR)}: X chromosome pseudoautosomal (PAR) region variants",
					r"		\item \textbf{Mito}: Mitochondrial variants",
					r"		\item \textbf{InvalInDel}: Invalid Insertion/Deletion variants (I/D or D/I alleles)",
					r"		\item \textbf{ValInDel}: Valid Insertion/Deletion variants",
					r"		\item \textbf{Multi}: Multiallelic variants (2 or more alternate alleles)",
					r"		\item \textbf{Dup}: Duplicated variants with respect to position and alleles",
					r"		\item \textbf{NoGeno}: No Genotypes available",
					r"	\end{TableNotes}",
					r"	\pgfplotstabletypeset[",
					r"		begin table=\begin{longtable},",
					r"		end table=\end{longtable},",
					r"		font=\tiny\sffamily,",
					r"		string replace={NA}{},",
					r"		col sep=tab,",
					r"		columns={Array,Freq,Unpl,Auto,X,Y,X(PAR),Mito,InvalInDel,ValInDel,Multi,Dup,NoGeno,Total},",
					r"		column type={>{\fontseries{bx}\selectfont}c},",
					r"		columns/Array/.style={column name=, string type},",
					r"		columns/Freq/.style={column name=Freq, string type, column type={>{\fontseries{bx}\selectfont}r}},",
					r"		columns/Unpl/.style={column name=Unpl, string type},",
					r"		columns/Auto/.style={column name=Auto, string type},",
					r"		columns/X/.style={column name=X, string type},",
					r"		columns/Y/.style={column name=Y, string type},",
					r"		columns/X(PAR)/.style={column name=X(PAR), string type},",
					r"		columns/Mito/.style={column name=Mito, string type},",
					r"		columns/InvalInDel/.style={column name=InvalInDel, string type},",
					r"		columns/ValInDel/.style={column name=ValInDel, string type},",
					r"		columns/Multi/.style={column name=Multi, string type},",
					r"		columns/Dup/.style={column name=Dup, string type},",
					r"		columns/NoGeno/.style={column name=NoGeno, string type},",
					r"		columns/Total/.style={column name=Total, string type, column type={>{\fontseries{bx}\selectfont}l}},",
					r"		postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
					r"		every head row/.append style={",
					r"			before row=",
					r"				\caption{Summary of raw variants by frequency and classification}\label{table:genoVariantsSummaryTable}\\",
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
				text.extend([r"		every row no " + str(row) + r"/.append style={after row=*}," for row in xrange(1,rows-2) if row % 7 != 0])
				for i in xrange(2,rows-1,14):
					text.extend([r"		grayrow=" + str(row) + r"," for row in xrange(i,i+7)])
				text.extend([
					r"		every last row/.style={",
					r"			after row=",
					r"				\bottomrule",
					r"		},",
					r"		empty cells with={}",
					r"	]{" + args.geno_variants_summary_table + r"}",
					r"\end{ThreePartTable}"])
				f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

				text = r"To facilitate downstream operations on microarray data, such as merging and meta-analysis, each microarray batch was harmonized with modern reference data. The harmonization process was performed in two steps. First, using Genotype Harmonizer \cite{genotypeHarmonizer}, the variants were strand-aligned with the 1000 Genomes Phase 3 Version 5 \cite{1KG} variants. While some variants (A/C or G/T variants) may be removed due to strand ambiguity, if enough information exists, Genotype Harmonizer uses linkage disequilibrium (LD) patterns with nearby variants to accurately determine strand. This step removes variants that it is unable to reconcile and maintains variants that are unique to the input data. The second step manually reconciles non-1000 Genomes variants with the human reference assembly GRCh37 \cite{humref}. This step flags variants for removal that do not match an allele to the reference and variants that have only a single allele in the data file (0 for the other). Note that some monomorphic variants may have been maintained in this process if there were two alleles in the data file and one of them matched a reference allele. After harmonization, the data was loaded into a Hail \cite{hail} matrix table for downstream use."
				f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")
		
		with open(args.seq_variants_summary_table) as vsum:

			rows = 0
			for i, row in enumerate(vsum):
				rows = rows + 1

			if rows > 0:

				text=r"Table \ref{table:alignedVariantsSummaryTable} gives an overview of the different variant classes and how they distributed across allele frequencies in the data that was previously aligned to a reference. Note that the totals reflect the sum of the chromosomes only. A legend has been provided below the table for further inspection of the class definitions."
				f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")
	
				text=[
					r"\needlines{25}",
					r"\begin{ThreePartTable}",
					r"	\begin{TableNotes}",
					r"		\footnotesize",
					r"		\item \textbf{Freq}: Minor allele frequency (MAF) range",
					r"		\item \textbf{Unpl}: Chromosome = 0",
					r"		\item \textbf{Auto}: Autosomal variants",
					r"		\item \textbf{X}: X chromosome non-pseudoautosomal region (non-PAR) variants",
					r"		\item \textbf{Y}: Y chromosome variants",
					r"		\item \textbf{XY}: X chromosome pseudoautosomal (PAR) region variants",
					r"		\item \textbf{MT}: Mitochondrial variants",
					r"		\item \textbf{InDel}: Insertion/Deletion variants",
					r"		\item \textbf{WasSplit}: Variants that were split from multiallelic variants",
					r"	\end{TableNotes}",
					r"	\pgfplotstabletypeset[",
					r"		begin table=\begin{longtable},",
					r"		end table=\end{longtable},",
					r"		font=\tiny\sffamily,",
					r"		string replace={NA}{},",
					r"		col sep=tab,",
					r"		columns={Batch,Freq,Unpl,Auto,X,Y,XY,MT,InDel,WasSplit,Total},",
					r"		column type={>{\fontseries{bx}\selectfont}c},",
					r"		columns/Batch/.style={column name=, string type},",
					r"		columns/Freq/.style={column name=Freq, string type, column type={>{\fontseries{bx}\selectfont}r}},",
					r"		columns/Unpl/.style={column name=Unpl, string type},",
					r"		columns/Auto/.style={column name=Auto, string type},",
					r"		columns/X/.style={column name=X, string type},",
					r"		columns/Y/.style={column name=Y, string type},",
					r"		columns/XY/.style={column name=XY, string type},",
					r"		columns/MT/.style={column name=MT, string type},",
					r"		columns/InDel/.style={column name=InDel, string type},",
					r"		columns/WasSplit/.style={column name=WasSplit, string type},",
					r"		columns/Total/.style={column name=Total, string type, column type={>{\fontseries{bx}\selectfont}l}},",
					r"		postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
					r"		every head row/.append style={",
					r"			before row=",
					r"				\caption{Summary of pre-aligned variants by frequency and classification}\label{table:alignedVariantsSummaryTable}\\",
					r"				\insertTableNotes\\\\",
					r"				\toprule,",
					r"			after row=",
					r"				\midrule",
					r"				\endfirsthead",
					r"		},",
					r"		every first row/.append style={",
					r"			before row=",
					r"				\multicolumn{9}{c}{}\\ \caption[]{... Continued from previous page}\\",
					r"				\toprule,",
					r"			after row=",
					r"				\midrule",
					r"				\endhead",
					r"				\midrule",
					r"				\multicolumn{9}{r}{Continued on next page ...}\\",
					r"				\bottomrule",
					r"				\endfoot",
					r"				\endlastfoot",
					r"		},"]
				text.extend([r"		every row no " + str(row) + r"/.append style={after row=*}," for row in xrange(1,rows-2) if row % 9 != 0])
				for i in xrange(2,rows-1,18):
					text.extend([r"		grayrow=" + str(row) + r"," for row in xrange(i,i+9)])
				text.extend([
						r"		every last row/.style={",
						r"			after row=",
						r"				\bottomrule",
						r"		},",
						r"		empty cells with={}",
						r"	]{" + args.seq_variants_summary_table + r"}",
						r"\end{ThreePartTable}"])
				f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

				text = r"For this data, reference genome alignment was likely performed prior to aquisition. So, harmonization was skipped and the data was loaded directly into a Hail \cite{hail} matrix table for downstream use."
				f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		if args.variants_upset_diagram is not None:

			text = r" See Figure \ref{fig:variantsUpsetDiagram} for final variant counts by batch."
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
			var_count = 0
			for vlist in args.variant_list:
				variant_list=pd.read_table(vlist.split(",")[1], low_memory=False, header=None)
				var_count = var_count + variant_list.shape[0]
			text = r"The resulting data consisted of {0:,d} variants for inclusion in downstream analyses.".format(var_count)
			f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--imiss', nargs='+', help='a list of array labels and samples removed due to high missingness, each separated by comma')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--narrays', type=int, help='an integer', required=True)
	requiredArgs.add_argument('--samples-upset-diagram', help='an upset diagram for samples')
	requiredArgs.add_argument('--sample-list', help='a sample list file')
	requiredArgs.add_argument('--geno-variants-summary-table', help='a genotyped variant summary table', required=True)
	requiredArgs.add_argument('--seq-variants-summary-table', help='a sequenced variant summary table', required=True)
	requiredArgs.add_argument('--variants-upset-diagram', help='an upset diagram for harmonized variants')
	requiredArgs.add_argument('--variant-list', nargs='+', help='a list of array labels and variant list files, each separated by comma')
	args = parser.parse_args()
	main(args)
