import argparse
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing sampleqc section"
		f.write("\n"); f.write(r"\subsection{Sample Outlier Detection}"); f.write("\n")

		text=r"""Each sample was evaluated for inclusion in association tests based on 10 sample-by-variant metrics (Table \ref{table:sampleMetricDefinitions}), calculated using Hail \cite{hail}."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{table}[H]
			\caption{Sample Metrics}
			\centering
			\begin{tabular}{>{\bfseries}r l}
				nNonRef & nHet + nHomVar \\
				nHet & Number of heterozygous variants \\
				nCalled & nHomRef + nHet + nHomVar \\
				callRate & Fraction of variants with called genotypes \\
				rTiTv &  Transition/transversion ratio \\
				het & Inbreeding coefficient \\
				hetHigh & Inbreeding coefficient for variants with \(MAF >= 0.03\) \\
				hetLow & Inbreeding coefficient for variants with \(MAF < 0.03\) \\
				nHomVar & Number of homozygous alternate variants \\
				rHetHomVar & Het/HomVar ratio across all variants
			\end{tabular}
			\label{table:sampleMetricDefinitions}
		\end{table}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Principal Component Adjustment and Normalization of Sample Metrics}"); f.write("\n")

		text=r"""Due to possible population substructure, the sample metrics exhibit some multi-modality in their distributions. To evaluate more normally distributed data, we calculated principal component adjusted residuals of the metrics using the top 10 principal components (PCARM's). Figure \ref{{fig:nhetCompare}} shows the nHet metric for {0} samples before and after adjustment.""".format(args.compare_dist_nhet_label)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{figure}[H]
			\caption{Comparison of nHet distributions before and after adjustment / normalization}
			\centering
			\begin{subfigure}{\textwidth}
				\centering
				\includegraphics[width=\linewidth]{""" + args.compare_dist_nhet_unadj + r"""}
				\caption{nHet Original}
				\label{fig:nhet}
			\end{subfigure}\newline
			\begin{subfigure}{\textwidth}
				\centering
				\includegraphics[width=\linewidth]{""" + args.compare_dist_nhet_adj + r"""}
				\caption{nHet Adjusted}
				\label{fig:nhetAdj}
			\end{subfigure}
			\label{fig:nhetCompare}
		\end{figure}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Individual Sample Metric Clustering}"); f.write("\n")

		text=r"""For outlier detection, we clustered the samples into Gaussian distributed subsets with respect to each PCARM using the software Klustakwik \cite{klustakwik}. During this process, samples that did not fit into any Gaussian distributed set of samples were identified and flagged for removal."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Principal Components of Variation in PCARM's}"); f.write("\n")

		text=r"""In addition to outliers along individual sample metrics, there may be samples that exhibit deviation from the norm across multiple metrics. In order to identify these samples, we calculated principal components explaining 95\% of the variation in all 10 PCARMs combined. """
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Combined PCARM Clustering}"); f.write("\n")

		text=r"""All samples were clustered into Gaussian distributed subsets along the principal components of the PCARM's, again using Klustakwik \cite{klustakwik}. This effectively removed any samples that were far enough outside the distribution on more than one PCARM, but not necessarily flagged as an outlier on any of the individual metrics alone."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Plots of Sample Outliers}"); f.write("\n")

		text_insert=""
		i = 0
		for a in args.sampleqc_outliers.split(","):
			i = i + 1
			l = a.split("___")[0]
			if i == 1:
				if len(args.sampleqc_outliers.split(",")) > 1:
					text_insert = r"Figures \ref{fig:adjSampleMetricDist" + l + r"}"
				else:
					text_insert = r"Figure \ref{fig:adjSampleMetricDist" + l + r"}"
			elif i < len(args.sampleqc_outliers.split(",")) - 1:
				text_insert = text_insert + r", \ref{fig:adjSampleMetricDist" + l + r"}"
			else:
				if len(args.sampleqc_outliers.split(",")) == 2:
					text_insert = text_insert + r" and \ref{fig:adjSampleMetricDist" + l + r"}"
				else:
					text_insert = text_insert + r", and \ref{fig:adjSampleMetricDist" + l + r"}"
		text=r"""The distributions for each PCARM and any outliers (cluster = 1) found are shown in {0}. Samples are labeled according to Table \ref{{table:sampleOutlierLegend}}.""".format(text_insert)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{table}[H]
			\caption{Sample Legend for Outlier Plots}
			\centering
			\begin{tabular}{>{\bfseries}r l}
				Grey & Clustered into Gaussian distributed subsets (not Flagged) \\
				Orange & Flagged as outlier based on individual PCARM's \\
				Blue & Flagged as outlier based on PC's of PCARM's \\
				Green & Flagged as outlier for both methods
			\end{tabular}
			\label{table:sampleOutlierLegend}
		\end{table}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		for a in args.sampleqc_outliers.split(","):
			l = a.split("___")[0]
			m = a.split("___")[1]
			text=r"""\begin{figure}[H]
				\centering
				\includegraphics[width=\paperwidth,height=0.9\textheight,keepaspectratio]{""" + m + r"""}
				\caption{Adjusted sample metric distributions for """ + l + r"""}
				\label{fig:adjSampleMetricDist""" + l + r"""}
			\end{figure}"""
			f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsection{Summary of Sample Outlier Detection}"); f.write("\n")

		text=r"""Table \ref{table:outlierSummaryTable} contains a summary of outliers detected by each method and across both genotyping technologies. Note that "PCA(Metrics)" results from the clustering of the PCs of the 10 PCARM's combined, so "Metrics + PCA(Metrics)" is the union of samples flagged by that method with samples flagged by each of the 10 individual metric clusterings. Figure \ref{fig:samplesRemaining} summarizes the samples remaining for analysis."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")
		
		text=r"""\begin{table}[H]
			\caption{Samples flagged for removal}
			\begin{center}
				\resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%
					\sffamily
					\pgfplotstabletypeset[
						font=\footnotesize,
						col sep=tab,"""
		tbl = pd.read_table(args.sampleqc_summary_table)
		cols = tbl.columns
		text = text + r"""
						columns={{{0}}},""".format(",".join(cols))		
		text = text + r"""
						column type={>{\fontseries{bx}\selectfont}c},"""
		for c in cols:
			if c == cols[0]:
				text = text + r"""
						columns/""" + c + r"""/.style={column name=, string type, column type={>{\fontseries{bx}\selectfont}r}},"""
			elif c == cols[len(cols)-1]:
				text = text + r"""
						columns/""" + c + r"""/.style={column name=""" + c + r""", string type, column type={>{\fontseries{bx}\selectfont}l}},"""
			else:
				text = text + r"""
						columns/""" + c + r"""/.style={column name=""" + c + r""", string type},"""
		text = text + r"""
						postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},
						every head row/.style={before row={\toprule}, after row={\midrule}},
						every last row/.style={before row=\bottomrule}
					]{""" + args.sampleqc_summary_table + r"""}}
			\label{table:outlierSummaryTable}
			\end{center}
		\end{table}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{figure}[H]
				 \centering
				 \includegraphics[width=0.75\linewidth]{""" + args.samples_upset_diagram + r"""}
				 \caption{Samples remaining for analysis}
				 \label{fig:samplesRemaining}
			  \end{figure}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--compare-dist-nhet-unadj', help='an nHet plot', required=True)
	requiredArgs.add_argument('--compare-dist-nhet-adj', help='an nHet adjusted plot', required=True)
	requiredArgs.add_argument('--compare-dist-nhet-label', help='an array label', required=True)
	requiredArgs.add_argument('--sampleqc-outliers', help='a comma separated list of array labels and sampleqc outlier plots, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--sampleqc-summary-table', help='a sampleqc summary table', required=True)
	requiredArgs.add_argument('--samples-upset-diagram', help='an upset diagram for samples remaining', required=True)
	args = parser.parse_args()
	main(args)
