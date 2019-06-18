import argparse
import numpy as np
import pandas as pd
import collections

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing sampleqc section"
		f.write("\n"); f.write(r"\subsection{Sample Outlier Detection}"); f.write("\n")

		text=r"Each sample was evaluated for inclusion in association tests based on 10 sample-by-variant metrics (Table \ref{table:sampleMetricDefinitions}), calculated using Hail \cite{hail}. Note that for the metrics n\_called and call\_rate, only samples below the mean are filtered."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=[
			r"\begin{table}[H]",
			r"	\caption{Sample Metrics}",
			r"	\begin{center}",
			r"	\begin{tabular}{>{\bfseries}r l}",
			r"		\toprule",
			r"		n\_non\_ref & n\_het + n\_hom\_var\\",
			r"		n\_het & Number of heterozygous variants\\",
			r"		n\_called & n\_hom\_ref + n\_het + n\_hom\_var\\",
			r"		call\_rate & Fraction of variants with called genotypes\\",
			r"		r\_ti\_tv &  Transition/transversion ratio\\",
			r"		het & Inbreeding coefficient\\",
			r"		het\_high & Inbreeding coefficient for variants with \(MAF >= 0.03\)\\",
			r"		het\_low & Inbreeding coefficient for variants with \(MAF < 0.03\)\\",
			r"		n\_hom\_var & Number of homozygous alternate variants\\",
			r"		r\_het\_hom\_var & het/hom\_var ratio across all variants\\",
			r"		\bottomrule",
			r"	\end{tabular}",
			r"	\end{center}",
			r"	\label{table:sampleMetricDefinitions}",
			r"\end{table}"]
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Principal Component Adjustment and Normalization of Sample Metrics}"); f.write("\n")

		text=r"Due to possible population substructure, the sample metrics exhibit some multi-modality in their distributions. To evaluate more normally distributed data, we calculated principal component adjusted residuals of the metrics using the top 10 principal components (PCARM's). Figure \ref{{fig:metricCompare}} shows the {0} metric for {1} samples before and after adjustment.".format(args.compare_dist_metric.replace("_res","").replace("_","\_"), args.compare_dist_label.replace("_","\_"))
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=[
			r"\begin{figure}[H]",
			r"	\caption{Comparison of " + args.compare_dist_metricreplace("_","\_") + r" distributions before and after adjustment / normalization}",
			r"	\centering",
			r"	\begin{subfigure}{\textwidth}",
			r"		\centering",
			r"		\includegraphics[width=\linewidth,page=1]{" + args.compare_dist_unadj + r"}",
			r"		\caption{Original}",
			r"		\label{fig:metric}",
			r"	\end{subfigure}\newline",
			r"	\begin{subfigure}{\textwidth}",
			r"		\centering",
			r"		\includegraphics[width=\linewidth,page=1]{" + args.compare_dist_adj + r"}",
			r"		\caption{Adjusted}",
			r"		\label{fig:metricAdj}",
			r"	\end{subfigure}",
			r"	\label{fig:metricCompare}",
			r"\end{figure}"]
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Individual Sample Metric Clustering}"); f.write("\n")

		text=r"For outlier detection, we clustered the samples into Gaussian distributed subsets with respect to each PCARM using the software Klustakwik \cite{klustakwik}. During this process, samples that did not fit into any Gaussian distributed set of samples were identified and flagged for removal."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Principal Components of Variation in PCARM's}"); f.write("\n")

		text=r"In addition to outliers along individual sample metrics, there may be samples that exhibit deviation from the norm across multiple metrics. In order to identify these samples, we calculated principal components explaining 95\% of the variation in 8 of the 10 PCARMs combined. The adjusted residuals for metrics 'call\_rate' and 'n\_called' are characterized by long tails that lead to the maximum value, which is not consistent with the other metrics. In order to avoid excessive flagging of samples with lower, yet still completely acceptable, call rates, these metrics were left out of principal component calculation."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Combined PCARM Clustering}"); f.write("\n")

		text=r"All samples were clustered into Gaussian distributed subsets along the principal components of the PCARM's, again using Klustakwik \cite{klustakwik}. This effectively removed any samples that were far enough outside the distribution on more than one PCARM, but not necessarily flagged as an outlier on any of the individual metrics alone."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Plots of Sample Outliers}"); f.write("\n")

		text_insert=""
		i = 0
		for x in args.metric_outlier_plots:
			i = i + 1
			array = x.split(",")[0]
			if i == 1:
				if len(args.metric_outlier_plots) > 1:
					text_insert = r"Figures \ref{fig:adjSampleMetricDist" + array.replace("_","") + r"}"
				else:
					text_insert = r"Figure \ref{fig:adjSampleMetricDist" + array.replace("_","") + r"}"
			elif i < len(args.metric_outlier_plots):
				text_insert = text_insert + r", \ref{fig:adjSampleMetricDist" + array.replace("_","") + r"}"
			else:
				if len(args.metric_outlier_plots) == 2:
					text_insert = text_insert + r" and \ref{fig:adjSampleMetricDist" + array.replace("_","") + r"}"
				else:
					text_insert = text_insert + r", and \ref{fig:adjSampleMetricDist" + array.replace("_","") + r"}"
		text=r"The distributions for each PCARM and any outliers (cluster = 1) found are shown in {0}. Samples are labeled according to Table \ref{{table:sampleOutlierLegend}}.".format(text_insert)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=[
			r"\begin{table}[H]",
			r"	\caption{Sample Legend for Outlier Plots}",
			r"	\begin{center}",
			r"	\begin{tabular}{>{\bfseries}r l}",
			r"		\toprule",
			r"		Grey & Clustered into Gaussian distributed subsets (not Flagged)\\*",
			r"		Orange & Flagged as outlier based on individual PCARM's\\*",
			r"		Blue & Flagged as outlier based on PC's of PCARM's\\*",
			r"		Green & Flagged as outlier for both methods\\*",
			r"		\bottomrule",
			r"	\end{tabular}",
			r"	\end{center}",
			r"	\label{table:sampleOutlierLegend}",
			r"\end{table}"]
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		for x in args.metric_outlier_plots:
			array = x.split(",")[0]
			text=[
				r"\begin{figure}[H]",
				r"	\centering",
				r"	\includegraphics[width=\paperwidth,height=0.9\textheight,keepaspectratio,page=1]{" + x.split(",")[1] + r"}",
				r"	\caption{Adjusted sample metric distributions for " + array.replace("_","\_") + r"}",
				r"	\label{fig:adjSampleMetricDist""" + array.replace("_","") + r"}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsection{Summary of Sample Outlier Detection}"); f.write("\n")

		text_dict1 = collections.OrderedDict()
		for x in args.restore:
			df = pd.read_table(x.split(",")[1])
			df = df[df['RestoreFrom'] == "sampleqcKeep"]
			if df.shape[0] > 0:
				text_dict1[x.split(",")[0]] = "{0:,d}".format(df.shape[0])

		if len(text_dict1) == 0:
			text1 = "no"
		if len(text_dict1) == 1:
			text1 = text_dict1[text_dict1.keys()[0]]
		if len(text_dict1) == 2:
			text1 = " and ".join([str(text_dict1[x]) + " " + x.replace("_","\_") for x in text_dict1.keys()[0:len(text_dict1.keys())]])
		elif len(text_dict1) > 2:
			text1 = ", ".join([str(text_dict1[x]) + " " + x.replace("_","\_") for x in text_dict1.keys()[0:(len(text_dict1.keys())-1)]]) + " and " + str(text_dict1[text_dict1.keys()[len(text_dict1.keys())-1]]) + " " + text_dict1.keys()[len(text_dict1.keys())-1].replace("_","\_")

		text=r"Table \ref{{table:outlierSummaryTable}} contains a summary of outliers detected by each method and across all genotyping technologies. Note that 'PCA(Metrics)' results from the clustering of the PCs of the 8 PCARM's combined, so 'Metrics + PCA(Metrics)' is the union of samples flagged by that method with samples flagged by each of the 10 individual metric clusterings. Figure \ref{{fig:samplesRemaining}} summarizes the samples remaining for analysis. Upon further inspection, {0} samples were manually reinstated during this step. More information is available upon request".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=[
					r"\begin{table}[H]",
					r"	\caption{Samples flagged for removal}",
					r"	\begin{center}",
					r"	\pgfplotstabletypeset[",
					r"		font=\footnotesize\sffamily,",
					r"		col sep=tab,"]
		tbl = pd.read_table(args.sampleqc_summary_table)
		cols = tbl.columns
		text.extend([
					r"		columns={{{0}}},".format(",".join(cols))])
		text.extend([
					r"		column type={>{\fontseries{bx}\selectfont}c},"])
		for c in cols:
			if c == cols[0]:
				text.extend([
					r"		columns/" + c + r"/.style={column name=, string type, column type={>{\fontseries{bx}\selectfont}r}},"])
			elif c == cols[len(cols)-1]:
				text.extend([
					r"		columns/" + c + r"/.style={column name=" + c.replace("_","\_") + r", string type, column type={>{\fontseries{bx}\selectfont}l}},"])
			else:
				text.extend([
					r"		columns/" + c + r"/.style={column name=" + c.replace("_","\_") + r", string type},"])
		text.extend([
					r"		postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
					r"		every head row/.append style={",
					r"			before row=",
					r"				\toprule,",
					r"			after row=",
					r"				\midrule",
					r"		},",
					r"		every last row/.style={",
					r"			after row=",
					r"				\bottomrule",
					r"		},",
					r"		empty cells with={}",
					r"	]{" + args.sampleqc_summary_table + r"}",
					r"	\end{center}",
					r"	\label{table:outlierSummaryTable}",
					r"\end{table}"])
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text=[
			r"\begin{figure}[H]",
			r"	\centering",
			r"	\includegraphics[width=0.75\linewidth,page=1]{" + args.samples_upset_diagram + r"}",
			r"	\caption{Samples remaining for analysis}",
			r"	\label{fig:samplesRemaining}",
			r"\end{figure}"]
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--compare-dist-unadj', help='an nHet plot', required=True)
	requiredArgs.add_argument('--compare-dist-adj', help='an nHet adjusted plot', required=True)
	requiredArgs.add_argument('--compare-dist-label', help='an array label', required=True)
	requiredArgs.add_argument('--compare-dist-metric', help='a metric', required=True)
	requiredArgs.add_argument('--metric-outlier-plots', nargs='+', help='a comma separated list of array labels and sampleqc outlier plots, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--sampleqc-summary-table', help='a sampleqc summary table', required=True)
	requiredArgs.add_argument('--samples-upset-diagram', help='an upset diagram for samples remaining', required=True)
	requiredArgs.add_argument('--restore', nargs='+', help='a space separated list of array labels and sample restore files, each separated by comma', required=True)
	args = parser.parse_args()
	main(args)
