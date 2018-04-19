import argparse
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Sample QC}"); f.write("\n")
		f.write("\n"); f.write(r"\subsection{Ancestry Inference}"); f.write("\n")

		text=r"""Prior to association testing, it is useful to infer ancestry in relation to a modern reference panel representing the major human populations. While our particular sample QC process does not directly depend on this information, it is useful to downstream analysis when stratifying the calculation of certain variant statistics that are sensitive to population substructure (eg. Hardy Weinberg equilibrium). Additionally, ancestry inference may identify samples that do not seem to fit into a well-defined major population group, which would allow them to be flagged for removal from association testing."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		bim_list = args.kg_merged_bim.split(",")
		text1 = "the array"
		if len(bim_list) > 1:
			i = 0
			text1 = "each array"
			for a in bim_list:
				i = i + 1
				l = a.split("___")[0]
				m = a.split("___")[1]
				df = pd.read_table(m, header=None)
				if i == 1:
					text2 = "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_") + " variants"
				elif i < len(bim_list)-1:
					text2 = text2 + ", " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_") + " variants"
				else:
					if len(bim_list) == 2:
						text2 = text2 + " and " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_") + " variants"
					else:
						text2 = text2 + ", and " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_") + " variants"
		else:
			l = args.kg_merged_bim.split("___")[0]
			m = args.kg_merged_bim.split("___")[1]
			df = pd.read_table(m, header=None)
			text2 = "{0:,d}".format(df.shape[0]) + " variants"

		text=r"""Initially, {0} was merged with reference data. In this case, the reference used was the entire set of 2,504 1000 Genomes Phase 3 Version 5 \cite{{1KG}} samples and our method restricted this merging to a set of 5,166 known ancestry informative SNPs. The merged data consisted of {1}. After merging, principal components (PCs) were computed using the PC-AiR \cite{{pcair}} method in the GENESIS R package. This particular algorithm allows for the calculation of PCs that reflect ancestry in the presence of known or cryptic relatedness. The 1000 Genomes samples were forced into the "unrelated" set and the PC-AiR algorithm was used to find the "unrelated" samples from the array data. Then PCs were calculated on them and projected onto the remaining samples.""".format(text1, text2)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		pca_list = args.pca_plots.split(",")
		if len(pca_list) > 1:
			i = 0
			for a in pca_list:
				i = i + 1
				l = a.split("___")[0]
				if i == 1:
					text1 = r"Figures \ref{fig:ancestryPcaPlots" + l + r"}"
				elif i < len(pca_list) - 1:
					text1 = text1 + r", \ref{fig:ancestryPcaPlots" + l + r"}"
				else:
					if len(pca_list) == 2:
						text1 = text1 + r" and \ref{fig:ancestryPcaPlots" + l + r"} display"
					else:
						text1 = text1 + r", and \ref{fig:ancestryPcaPlots" + l + r"} display"
		else:
			l = args.pca_plots.split("___")[0]
			text1 = r"Figure \ref{fig:ancestryPcaPlots" + l + r"} displays"
		text=r"""{0} plots of the top three principal components along with the 1000 Genomes major population groups.""".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		for a in pca_list:
			l = a.split("___")[0]
			p = a.split("___")[1]
			text = [
				r"\begin{figure}[H]",
				r"   \centering",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth,page=1]{" + p + r"}",
				r"      \caption{PC1 vs. PC2}",
				r"      \label{fig:ancestryPca1vs2Plot" + l + r"}",
				r"   \end{subfigure}%",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth,page=2]{" + p + r"}",
				r"      \caption{PC2 vs. PC3}",
				r"      \label{fig:ancestryPca2vs3Plot" + l + r"}",
				r"   \end{subfigure}",
				r"   \caption{Principal components of ancestry for " + l.replace("_","\_") + r"}",
				r"   \label{fig:ancestryPcaPlots" + l + r"}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		cluster_list = args.cluster_plots.split(",")
		text1 = "the array"
		if len(cluster_list) > 1:
			i = 0
			for a in cluster_list:
				i = i + 1
				l = a.split("___")[0]
				if i == 1:
					text2 = r"Figures \ref{fig:ancestryClusterPlots" + l + r"}"
				elif i < len(cluster_list) -1:
					text2 = text2 + r", \ref{fig:ancestryClusterPlots" + l + r"}"
				else:
					if len(cluster_list) == 2:
						text2 = text2 + r" and \ref{fig:ancestryClusterPlots" + l + r"} clearly indicate"
					else:
						text2 = text2 + r", and \ref{fig:ancestryClusterPlots" + l + r"} clearly indicate"
		else:
			l = args.cluster_plots.split("___")[0]
			text2 = r"Figure \ref{fig:ancestryClusterPlots" + l + r"} clearly indicates"
		text=r"""Using the principal components of ancestry as features, we employed the signal processing software Klustakwik \cite{{klustakwik}} to model {0} as a mixture of Gaussians, identifying clusters, or population groups/subgroups. In order to generate clusters of sufficient size for statistical association tests, we used the first three principal components as features in the clustering algorithm. This number of PC's distinctly separates the five major 1000 Genomes population groups: AFR, AMR, EUR, EAS, and SAS. {1} the population structure in the datasets. In Klustakwik output, cluster 1 is always reserved for outliers, or samples that did not fit into any of the clusters found by the program.""".format(text1, text2)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		for a in cluster_list:
			l = a.split("___")[0]
			p = a.split("___")[1]
			text = [
				r"\begin{figure}[H]",
				r"   \centering",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth,page=1]{" + p + r"}",
				r"      \caption{PC1 vs. PC2}",
				r"      \label{fig:ancestryClusterPc1vs2Plot" + l + r"}",
				r"   \end{subfigure}%",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth,page=2]{" + p + r"}",
				r"      \caption{PC2 vs. PC3}",
				r"      \label{fig:ancestryClusterPc2vs3Plot" + l + r"}",
				r"   \end{subfigure}",
				r"   \caption{Population clusters for " + l.replace("_","\_") + r"}",
				r"   \label{fig:ancestryClusterPlots" + l + r"}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		if len(bim_list) > 1:
			i = 0
			text1 = "A final population assignment is determined by setting a hierarchy on the genotyping technologies ("
			for a in bim_list:	
				i = i + 1
				l = a.split("___")[0]
				if i == 1:
					text1 = text1 + l.replace("_","\_")
				else:
					text1 = text1 + " > " + l.replace("_","\_")
			text1 = text1 + ") and assigning each sample to the population determined using the highest technology"
		else:
			text1="Table \ref{table:ancestryFinalTable} describes the final population assignments."
		text=r"The resulting clusters are then combined with the nearest 1000 Genomes cohort. Table \ref{{table:ancestryClusterTable}} describes the classification using this method. {0}.".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=[
			r"\begin{ThreePartTable}",
			r"	\pgfplotstabletypeset[",
			r"		font=\footnotesize\sffamily,",
			r"		col sep=tab,",
			r"		columns={Data,Population,Clusters,Samples},",
			r"		column type={>{\fontseries{bx}\selectfont}c},",
			r"		columns/Data/.style={column name=, string type},",
			r"		columns/Population/.style={column name=Population, string type, column type={>{\fontseries{bx}\selectfont}r}},",
			r"		columns/Clusters/.style={column name=Clusters, string type},",
			r"		columns/Samples/.style={column name=Samples, string type, column type={>{\fontseries{bx}\selectfont}l}},",
			r"		postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
			r"		every head row/.append style={before row={\caption{Inferred ancestry by dataset and cluster}\label{table:ancestryClusterTable}\\\toprule}, after row=\midrule\endfirsthead},",
			r"		every first row/.append style={before row={\multicolumn{4}{c}{}\\ \caption[]{... continued from previous page}\\\\\toprule}, after row=\midrule\endhead},",
			r"		every last row/.style={after row=\bottomrule},",
			r"		empty cells with={}",
			r"	]{" + args.cluster_table + r"}"",
			r"\end{ThreePartTable}"]
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text=[
			r"\begin{ThreePartTable}",
			r"	\pgfplotstabletypeset[",
			r"		font=\footnotesize\sffamily,",
			r"		col sep=tab,",
			r"		columns={Population,Samples},",
			r"		column type={>{\fontseries{bx}\selectfont}c},",
			r"		columns/Population/.style={column name=Population, string type, column type={>{\fontseries{bx}\selectfont}r}},",
			r"		columns/Samples/.style={column name=Samples, string type, column type={>{\fontseries{bx}\selectfont}l}},",
			r"		postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
			r"		every head row/.append style={before row={\caption{Final inferred ancestry}\label{table:ancestryFinalTable}\\\toprule}, after row=\midrule\endfirsthead},",
			r"		every first row/.append style={before row={\multicolumn{2}{c}{}\\ \caption[]{... continued from previous page}\\\\\toprule}, after row=\midrule\endhead},",
			r"		every last row/.style={after row=\bottomrule},",
			r"		empty cells with={}",
			r"	]{" + args.final_table + r"}",
			r"	\end{ThreePartTable}"]
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--kg-merged-bim', help='a comma separated list of array labels and bim files from merging array and 1kg data, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pca-plots', help='a comma separated list of array labels and PCA plot pdfs, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--cluster-plots', help='a comma separated list of array labels and PCA cluster plot pdfs, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--cluster-table', help='an ancestry cluster table', required=True)
	requiredArgs.add_argument('--final-table', help='a final ancestry table', required=True)
	args = parser.parse_args()
	main(args)
