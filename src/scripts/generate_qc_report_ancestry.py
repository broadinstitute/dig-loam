import argparse
import numpy as np
import pandas as pd
import collections

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Sample QC}"); f.write("\n")
		f.write("\n"); f.write(r"\subsection{Ancestry Inference}"); f.write("\n")

		text=r"Prior to association testing, it is useful to infer ancestry in relation to a modern reference panel representing the major human populations. While our particular sample QC process does not directly depend on this information, it is useful to downstream analysis when stratifying the calculation of certain variant statistics that are sensitive to population substructure (eg. Hardy Weinberg equilibrium). Additionally, ancestry inference may identify samples that do not seem to fit into a well-defined major population group, which would allow them to be flagged for removal from association testing."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text1 = "each array" if len(args.kg_merged_bim) > 1 else "the array"
		text2_dict = collections.OrderedDict()
		for x in args.kg_merged_bim:
			df = pd.read_table(x.split(",")[1], header=None)
			if df.shape[0] > 0:
				text2_dict[x.split(",")[0]] = "{0:,d}".format(df.shape[0])

		if len(text2_dict) == 0:
			text2 = "no variants"
		if len(text2_dict) == 1:
			text2 = text2_dict[text2_dict.keys()[0]] + " variants"
		if len(text2_dict) == 2:
			text2 = " and ".join([str(text2_dict[x]) + " " + x.replace("_","\_") for x in text2_dict.keys()[0:len(text2_dict.keys())]]) + " variants"
		elif len(text2_dict) > 2:
			text2 = ", ".join([str(text2_dict[x]) + " " + x.replace("_","\_") for x in text2_dict.keys()[0:(len(text2_dict.keys())-1)]]) + " and " + str(text2_dict[text2_dict.keys()[len(text2_dict.keys())-1]]) + " " + text2_dict.keys()[len(text2_dict.keys())-1].replace("_","\_") + " variants"

		text=r"Initially, {0} was merged with reference data. In this case, the reference used was the entire set of 2,504 1000 Genomes Phase 3 Version 5 \cite{{1KG}} samples and our method restricted this merging to a set of 5,166 known ancestry informative SNPs. The merged data consisted of {1}. After merging, principal components (PCs) were computed using the PC-AiR \cite{{pcair}} method in the GENESIS R package. This particular algorithm allows for the calculation of PCs that reflect ancestry in the presence of known or cryptic relatedness. The 1000 Genomes samples were forced into the 'unrelated' set and the PC-AiR algorithm was used to find the 'unrelated' samples from the array data. Then PCs were calculated on them and projected onto the remaining samples.".format(text1, text2)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		if len(args.pca_plots) > 1:
			i = 0
			for x in args.pca_plots:
				i = i + 1
				array = x.split(",")[0]
				if i == 1:
					text1 = r"Figures \ref{fig:ancestryPcaPlots" + array.replace("_","") + r"}"
				elif i < len(args.pca_plots):
					text1 = text1 + r", \ref{fig:ancestryPcaPlots" + array.replace("_","") + r"}"
				else:
					if len(args.pca_plots) == 2:
						text1 = text1 + r" and \ref{fig:ancestryPcaPlots" + array.replace("_","") + r"} display"
					else:
						text1 = text1 + r", and \ref{fig:ancestryPcaPlots" + array.replace("_","") + r"} display"
		else:
			array = args.pca_plots.split(",")[0]
			text1 = r"Figure \ref{fig:ancestryPcaPlots" + array + r"} displays"
		text=r"{0} plots of the top three principal components along with the 1000 Genomes major population groups.".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		for x in args.pca_plots:
			array = x.split(",")[0]
			plot = x.split(",")[1]
			text = [
				r"\begin{figure}[H]",
				r"   \centering",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth,page=1]{" + plot + r"}",
				r"      \caption{PC1 vs. PC2}",
				r"      \label{fig:ancestryPca1vs2Plot" + array.replace("_","") + r"}",
				r"   \end{subfigure}%",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth,page=2]{" + plot + r"}",
				r"      \caption{PC2 vs. PC3}",
				r"      \label{fig:ancestryPca2vs3Plot" + array.replace("_","") + r"}",
				r"   \end{subfigure}",
				r"   \caption{Principal components of ancestry for " + array.replace("_","\_") + r"}",
				r"   \label{fig:ancestryPcaPlots" + array.replace("_","") + r"}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text1 = "the array"
		if len(args.cluster_plots) > 1:
			i = 0
			for x in args.cluster_plots:
				i = i + 1
				array = x.split(",")[0]
				if i == 1:
					text2 = r"Figures \ref{fig:ancestryClusterPlots" + array.replace("_","") + r"}"
				elif i < len(args.cluster_plots):
					text2 = text2 + r", \ref{fig:ancestryClusterPlots" + array.replace("_","") + r"}"
				else:
					if len(args.cluster_plots) == 2:
						text2 = text2 + r" and \ref{fig:ancestryClusterPlots" + array.replace("_","") + r"} clearly indicate"
					else:
						text2 = text2 + r", and \ref{fig:ancestryClusterPlots" + array.replace("_","") + r"} clearly indicate"
		else:
			array = args.cluster_plots.split(",")[0]
			text2 = r"Figure \ref{fig:ancestryClusterPlots" + array.replace("_","") + r"} clearly indicates"

		text_dict3 = collections.OrderedDict()
		for x in args.restore:
			df = pd.read_table(x.split(",")[1])
			df = df[df['RestoreFrom'] == "ancestryOutliersKeep"]
			if df.shape[0] > 0:
				text_dict3[x.split(",")[0]] = "{0:,d}".format(df.shape[0])

		if len(text_dict3) == 0:
			text3 = "no"
		if len(text_dict3) == 1:
			text3 = text_dict3[text_dict3.keys()[0]]
		if len(text_dict3) == 2:
			text3 = " and ".join([str(text_dict3[x]) + " " + x.replace("_","\_") for x in text_dict3.keys()[0:len(text_dict3.keys())]])
		elif len(text_dict3) > 2:
			text3 = ", ".join([str(text_dict3[x]) + " " + x.replace("_","\_") for x in text_dict3.keys()[0:(len(text_dict3.keys())-1)]]) + " and " + str(text_dict3[text_dict3.keys()[len(text_dict3.keys())-1]]) + " " + text_dict3.keys()[len(text_dict3.keys())-1].replace("_","\_")

		text=r"Using the principal components of ancestry as features, we employed the signal processing software Klustakwik \cite{{klustakwik}} to model {0} as a mixture of Gaussians, identifying clusters, or population groups/subgroups. In order to generate clusters of sufficient size for statistical association tests, we used the first {1} principal components as features in the clustering algorithm. This number of PC's distinctly separates the five major 1000 Genomes population groups: AFR, AMR, EUR, EAS, and SAS. {2} the population structure in the datasets. In Klustakwik output, cluster 1 is always reserved for outliers, or samples that did not fit into any of the clusters found by the program. Upon further inspection, {3} samples were manually reinstated during this step. More information is available upon request".format(text1, str(args.features), text2, text3)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		for x in args.cluster_plots:
			array = x.split(",")[0]
			plot = x.split(",")[1]
			text = [
				r"\begin{figure}[H]",
				r"   \centering",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth,page=1]{" + plot + r"}",
				r"      \caption{PC1 vs. PC2}",
				r"      \label{fig:ancestryClusterPc1vs2Plot" + array.replace("_","") + r"}",
				r"   \end{subfigure}%",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth,page=2]{" + plot + r"}",
				r"      \caption{PC2 vs. PC3}",
				r"      \label{fig:ancestryClusterPc2vs3Plot" + array.replace("_","") + r"}",
				r"   \end{subfigure}",
				r"   \caption{Population clusters for " + array.replace("_","\_") + r"}",
				r"   \label{fig:ancestryClusterPlots" + array.replace("_","") + r"}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		if len(args.kg_merged_bim) > 1:
			i = 0
			text1 = "A final population assignment is determined by setting a hierarchy on the genotyping technologies ("
			for x in args.kg_merged_bim:	
				i = i + 1
				array = x.split(",")[0]
				if i == 1:
					text1 = text1 + array.replace("_","\_")
				else:
					text1 = text1 + " > " + array.replace("_","\_")
			text1 = text1 + ") and assigning each sample to the population determined using the highest technology"
		else:
			text1="Table \ref{table:ancestryFinalTable} describes the final population assignments."
		text=r"The resulting clusters are then combined with the nearest 1000 Genomes cohort. Table \ref{{table:ancestryClusterTable}} describes the classification using this method. {0}.".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=[
			r"\begin{table}[H]",
			r"	\caption{Inferred ancestry by dataset and cluster}",
			r"	\label{table:ancestryClusterTable}",
			r"	\begin{center}",
			r"	\pgfplotstabletypeset[",
			r"		font=\footnotesize\sffamily,",
			r"		string replace={NA}{},",
			r"		col sep=tab,",
			r"		columns={Data,Population,Clusters,Samples},",
			r"		column type={>{\fontseries{bx}\selectfont}c},",
			r"		columns/Data/.style={column name=, string type},",
			r"		columns/Population/.style={column name=Population, string type, column type={>{\fontseries{bx}\selectfont}r}},",
			r"		columns/Clusters/.style={column name=Clusters, string type},",
			r"		columns/Samples/.style={column name=Samples, string type, column type={>{\fontseries{bx}\selectfont}l}},",
			r"		postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
			r"		every head row/.append style={",
			r"			before row=",
			r"				\toprule,",
			r"			after row=",
			r"				\midrule",
			r"		},"]
		clus = pd.read_table(args.cluster_table,sep="\t")
		j = 0
		for idx, row in clus.iterrows():
			if not pd.isnull(row['Data']):
				j = j + 1
			if j % 2 != 0:
				text.extend([r"		grayrow=" + str(idx+1) + r","])
		text.extend([
			r"		every last row/.style={",
			r"			after row=",
			r"				\bottomrule",
			r"		},",
			r"		empty cells with={}",
			r"	]{" + args.cluster_table + r"}",
			r"	\end{center}",
			r"\end{table}"])
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text=[
			r"\begin{table}[H]",
			r"	\caption{Final inferred ancestry}",
			r"	\label{table:ancestryFinalTable}",
			r"	\begin{center}",
			r"	\pgfplotstabletypeset[",
			r"		font=\footnotesize\sffamily,",
			r"		string replace={NA}{},",
			r"		col sep=tab,",
			r"		columns={Population,Samples},",
			r"		column type={>{\fontseries{bx}\selectfont}c},",
			r"		columns/Population/.style={column name=Population, string type, column type={>{\fontseries{bx}\selectfont}r}},",
			r"		columns/Samples/.style={column name=Samples, string type, column type={>{\fontseries{bx}\selectfont}l}},",
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
			r"	]{" + args.final_table + r"}",
			r"	\end{center}",
			r"\end{table}"]
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--kg-merged-bim', nargs='+', help='a list of array labels and bim files from merging array and 1kg data, each separated by comma', required=True)
	requiredArgs.add_argument('--features', help='an integer indicating how many PCs were used for ancestry inference', required=True)
	requiredArgs.add_argument('--pca-plots', nargs='+', help='a list of array labels and PCA plot pdfs, each separated by comma', required=True)
	requiredArgs.add_argument('--cluster-plots', nargs='+', help='a list of array labels and PCA cluster plot pdfs, each separated by comma', required=True)
	requiredArgs.add_argument('--cluster-table', help='an ancestry cluster table', required=True)
	requiredArgs.add_argument('--restore', nargs='+', help='a space separated list of array labels and sample restore files, each separated by comma', required=True)
	requiredArgs.add_argument('--final-table', help='a final ancestry table', required=True)
	args = parser.parse_args()
	main(args)
