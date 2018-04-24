import argparse
import numpy as np
import pandas as pd
import pandas.io.common

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing duplicates and excessive sharing section"
		f.write("\n"); f.write(r"\subsection{Duplicates and Excessive Sharing of Identity-by-Descent (IBD)}"); f.write("\n")

		text=r"Sample pair kinship coefficients were determined using KING \cite{king} relationship inference software, which offers a robust algorithm for relationship inference under population stratification. Prior to inferring relationships, we filtered variants with low callrate, variants with low minor allele frequency, variants with positions in known high LD regions \cite{umichHiLd}, and known Type 2 diabetes associated loci using the software Hail \cite{hail}. Then an LD pruned dataset was created. The specific filters that were used are listed below."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=[
			r"\begin{samepage}",
			r"	\begin{itemize}",
			r"		\item v.altAllele.isSNP",
			r"		\item ! v.altAllele.isComplex",
			r"		\item {[`A',`C',`G',`T']}.toSet.contains(v.altAllele.ref)",
			r"		\item {[`A',`C',`G',`T']}.toSet.contains(v.altAllele.alt)",
			r"		\item va.qc.AF >= 0.01",
			r"		\item va.qc.callRate >= 0.98",
			r"	\end{itemize}",
			r"\end{samepage}"]
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		if len(args.filtered_bim) > 1:
			i = 0
			for x in args.filtered_bim:
				i = i + 1
				array = x.split(",")[0]
				df = pd.read_table(x.split(",")[1], header=None)
				if i == 1:
					text1 = "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_") + " variants"
				elif i < len(args.filtered_bim)-1:
					text1 = text1 + ", " + "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_") + " variants"
				else:
					if len(args.filtered_bim) == 2:
						text1 = text1 + " and " + "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_") + " variants"
					else:
						text1 = text1 + ", and " + "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_") + " variants"
		else:
			df = pd.read_table(args.filtered_bim.split(",")[1], header=None)
			text1 = "{0:,d}".format(df.shape[0]) + " variants"
		text=r"After filtering there were {0} remaining.".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		if len(args.kin0_related) > 1:
			i = 0
			for x in args.kin0_related:
				i = i + 1
				array = x.split(",")[0]
				df = pd.read_table(x.split(",")[1])
				df = df[df['Kinship'] > 0.4]
				if i == 1:
					text1 = "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_")
				elif i < len(args.kin0_related)-1:
					text1 = text1 + ", " + "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_")
				else:
					if len(args.kin0_related) == 2:
						text1 = text1 + " and " + "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_")
					else:
						text1 = text1 + ", and " + "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_")
		else:
			df = pd.read_table(args.kin0_related.split(",")[1])
			df = df[df['Kinship'] > 0.4]
			text1 = "{0:,d}".format(df.shape[0])
		text=r"In order to identify duplicate pairs of samples, a filter was set to $Kinship > 0.4$. There were {0} sample pairs identified as duplicate in the array data.".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		if len(args.famsizes) > 0:
			i = 0
			for x in args.famsizes:
				i = i + 1
				array = x.split(",")[0]
				try:
					df = pd.read_table(x.split(",")[1], header=None)
				except pandas.io.common.EmptyDataError:
					print "skipping empty famsize file " + x.split(",")[1]
					df = pd.DataFrame()
				else:
					df = df[df[1] >= 10]
				if i == 1:
					text1 = "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_")
				elif i < len(args.famsizes) -1:
					text1 = text1 + ", " + "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_")
				else:
					if len(args.famsizes) == 2:
						text1 = text1 + " and " + "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_")
					else:
						text1 = text1 + ", and " + "{0:,d}".format(df.shape[0]) + " " + array.replace("_","\_")
		else:
			df = pd.read_table(args.famsizes.split(",")[1], header=None)
			text1 = "{0:,d}".format(df.shape[0])
		text=r"In addition to identifying duplicate samples, any single individual that exhibited kinship values indicating a 2nd degree relative or higher relationship with 10 or more others was flagged for removal. The relationship count indicated {0} samples that exhibited high levels of sharing identity by descent.".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		print "writing sex check section"
		f.write("\n"); f.write(r"\subsection{Sex Chromosome Check}"); f.write("\n")

		text_nomatch=""
		text_noimpute={}
		if len(args.sexcheck_problems) > 0:
			i = 0
			for x in args.sexcheck_problems:
				i = i + 1
				array = x.split(",")[0]
				df = pd.read_table(x.split(",")[1])
				if df.shape[0] > 0:
					nnomatch = df[~np.isnan(df['isFemale'])].shape[0]
					nnoimpute = df[np.isnan(df['isFemale'])].shape[0]
				else:
					nnomatch = 0
					nnoimpute = 0
				if i == 1:
					text_nomatch = str(nnomatch) + " " + array.replace("_","\_")
					text_noimpute = str(nnoimpute) + " " + array.replace("_","\_")
				elif i < len(args.sexcheck_problems) -1:
					text_nomatch = text_nomatch + ", " + str(nnomatch) + " " + array.replace("_","\_")
					text_noimpute = text_noimpute + ", " + str(nnoimpute) + " " + array.replace("_","\_")
				else:
					if len(args.sexcheck_problems) == 2:
						text_nomatch = text_nomatch + " and " + str(nnomatch) + " " + array.replace("_","\_")
						text_noimpute = text_noimpute + " and " + str(nnoimpute) + " " + array.replace("_","\_")
					else:
						text_nomatch = text_nomatch + ", and " + str(nnomatch) + " " + array.replace("_","\_")
						text_noimpute = text_noimpute + ", and " + str(nnoimpute) + " " + array.replace("_","\_")
		else:
			df = pd.read_table(args.sexcheck_problems.split(",")[1])
			if df.shape[0] > 0:
				nnomatch = df[~np.isnan(df['isFemale'])].shape[0]
				nnoimpute = df[np.isnan(df['isFemale'])].shape[0]
			else:
				nnomatch = 0
				nnoimpute = 0
		text=r"Each array was checked for genotype / clinical data agreement for sex. There were {0} samples that were flagged as a 'PROBLEM' by Hail because it was unable to impute sex and there were {1} samples that were flagged for removal because the genotype based sex did not match their clinical sex.".format(text_noimpute, text_nomatch)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--filtered-bim', nargs='+', help='a comma separated list of array labels and filtered bim files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--kin0-related', nargs='+', help='a comma separated list of array labels and kin0 related files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--famsizes', nargs='+', help='a comma separated list of array labels and famsizes files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--sexcheck-problems', nargs='+', help='a comma separated list of array labels and sexcheck problems files, each separated by 3 underscores', required=True)
	args = parser.parse_args()
	main(args)
