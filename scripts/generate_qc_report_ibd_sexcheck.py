import argparse
import numpy as np
import pandas as pd
import pandas.io.common

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing duplicates and excessive sharing section"
		f.write("\n"); f.write(r"\subsection{Duplicates and Excessive Sharing of Identity-by-Descent (IBD)}"); f.write("\n")

		text=r"""Sample pair kinship coefficients were determined using KING \cite{king} relationship inference software, which offers a robust algorithm for relationship inference under population stratification. Prior to inferring relationships, we filtered variants with low callrate, variants with low minor allele frequency, variants with positions in known high LD regions \cite{umichHiLd}, and known Type 2 diabetes associated loci using the software Hail \cite{hail}. Then an LD pruned dataset was created. The specific filters that were used are listed below."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{itemize}
			\item v.altAllele.isSNP 
			\item ! v.altAllele.isComplex
			\item {["A","C","G","T"]}.toSet.contains(v.altAllele.ref)
			\item {["A","C","G","T"]}.toSet.contains(v.altAllele.alt)
			\item va.qc.AF >= 0.01
			\item va.qc.callRate >= 0.98
		\end{itemize}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		bim_list = args.filtered_bim.split(",")
		if len(bim_list) > 1:
			i = 0
			for a in bim_list:
				i = i + 1
				l = a.split("___")[0]
				m = a.split("___")[1]
				df = pd.read_table(m, header=None)
				if i == 1:
					text1 = "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_") + " variants"
				elif i < len(bim_list)-1:
					text1 = text1 + ", " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_") + " variants"
				else:
					if len(bim_list) == 2:
						text1 = text1 + " and " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_") + " variants"
					else:
						text1 = text1 + ", and " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_") + " variants"
		else:
			m = args.filtered_bim.split("___")[1]
			df = pd.read_table(m, header=None)
			text1 = "{0:,d}".format(df.shape[0]) + " variants"
		text=r"""After filtering there were {0} remaining.""".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		kin0_list = args.kin0_related.split(",")
		if len(kin0_list) > 1:
			i = 0
			for a in kin0_list:
				i = i + 1
				l = a.split("___")[0]
				m = a.split("___")[1]
				df = pd.read_table(m)
				df = df[df['Kinship'] > 0.4]
				if i == 1:
					text1 = "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_")
				elif i < len(kin0_list)-1:
					text1 = text1 + ", " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_")
				else:
					if len(kin0_list) == 2:
						text1 = text1 + " and " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_")
					else:
						text1 = text1 + ", and " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_")
		else:
			l = args.kin0_related.split("___")[0]
			m = args.kin0_related.split("___")[1]
			df = pd.read_table(m)
			df = df[df['Kinship'] > 0.4]
			text1 = "{0:,d}".format(df.shape[0])
		text=r"""In order to identify duplicate pairs of samples, a filter was set to $Kinship > 0.4$. There were {0} sample pairs identified as duplicate in the array data.""".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		fam_list = args.famsizes.split(",")
		if len(fam_list) > 0:
			i = 0
			for a in fam_list:
				i = i + 1
				l = a.split("___")[0]
				m = a.split("___")[1]
				try:
					df = pd.read_table(m, header=None)
				except pandas.io.common.EmptyDataError:
					print "skipping empty famsize file " + m
					df = pd.DataFrame()
				else:
					df = df[df[1] >= 10]
				if i == 1:
					text1 = "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_")
				elif i < len(bim_list) -1:
					text1 = text1 + ", " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_")
				else:
					if len(bim_list) == 2:
						text1 = text1 + " and " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_")
					else:
						text1 = text1 + ", and " + "{0:,d}".format(df.shape[0]) + " " + l.replace("_","\_")
		else:
			l = a.split("___")[0]
			m = a.split("___")[1]
			df = pd.read_table(m, header=None)
			text1 = "{0:,d}".format(df.shape[0])
		text=r"""In addition to identifying duplicate samples, any single individual that exhibited kinship values indicating a 2nd degree relative or higher relationship with 10 or more others was flagged for removal. The relationship count indicated {0} samples that exhibited high levels of sharing identity by descent.""".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		print "writing sex check section"
		f.write("\n"); f.write(r"\subsection{Sex Chromosome Check}"); f.write("\n")

		sex_list = args.sexcheck_problems.split(",")
		text_nomatch=""
		text_noimpute={}
		if len(sex_list) > 0:
			i = 0
			for a in sex_list:
				i = i + 1
				l = a.split("___")[0]
				m = a.split("___")[1]
				df = pd.read_table(m)
				if df.shape[0] > 0:
					nnomatch = df[~np.isnan(df['isFemale'])].shape[0]
					nnoimpute = df[np.isnan(df['isFemale'])].shape[0]
				else:
					nnomatch = 0
					nnoimpute = 0
				if i == 1:
					text_nomatch = str(nnomatch) + " " + l.replace("_","\_")
					text_noimpute = str(nnoimpute) + " " + l.replace("_","\_")
				elif i < len(sex_list) -1:
					text_nomatch = text_nomatch + ", " + str(nnomatch) + " " + l.replace("_","\_")
					text_noimpute = text_noimpute + ", " + str(nnoimpute) + " " + l.replace("_","\_")
				else:
					if len(sex_list) == 2:
						text_nomatch = text_nomatch + " and " + str(nnomatch) + " " + l.replace("_","\_")
						text_noimpute = text_noimpute + " and " + str(nnoimpute) + " " + l.replace("_","\_")
					else:
						text_nomatch = text_nomatch + ", and " + str(nnomatch) + " " + l.replace("_","\_")
						text_noimpute = text_noimpute + ", and " + str(nnoimpute) + " " + l.replace("_","\_")
		else:
			l = args.sexcheck_problems.split("___")[0]
			m = args.sexcheck_problems.split("___")[1]
			df = pd.read_table(m)
			if df.shape[0] > 0:
				nnomatch = df[~np.isnan(df['isFemale'])].shape[0]
				nnoimpute = df[np.isnan(df['isFemale'])].shape[0]
			else:
				nnomatch = 0
				nnoimpute = 0
		text=r"""Each array was checked for genotype / clinical data agreement for sex. There were {0} samples that were flagged as a 'PROBLEM' by Hail because it was unable to impute sex and there were {1} samples that were flagged for removal because the genotype based sex did not match their clinical sex.""".format(text_noimpute, text_nomatch)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--filtered-bim', help='a comma separated list of array labels and filtered bim files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--kin0-related', help='a comma separated list of array labels and kin0 related files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--famsizes', help='a comma separated list of array labels and famsizes files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--sexcheck-problems', help='a comma separated list of array labels and sexcheck problems files, each separated by 3 underscores', required=True)
	args = parser.parse_args()
	main(args)
