import argparse
import numpy as np
import pandas as pd
import pandas.io.common
import collections

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing duplicates and excessive sharing section"
		f.write("\n"); f.write(r"\subsection{Duplicates and Excessive Sharing of Identity-by-Descent (IBD)}"); f.write("\n")

		text=r"Sample pair kinship coefficients were determined using KING \cite{king} relationship inference software, which offers a robust algorithm for relationship inference under population stratification. Prior to inferring relationships, we used Plink \cite{plink} to filter out non-autosomal, non-A/C/G/T, low callrate, and low minor allele frequency variants. Also, variants with positions in known high LD regions \cite{umichHiLd} and known Type 2 diabetes associated loci were removed and an LD-pruned dataset was created. The specific filters that were used are listed below."
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=[
			r"\begin{samepage}",
			r"	\begin{itemize}",
			r"		\item --chr 1-22",
			r"		\item --snps-only just-acgt",
			r"		\item --exclude range ...",
			r"		\item --maf 0.01",
			r"		\item --geno 0.02",
			r"		\item --indep-pairwise 1000kb 1 0.2",
			r"	\end{itemize}",
			r"\end{samepage}"]
		f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text_dict = collections.OrderedDict()
		for x in args.filtered_bim:
			df = pd.read_table(x.split(",")[1], header=None)
			if df.shape[0] > 0:
				text_dict[x.split(",")[0]] = "{0:,d}".format(df.shape[0])

		if len(text_dict) == 0:
			text1 = "no variants"
		if len(text_dict) == 1:
			text1 = text_dict[text_dict.keys()[0]] + " variants"
		if len(text_dict) == 2:
			text1 = " and ".join([str(text_dict[x]) + " " + x.replace("_","\_") for x in text_dict.keys()[0:(len(text_dict.keys()))]]) + " variants"
		elif len(text_dict) > 2:
			text1 = ", ".join([str(text_dict[x]) + " " + x.replace("_","\_") for x in text_dict.keys()[0:(len(text_dict.keys())-1)]]) + " and " + str(text_dict[text_dict.keys()[len(text_dict.keys())-1]]) + " " + text_dict.keys()[len(text_dict.keys())-1].replace("_","\_") + " variants"

		text=r"After filtering there were {0} remaining.".format(text1)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text_dict1 = collections.OrderedDict()
		for x in args.kin0_related:
			df = pd.read_table(x.split(",")[1])
			df = df[df['Kinship'] > 0.4]
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

		text_dict2 = collections.OrderedDict()
		for x in args.restore:
			print "HERE1"
			df = pd.read_table(x.split(",")[1])
			print "HERE2"
			df = df[df['RestoreFrom'] == "duplicatesKeep"]
			if df.shape[0] > 0:
				text_dict2[x.split(",")[0]] = "{0:,d}".format(df.shape[0])

		if len(text_dict2) == 0:
			text2 = "no"
		if len(text_dict2) == 1:
			text2 = text_dict2[text_dict2.keys()[0]]
		if len(text_dict2) == 2:
			text2 = " and ".join([str(text_dict2[x]) + " " + x.replace("_","\_") for x in text_dict2.keys()[0:len(text_dict2.keys())]])
		elif len(text_dict2) > 2:
			text2 = ", ".join([str(text_dict2[x]) + " " + x.replace("_","\_") for x in text_dict2.keys()[0:(len(text_dict2.keys())-1)]]) + " and " + str(text_dict2[text_dict2.keys()[len(text_dict2.keys())-1]]) + " " + text_dict2.keys()[len(text_dict2.keys())-1].replace("_","\_")

		text=r"In order to identify duplicate pairs of samples, a filter was set to $Kinship > 0.4$. There were {0} sample pairs identified as duplicate in the array data. Upon manual inspection, If the clinical data for any of the duplicate pairs was nearly identical (same date of birth, etc.), then the sample with the higher call rate was reinstated. If the clinical data did not match, both samples were removed. In this case, {1} samples have been reinstated. More information is available upon request".format(text1, text2)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")


		text_dict1 = collections.OrderedDict()
		for x in args.famsizes:
			try:
				df = pd.read_table(x.split(",")[1], header=None)
			except pandas.io.common.EmptyDataError:
				print "skipping empty famsize file " + x.split(",")[1]
				df = pd.DataFrame()
			else:
				df = df[df[1] >= 10]	
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

		text_dict2 = collections.OrderedDict()
		for x in args.restore:
			df = pd.read_table(x.split(",")[1])
			df = df[df['RestoreFrom'] == "famsizeKeep"]
			if df.shape[0] > 0:
				text_dict2[x.split(",")[0]] = "{0:,d}".format(df.shape[0])

		if len(text_dict2) == 0:
			text2 = "no"
		if len(text_dict2) == 1:
			text2 = text_dict2[text_dict2.keys()[0]]
		if len(text_dict2) == 2:
			text2 = " and ".join([str(text_dict2[x]) + " " + x.replace("_","\_") for x in text_dict2.keys()[0:len(text_dict2.keys())]])
		elif len(text_dict2) > 2:
			text2 = ", ".join([str(text_dict2[x]) + " " + x.replace("_","\_") for x in text_dict2.keys()[0:(len(text_dict2.keys())-1)]]) + " and " + str(text_dict2[text_dict2.keys()[len(text_dict2.keys())-1]]) + " " + text_dict2.keys()[len(text_dict2.keys())-1].replace("_","\_")

		text=r"In addition to identifying duplicate samples, any single individual that exhibited kinship values indicating a 2nd degree relative or higher relationship with 10 or more others was flagged for removal. The relationship count indicated {0} samples that exhibited high levels of sharing identity by descent. Upon further inspection, {1} samples were manually reinstated during this step. More information is available upon request".format(text1, text2)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		print "writing sex check section"
		f.write("\n"); f.write(r"\subsection{Sex Chromosome Check}"); f.write("\n")

		text1_dict = collections.OrderedDict()
		text2_dict = collections.OrderedDict()
		for x in args.sexcheck_problems:
			df = pd.read_table(x.split(",")[1])
			if df.shape[0] > 0:
				df_nomatch = df[~np.isnan(df['is_female'])]
				df_noimpute = df[np.isnan(df['is_female'])]
				if df_nomatch.shape[0] > 0:
					text1_dict[x.split(",")[0]] = "{0:,d}".format(df_nomatch.shape[0])
				if df_noimpute.shape[0] > 0:
					text2_dict[x.split(",")[0]] = "{0:,d}".format(df_noimpute.shape[0])

		text1 = "There were " if len(text1_dict) != 1 or text1_dict[text1_dict.keys()[0]] != '1' else "There was "
		if len(text1_dict) == 0:
			text1 = text1 + "no"
		if len(text1_dict) == 1:
			text1 = text1 + str(text1_dict[text1_dict.keys()[0]])
		if len(text1_dict) == 2:
			text1 = text1 + " and ".join([str(text1_dict[x]) + " " + x.replace("_","\_") for x in text1_dict.keys()[0:len(text1_dict.keys())]])
		elif len(text1_dict) > 2:
			text1 = text1 + ", ".join([str(text1_dict[x]) + " " + x.replace("_","\_") for x in text1_dict.keys()[0:(len(text1_dict.keys())-1)]]) + " and " + str(text1_dict[text1_dict.keys()[len(text1_dict.keys())-1]]) + " " + text1_dict.keys()[len(text1_dict.keys())-1].replace("_","\_")
		text1 = text1 + " samples that were" if len(text1_dict) != 1 or text1_dict[text1_dict.keys()[0]] != '1' else text1 + " sample that was"

		text2 = "there were " if len(text2_dict) != 1 or text2_dict[text2_dict.keys()[0]] != '1' else "there was "
		if len(text2_dict) == 0:
			text2 = text2 + "no"
		if len(text2_dict) == 1:
			text2 = text2 + str(text2_dict[text2_dict.keys()[0]])
		if len(text2_dict) == 2:
			text2 = text2 + " and ".join([str(text2_dict[x]) + " " + x.replace("_","\_") for x in text2_dict.keys()[0:len(text2_dict.keys())]])
		elif len(text2_dict) > 2:
			text2 = text2 + ", ".join([str(text2_dict[x]) + " " + x.replace("_","\_") for x in text2_dict.keys()[0:(len(text2_dict.keys())-1)]]) + " and " + str(text2_dict[text2_dict.keys()[len(text2_dict.keys())-1]]) + " " + text2_dict.keys()[len(text2_dict.keys())-1].replace("_","\_")
		text2 = text2 + " samples that were" if len(text2_dict) != 1 or text2_dict[text2_dict.keys()[0]] != '1' else text2 + " sample that was"

		text_dict3 = collections.OrderedDict()
		for x in args.restore:
			df = pd.read_table(x.split(",")[1])
			df = df[df['RestoreFrom'] == "sexcheckKeep"]
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

		text=r"Each array was checked for genotype / clinical data agreement for sex. {0} flagged as a 'PROBLEM' by Hail because it was unable to impute sex and {1} flagged for removal because the genotype based sex did not match their clinical sex. Upon further inspection, {2} samples were manually reinstated during this step. More information is available upon request".format(text1, text2, text3)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--filtered-bim', nargs='+', help='a space separated list of array labels and filtered bim files, each separated by comma', required=True)
	requiredArgs.add_argument('--kin0-related', nargs='+', help='a space separated list of array labels and kin0 related files, each separated by comma', required=True)
	requiredArgs.add_argument('--famsizes', nargs='+', help='a space separated list of array labels and famsizes files, each separated by comma', required=True)
	requiredArgs.add_argument('--restore', nargs='+', help='a space separated list of array labels and sample restore files, each separated by comma', required=True)
	requiredArgs.add_argument('--sexcheck-problems', nargs='+', help='a space separated list of array labels and sexcheck problems files, each separated by comma', required=True)
	args = parser.parse_args()
	main(args)
