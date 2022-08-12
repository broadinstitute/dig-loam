import argparse
import pysam
import numpy as np
import pandas as pd
import sys

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		## begin document
		f.write("\n"); f.write(r"\begin{document}"); f.write("\n")

		## title page
		f.write("\n"); f.write(r"\title{AMP-DCC Quality Control Report \\")
		f.write("\n"); f.write(args.id.replace("_","\_") + "}"); f.write("\n")
		f.write("\n"); f.write(r"\date{\mmddyyyydate\today\ (\currenttime)}"); f.write("\n")
		f.write("\n"); f.write(r"\maketitle"); f.write("\n")

		if len(args.authors.split(",")) == 1:
			authors = args.authors
		else:
			a = args.authors.split(",")
			authors = a[0]
			for author in a[1:]:
				if author == a[-1]:
					authors = authors + " and " + author
				else:
					authors = authors + ", " + author

		f.write("\n"); f.write("Prepared by " + authors.replace("_","\_") + " on behalf of the AMP-DCC Data Analysis Team at " + args.organization.replace("_","\_")); f.write("\n")
		f.write("\n"); f.write(r"\bigskip"); f.write("\n")
		f.write("\n"); f.write(r"Contact: \href{mailto:" + args.email.replace("_","\_") + "}{" + args.email.replace("_","\_") + "}"); f.write("\n")
		f.write("\n"); f.write(r"\bigskip"); f.write("\n")
		f.write("\n"); f.write("This document was generated using Loamstream \cite{Loamstream} and the AMP-DCC Data Analysis Pipeline \cite{Pipeline}"); f.write("\n")

		## table of contents
		f.write("\n"); f.write(r"\tableofcontents"); f.write("\n")

		## introduction
		nArrays = len(args.array_data)
		nGwas = 0
		nWes = 0
		nWgs = 0
		samples = []
		for a in args.array_data:
			aTech = a.split(",")[0]
			if aTech == "gwas": nGwas = nGwas+1
			if aTech == "wes": nWes = nWes+1
			if aTech == "wgs": nWgs = nWgs+1
			aType = a.split(",")[1]
			aFile = a.split(",")[2]
			if aType == "vcf":
				print "loading vcf file " + aFile
				try:
					handle=pysam.Tabixfile(filename=aFile)
				except:
					sys.exit("failed to load vcf file " + aFile)
				else:
					samples = samples + [a for a in handle.header][-1].split('\t')[9:]
			elif aType == "plink":
				handle = pd.read_table(aFile + ".fam", header=None, sep = None)
				handle.columns = ['fid','iid','fat','mot','sex','pheno']
				samples = samples + handle['iid'].tolist()
			else:
				sys.exit("failed to load file of unsupported type " + aType)
		samples = set(samples)
		nSamples = len(samples)

		print "writing introduction"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Introduction}"); f.write("\n")

		geno_platforms_text = []
		harmonization_text = "The"
		if nGwas == 1:
			geno_platforms_text = geno_platforms_text + ["a microarray batch"]
			harmonization_text = "After harmonizing the microarray with modern reference data, the"
		elif nGwas > 1:
			geno_platforms_text = geno_platforms_text + ["{0:d} microarray batches".format(nGwas)]
			harmonization_text = "After harmonizing each microarray with modern reference data, the"
		
		if nWes == 1:
			geno_platforms_text = geno_platforms_text + ["a single whole exome sequencing batch"]
		elif nWes > 1:
			geno_platforms_text = geno_platforms_text + ["{0:d} whole exome sequencing batches".format(nWes)]
		
		if nWgs == 1:
			geno_platforms_text = geno_platforms_text + ["a single whole genome sequencing batch"]
		elif nWgs > 1:
			geno_platforms_text = geno_platforms_text + ["{0:d} whole genome sequencing batches".format(nWes)]
		
		if len(geno_platforms_text) == 1:
			geno_platforms_text_merged = geno_platforms_text[0]
		elif len(geno_platforms_text) == 2:
			geno_platforms_text_merged = " and ".join(geno_platforms_text)
		elif len(geno_platforms_text) > 2:
			geno_platforms_text_merged = ", ".join(geno_platforms_text[:len(geno_platforms_text)-1]) + ", and " + geno_platforms_text[len(geno_platforms_text)-1]
		else:
			sys.exit("failed to configure geno_platforms_text")

		text = "This document contains details of our in-house quality control procedure and its application to the {0:s} dataset. We received individual level data for {1:,d} unique samples with genotypic variants determined via {2:s}. Quality control was performed on these data to detect samples and variants that did not fit our standards for inclusion in association testing. {3:s} highest quality variants were used in a battery of tests to assess the quality of each sample. Duplicate pairs, samples exhibiting excessive sharing of identity by descent, samples whose genotypic sex did not match their clinical sex, and outliers detected among several sample-by-variant statistics have been flagged for removal from further analysis. Additionally, genotypic ancestry was inferred with respect to a modern reference panel, allowing for variant filtering and association analyses to be performed within population as needed.".format(args.id, nSamples, geno_platforms_text_merged, harmonization_text)
		if nArrays > 1:
			text = text + "With the exception of inferring each samples ancestry, QC was performed on each batch separately as much as possible, allowing for flexibility in the way the data can be used in downstream analyses."
		f.write("\n"); f.write(text.replace("_","\_").encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--id', help='a project ID', required=True)
	requiredArgs.add_argument('--authors', help='a comma separated list of authors', required=True)
	requiredArgs.add_argument('--organization', help='an organization name', required=True)
	requiredArgs.add_argument('--email', help='an email address', required=True)
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--array-data', nargs='+', help='a list of array data (plink binary file name or vcf file) each a comma delimited datatype (bfile or vcf) and data file pair', required=True)
	args = parser.parse_args()
	main(args)
