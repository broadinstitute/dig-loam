import argparse
import pysam
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		## begin document
		f.write("\n"); f.write(r"\begin{document}"); f.write("\n")

		## title page
		f.write("\n"); f.write(r"\title{AMP-DCC Quality Control Report \\")
		f.write("\n"); f.write(args.id.replace("_","\_") + "}"); f.write("\n")
		f.write("\n"); f.write(r"\date{\today}"); f.write("\n")
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

		f.write("\n"); f.write("Prepared by " + authors + " on behalf of the AMP-DCC Analysis Team"); f.write("\n")
		f.write("\n"); f.write(r"\bigskip"); f.write("\n")
		f.write("\n"); f.write(r"Contact: AMP-DCC Analysis Team (\href{mailto:amp-dcc-dat@broadinstitute.org}{amp-dcc-dat@broadinstitute.org})"); f.write("\n")

		## table of contents
		f.write("\n"); f.write(r"\tableofcontents"); f.write("\n")

		## introduction
		nArrays = len(args.array_data)
		samples = []
		for a in args.array_data:
			aType = a.split(",")[0]
			aFile = a.split(",")[1]
			if aType == "vcf":
				print "loading vcf file " + aFile
				try:
					handle=pysam.TabixFile(filename=aFile,parser=pysam.asVCF())
				except:
					sys.exit("failed to load vcf file " + aFile)
				else:
					samples = samples + [a for a in handle.header][-1].split('\t')[9:]
			elif aType == "bfile":
				handle = pd.read_table(aFile + ".fam", header=None, sep=" ")
				handle.columns = ['fid','iid','fat','mot','sex','pheno']
				samples = samples + handle['iid'].tolist()
			else:
				sys.exit("failed to load file of unsupported type " + aType)
		samples = set(samples)
		nSamples = len(samples)

		print "writing introduction"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Introduction}"); f.write("\n")

		if nArrays > 1:
			geno_platforms_text = "distributed across {0:d} different genotype arrays".format(nArrays)
		else:
			geno_platforms_text = "genotyped on a single array"
		text = "This document contains details of our in-house quality control procedure and its application to the {0:s} dataset. We received genotypes for {1:,d} unique samples {2:s}. Quality control was performed on these data to detect samples and variants that did not fit our standards for inclusion in association testing. After harmonizing with modern reference data, the highest quality variants were used in a battery of tests to assess the quality of each sample. Duplicate pairs, samples exhibiting excessive sharing of identity by descent, samples whose genotypic sex did not match their clinical sex, and outliers detected among several sample-by-variant statistics have been flagged for removal from further analysis. Additionally, genotypic ancestry was inferred with respect to a modern reference panel, allowing for variant filtering and association analyses to be performed within population as needed.".format(args.id, nSamples, geno_platforms_text)
		if nArrays > 1:
			text = text + "With the exception of inferring each samples ancestry, QC was performed on each array separately as much as possible, allowing for flexibility in the way the data can be used in downstream analyses."
		f.write("\n"); f.write(text.replace("_","\_").encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--id', help='a project ID', required=True)
	requiredArgs.add_argument('--authors', help='a comma separated list of authors', required=True)
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--array-data', nargs='+', help='a list of array data (plink binary file name or vcf file) each a comma delimited datatype (bfile or vcf) and data file pair', required=True)
	args = parser.parse_args()
	main(args)
