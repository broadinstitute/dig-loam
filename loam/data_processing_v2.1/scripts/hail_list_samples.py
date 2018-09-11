from hail import *
hc = HailContext()
import pandas as pd
import numpy as np
import argparse

def main(args=None):

	print "reading vds dataset"
	vds = hc.read(args.vds_in)

	print "extracting model specific columns from phenotype file"
	cols_keep = [args.iid_col,args.pheno_col]
	if args.covars != "":
		cols_keep = cols_keep + [x.replace('[','').replace(']','') for x in args.covars.split("+")]
	with hadoop_read(args.pheno_in) as f:
		pheno_df = pd.read_table(f, sep="\t", usecols=cols_keep, dtype=object)

	pheno_df.dropna(inplace=True)

	print "writing preliminary phenotypes to google cloud file"
	with hadoop_write(args.out_pheno_prelim) as f:
		pheno_df.to_csv(f, header=True, index=False, sep="\t", na_rep="NA")
	
	if len(pheno_df[args.pheno_col].unique()) == 2:
		print "adding case/control status annotation"
		annotate_cc = hc.import_table(args.out_pheno_prelim, no_header=False, types={args.pheno_col: TInt()}).key_by(args.iid_col)
		if annotate_cc.num_columns == 2:
			vds = vds.annotate_samples_table(annotate_cc, expr='sa.isCase = table == 1')
		else:
			vds = vds.annotate_samples_table(annotate_cc, expr='sa.isCase = table.' + args.pheno_col + ' == 1')
		vds = vds.annotate_samples_table(annotate_cc, root='sa.pheno')
	else:
		print "setting case/control status annotation to false"
		annotate = hc.import_table(args.out_pheno_prelim, no_header=False).key_by(args.iid_col)
		vds = vds.annotate_samples_table(annotate, root='sa.pheno')
		vds = vds.annotate_samples_expr('sa.isCase = 0 == 1')

	print "extracting samples with non-missing phenotype annotations"
	vds = vds.filter_samples_list(list(pheno_df[args.iid_col].astype(str)),keep=True)

	print "extracting variants from previously filtered and pruned bim file"
	bim = hc.import_table(args.bim_in, no_header=True, types={'f1': TVariant()}).key_by('f1')
	vds = vds.filter_variants_table(bim, keep=True)

	if args.test != "lmm":
		vds = vds.ibd_prune(0.1768, tiebreaking_expr="if (sa1.isCase) 1 else 0")

	print "write sample list to file"
	vds.export_samples(args.out_samples, 's')

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vds-in', help='a hail vds dataset name', required=True)
	requiredArgs.add_argument('--bim-in', help='a filtered and pruned bim file', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--pheno-col', help='a column name for the phenotype', required=True)
	requiredArgs.add_argument('--test', help='an association test name (firth, score, lm, lmm, lrt)', required=True)
	requiredArgs.add_argument('--covars', help="a '+' separated list of covariates", required=True)
	requiredArgs.add_argument('--out-pheno-prelim', help='a file name for the preliminary phenotypes', required=True)
	requiredArgs.add_argument('--out-samples', help='a file name for list of samples to include in association test', required=True)
	args = parser.parse_args()
	main(args)
