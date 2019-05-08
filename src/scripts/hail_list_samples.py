import hail as hl
import argparse
import pandas as pd

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log)
	else:
		hl.init()

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)

	print("extract model specific columns from phenotype file")
	cols_keep = [args.iid_col,args.pheno_col]
	if args.covars != "":
		cols_keep = cols_keep + [x.replace('[','').replace(']','') for x in args.covars.split("+")]
	with hl.hadoop_open(args.pheno_in) as f:
		pheno_df = pd.read_table(f, sep="\t", usecols=cols_keep, dtype=object)

	pheno_df.dropna(inplace=True)

	print("write preliminary phenotypes to google cloud file")
	with hl.hadoop_open(args.out_pheno_prelim, 'w') as f:
		pheno_df.to_csv(f, header=True, index=False, sep="\t", na_rep="NA")

	if len(pheno_df[args.pheno_col].unique()) == 2:
		print("add case/control status annotation")
		tbl = hl.import_table(args.out_pheno_prelim, no_header=False, types={args.pheno_col: hl.tint})
		tbl = tbl.key_by(args.iid_col)
		tbl = tbl.annotate(is_case = hl.cond(tbl[args.pheno_col] == 1, True, False))
	else:
		print("set case/control status annotation to false")
		tbl = hl.import_table(args.out_pheno_prelim, no_header=False)
		tbl = tbl.key_by(args.iid_col)
		tbl = tbl.annotate(is_case = False)
	mt = mt.annotate_cols(pheno = tbl[mt.s])

	print("extract samples with non-missing phenotype annotations")
	mt = mt.filter_cols(hl.is_defined(mt.pheno[args.pheno_col]), keep=True)

	print("extract variants from previously filtered and pruned bim file")
	tbl = hl.import_table(args.bim_in, no_header=True, types={'f0': hl.tint, 'f1': hl.tstr, 'f2': hl.tfloat, 'f3': hl.tint, 'f4': hl.tstr, 'f5': hl.tstr})
	tbl = tbl.rename({'f0': 'chr', 'f1': 'rsid', 'f2': 'cm', 'f3': 'pos', 'f4': 'alt', 'f5': 'ref'})
	tbl = tbl.annotate(locus = hl.parse_locus(hl.str(tbl.chr) + ":" + hl.str(tbl.pos)), alleles =  [tbl.ref, tbl.alt])
	tbl = tbl.key_by('locus', 'alleles')
	mt = mt.annotate_rows(in_bim = hl.cond(hl.is_defined(tbl[mt.locus, mt.alleles]), True, False))
	mt = mt.filter_rows(mt.in_bim, keep=True)

	if args.test != "lmm":
		print("prune samples to maximal independent set, favoring cases over controls")
		pairs = hl.pc_relate(mt.GT, min_individual_maf = 0.01, k = 10, statistics = 'kin', min_kinship = 0.0884, block_size = 1024)
		samples = mt.cols()
		pairs_with_case = pairs.key_by(
			i=hl.struct(id=pairs.i, is_case=samples[pairs.i].is_case),
			j=hl.struct(id=pairs.j, is_case=samples[pairs.j].is_case))
		def tie_breaker(l, r):
			return hl.cond(l.is_case & ~r.is_case, -1, hl.cond(~l.is_case & r.is_case, 1, 0))
		related_samples_to_remove = hl.maximal_independent_set(pairs_with_case.i, pairs_with_case.j, False, tie_breaker)
		mt = mt.filter_cols(hl.is_defined(related_samples_to_remove.key_by(s = related_samples_to_remove.node.id.s)[mt.col_key]), keep=False)

	print("write sample list to file")
	tbl = mt.cols()
	tbl = tbl.select()
	tbl.export(args.out_samples, header=False, types_file=None)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
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
