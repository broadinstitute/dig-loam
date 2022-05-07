import hail as hl
import argparse
import pandas as pd

def main(args=None):

	if args.hail_utils:
		import importlib.util
		with hl.hadoop_open(args.hail_utils, 'r') as f:
			script = f.read()
		with open("hail_utils.py", 'w') as f:
			f.write(script)
		spec = importlib.util.spec_from_file_location('hail_utils', 'hail_utils.py')
		hail_utils = importlib.util.module_from_spec(spec)   
		spec.loader.exec_module(hail_utils)
	else:
		import hail_utils

	if not args.cloud:
		hl.init(log = args.log, idempotent=True)
	else:
		hl.init(idempotent=True)

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)

	print("annotate samples with phenotype file")
	tbl = hl.import_table(
		args.pheno_in,
		no_header=False,
		missing="NA",
		impute=True,
		types={args.iid_col: hl.tstr}
	)
	tbl = tbl.key_by(args.iid_col)
	mt = mt.annotate_cols(pheno = tbl[mt.s])

	print("reduce to samples with non-missing phenotype")
	mt = mt.filter_cols(hl.is_missing(mt.pheno[args.pheno_analyzed]), keep=False)

	n_all = mt.rows().count()
	print(str(n_all) + " variants remaining for analysis")

	print("calculate global variant qc")
	mt = hl.variant_qc(mt, name="variant_qc")
	mt = hail_utils.update_variant_qc(mt, is_female = "is_female", variant_qc = "variant_qc")
	if args.test != 'hail.q.lm':
		print("add case/ctrl variant qc")
		mt = hail_utils.add_case_ctrl_stats_results(mt, is_female = "is_female", variant_qc = "variant_qc", is_case = args.pheno_analyzed)






	tbl = tbl.select(
				chr = tbl.locus.contig,
				pos = tbl.locus.position,
				id = tbl.rsid,
				ref = tbl.alleles[0],
				alt = tbl.alleles[1],

	mt_results = mt_results.key_by()
	mt_results = mt_results.annotate(chr_idx = hl.if_else(mt_results.locus.in_autosome(), hl.int(mt_results.chr), hl.if_else(mt_results.locus.contig == "X", 23, hl.if_else(mt_results.locus.contig == "Y", 24, hl.if_else(mt_results.locus.contig == "MT", 25, 26)))))
	mt_results = mt_results.drop(mt_results.locus, mt_results.alleles)
	mt_results = mt_results.order_by(hl.int(mt_results.chr_idx), hl.int(mt_results.pos), mt_results.ref, mt_results.alt)
	mt_results = mt_results.drop(mt_results.chr_idx)
	mt_results = mt_results.rename({'chr': '#chr'})
	mt_results.export(args.out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a matrix table', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--pheno-analyzed', help='a column name for the phenotype used in analysis', required=True)
	requiredArgs.add_argument('--out', help='an output file basename', required=True)
	args = parser.parse_args()
	main(args)
