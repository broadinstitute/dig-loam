import hail as hl
import argparse
import pandas as pd
import os

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
		os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-memory ' + args.driver_memory + ' --executor-memory ' + args.executor_memory + ' pyspark-shell'
		os.environ["SPARK_LOCAL_DIRS"] = args.tmp_dir
		hl.init(log = args.log, tmp_dir = args.tmp_dir, local_tmpdir = args.tmp_dir, idempotent=True)
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
	if args.binary:
		print("add case/ctrl variant qc")
		mt = hail_utils.add_case_ctrl_stats_results(mt, is_female = "is_female", variant_qc = "variant_qc", is_case = args.pheno_analyzed)
		print("add differential missingness")
		mt = hail_utils.add_diff_miss(mt, is_female = "is_female", variant_qc = "variant_qc", is_case = args.pheno_analyzed, diff_miss_min_expected_cell_count = 5)

	tbl = mt.rows()
	tbl = tbl.annotate(
		chr = tbl.locus.contig,
		pos = tbl.locus.position,
		ref = tbl.alleles[0],
		alt = tbl.alleles[1]
	)
	tbl = tbl.select(*['chr', 'pos', 'ref', 'alt', 'rsid','uid','variant_qc'])
	tbl = tbl.flatten()
	tbl = tbl.rename(dict(zip(list(tbl.row),[x.replace('variant_qc.','') if x.startswith('variant_qc') else x for x in list(tbl.row)])))
	tbl = tbl.key_by()
	tbl = tbl.annotate(chr_idx = hl.if_else(tbl.locus.in_autosome(), hl.int(tbl.chr.replace("chr","")), hl.if_else(tbl.locus.contig.replace("chr","") == "X", 23, hl.if_else(tbl.locus.contig.replace("chr","") == "Y", 24, hl.if_else(tbl.locus.contig.replace("chr","") == "MT", 25, hl.if_else(tbl.locus.contig.replace("chr","") == "M", 25, 26))))))
	tbl = tbl.order_by(hl.int(tbl.chr_idx), hl.int(tbl.pos), tbl.ref, tbl.alt)
	tbl = tbl.drop(tbl.chr_idx)
	tbl = tbl.rename({'chr': '#chr'})

	drop_cols = ['locus', 'alleles', 'homozygote_count']
	if args.binary:
		drop_cols = drop_cols + ['diff_miss_row1_sum', 'diff_miss_row2_sum', 'diff_miss_col1_sum', 'diff_miss_col2_sum', 'diff_miss_tbl_sum', 'diff_miss_expected_c1', 'diff_miss_expected_c2', 'diff_miss_expected_c3', 'diff_miss_expected_c4']

	tbl = tbl.drop(*drop_cols)

	tbl.export(args.out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--binary', action='store_true', default=False, help='flag indicates whether or not the phenotype analyzed is binary')
	parser.add_argument('--driver-memory', default="1g", help='spark driver memory')
	parser.add_argument('--executor-memory', default="1g", help='spark executor memory')
	parser.add_argument('--tmp-dir', help='a temporary path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a matrix table', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--pheno-analyzed', help='a column name for the phenotype used in analysis', required=True)
	requiredArgs.add_argument('--out', help='an output file basename', required=True)
	args = parser.parse_args()
	main(args)
