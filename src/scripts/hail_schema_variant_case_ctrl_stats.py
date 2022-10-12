import hail as hl
import argparse
import pandas as pd
import csv
from pathlib import Path
import time

def main(args=None):

	global_start_time = time.time()

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
		hl.init(log = args.log, tmp_dir = args.tmp_dir, idempotent=True)
	else:
		hl.init(idempotent=True)

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)
	mt = mt.annotate_rows(rsid = hl.if_else(hl.is_missing(mt.rsid), mt.locus.contig + ":" + hl.str(mt.locus.position) + ":" + mt.alleles[0] + ":" + mt.alleles[1], mt.rsid))

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
	mt = mt.filter_cols(hl.is_missing(mt.pheno[args.pheno_col]), keep=False)

	print("add cohort annotation")
	tbl = hl.import_table(
		args.cohorts_map_in,
		delimiter="\t",
		no_header=True,
		types={'f0': hl.tstr, 'f1': hl.tstr}
	)
	tbl = tbl.rename({'f0': 'IID', 'f1': 'COHORT'})
	tbl = tbl.key_by('IID')
	mt = mt.annotate_cols(COHORT = tbl[mt.s].COHORT)

	if args.cohort:
		print("filter samples to cohort " + args.cohort)
		mt = mt.filter_cols(mt.COHORT == args.cohort)
	else:
		print("filter samples to cohorts")
		mt = mt.filter_cols(hl.is_defined(mt.COHORT))
		
	start_time = time.time()
	mt = hail_utils.add_diff_miss(mt, is_female = "is_female", variant_qc = "variant_qc", is_case = args.pheno_col, diff_miss_min_expected_cell_count = args.diff_miss_min_expected_cell_count)
	elapsed_time = time.time() - start_time
	print(time.strftime("calculate differential missingness on " + str(mt.cols().count()) + " samples and " + str(mt.rows().count()) + " variants - %H:%M:%S", time.gmtime(elapsed_time)))

	print("write variant stats to hail table")
	tbl = mt.rows().drop("info","variant_qc_raw")
	tbl.write(args.variants_stats_ht_out, overwrite = True)

	print("write variant stats to file")
	tbl.flatten().export(args.variants_stats_out, header=True)
    
	if args.cloud:
		hl.copy_log(args.log)

	global_elapsed_time = time.time() - global_start_time
	print(time.strftime("total time elapsed - %H:%M:%S", time.gmtime(global_elapsed_time)))

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--cohort', help='a cohort id')
	parser.add_argument('--diff-miss-min-expected-cell-count', type=int, default = 5, help='the minimum expected cell count value needed to trigger a chi square test over a fisher exact test')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--tmp-dir', help='a temporary path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a matrix table', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--pheno-col', help='a column name for the phenotype', required=True)
	requiredArgs.add_argument('--cohorts-map-in', help='a cohorts map file', required=True)
	requiredArgs.add_argument('--variants-stats-out', help='a base filename for variant qc stats', required=True)
	requiredArgs.add_argument('--variants-stats-ht-out', help='a hail table name for variant qc stats', required=True)
	args = parser.parse_args()
	main(args)
