import hail as hl
import argparse
import pandas as pd
import csv
from pathlib import Path
import time
import os

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
		hl.init(log = args.log, idempotent=True)
		os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-memory ' + args.driver_memory + 'g --executor-memory ' + args.executor_memory + 'g pyspark-shell'
	else:
		hl.init(idempotent=True)

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)

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

	print("key matrix table rows by locus, alleles and rsid")
	mt = mt.key_rows_by('locus','alleles','rsid')

	print("read filter table and key by locus, alleles and rsid")
	ht = hl.read_table(args.filter_table_in)
	ht = ht.key_by('locus','alleles','rsid')

	print("annotate matrix table with filter table and key by locus and alleles")
	mt = mt.annotate_rows(ls_filters = ht[mt.row_key])
	mt = mt.key_rows_by('locus','alleles')

	print("filter out rows with ls_global_exclude == 1")
	n_all = mt.rows().count()
	mt = mt.filter_rows(mt.ls_filters.ls_global_exclude == 0, keep = True)
	n_post_filter = mt.rows().count()
	print(str(n_all - n_post_filter) + " variants removed via standard filters")
	print(str(n_post_filter) + " variants remaining for analysis")
	
	knockout_cohorts = [x.replace("ls_knockouts_","") for x in list(ht.row_value) if x.startswith("ls_knockouts_")]
	if len(knockout_cohorts) > 0:
		print("knockout filters found for cohorts: " + ", ".join(knockout_cohorts))
		for cohort in knockout_cohorts:
			n_samples = mt.aggregate_cols(hl.agg.count_where(mt.COHORT == cohort))
			n_variants = mt.aggregate_rows(hl.agg.count_where(mt['ls_filters']['ls_knockouts_' + cohort]['exclude'] == 1))
			mt = mt.annotate_entries(
				GT = hl.if_else((mt.COHORT == cohort) & (mt['ls_filters']['ls_knockouts_' + cohort]['exclude'] == 1), hl.missing(hl.tcall), mt.GT),
				AB = hl.if_else((mt.COHORT == cohort) & (mt['ls_filters']['ls_knockouts_' + cohort]['exclude'] == 1), hl.missing(hl.tfloat64), mt.AB),
				AB50 = hl.if_else((mt.COHORT == cohort) & (mt['ls_filters']['ls_knockouts_' + cohort]['exclude'] == 1), hl.missing(hl.tfloat64), mt.AB50),
				GQ = hl.if_else((mt.COHORT == cohort) & (mt['ls_filters']['ls_knockouts_' + cohort]['exclude'] == 1), hl.missing(hl.tfloat64), mt.GQ),
				GTT = hl.if_else((mt.COHORT == cohort) & (mt['ls_filters']['ls_knockouts_' + cohort]['exclude'] == 1), hl.missing(hl.tcall), mt.GTT),
				NALTT = hl.if_else((mt.COHORT == cohort) & (mt['ls_filters']['ls_knockouts_' + cohort]['exclude'] == 1), hl.missing(hl.tint32), mt.NALTT),
				DS = hl.if_else((mt.COHORT == cohort) & (mt['ls_filters']['ls_knockouts_' + cohort]['exclude'] == 1), hl.missing(hl.tint32), mt.DS)
			)
			print("performed genotype knockouts for " + str(n_variants) + " variants in " + str(n_samples) + " " + cohort + " cohort samples")
	else:
		print("no knockout filters found")
		
	# write vcf
	print("write VCF file to disk")
	hl.export_vcf(mt, args.vcf_out)
    
	if args.cloud:
		hl.copy_log(args.log)

	global_elapsed_time = time.time() - global_start_time
	print(time.strftime("total time elapsed - %H:%M:%S", time.gmtime(global_elapsed_time)))

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--driver-memory', type=int, default=1, help='spark driver memory in GB (an integer)')
	parser.add_argument('--executor-memory', type=int, default=1, help='spark executor memory in GB (an integer)')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a matrix table', required=True)
	requiredArgs.add_argument('--cohorts-map-in', help='a cohorts map file', required=True)
	requiredArgs.add_argument('--filter-table-in', help='a hail variant filter table', required=True)
	requiredArgs.add_argument('--vcf-out', help='a vcf file name', required=True)
	args = parser.parse_args()
	main(args)
