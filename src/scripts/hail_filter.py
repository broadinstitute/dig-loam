import hail as hl
import argparse
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

	if args.pheno_in is not None and args.id_col is not None and args.strat_col is not None and args.strat_codes is not None:
		print("extract samples with appropriate cohort codes")
		tbl = hl.import_table(args.pheno_in, no_header=False).key_by(args.id_col)
		tbl = tbl.filter(hl.literal(set(args.strat_codes.split(","))).contains(tbl[args.strat_col]))
		mt = mt.filter_cols(hl.is_defined(tbl[mt.s]), keep=True)
	
	if args.ancestry_in is not None and args.ancestry_keep is not None:
		print("extract samples with appropriate ancestry")
		tbl = hl.import_table(args.ancestry_in, no_header=False).key_by('IID')
		tbl = tbl.filter(hl.literal(set(args.ancestry_keep.split(","))).contains(tbl['FINAL']))
		mt = mt.filter_cols(hl.is_defined(tbl[mt.s]), keep=True)

	if args.samples_remove is not None:
		print("remove samples (ie samples that failed previous qc steps)")
		for sample_file in args.samples_remove.split(","):
			try:
				tbl = hl.import_table(sample_file, no_header=True).key_by('f0')
			except:
				print("skipping empty file " + sample_file)
			else:
				mt = mt.filter_cols(hl.is_defined(tbl[mt.s]), keep=False)

	if args.samples_extract is not None:
		print("extract samples")
		try:
			tbl = hl.import_table(args.samples_extract, no_header=True).key_by('f0')
		except:
			print("skipping empty file " + args.samples_extract)
		else:
			mt = mt.filter_cols(hl.is_defined(tbl[mt.s]), keep=True)

	print("key rows by locus, alleles and rsid")
	mt = mt.key_rows_by('locus','alleles','rsid')

	if args.variants_remove is not None:
		print("remove variants (ie variants that failed previous qc steps)")
		for variant_file in args.variants_remove.split(","):
			try:
				tbl = hl.import_table(variant_file, no_header=True, types={'f0': 'locus<' + args.reference_genome + '>', 'f1': 'array<str>', 'f2': 'str'}).key_by('f0', 'f1', 'f2')
			except:
				print("skipping empty file " + variant_file)
			else:
				mt = mt.filter_rows(hl.is_defined(tbl[mt.row_key]), keep=False)
    
	if args.variants_extract is not None:
		print("extract variants")
		try:
			tbl = hl.import_table(args.variants_extract, no_header=True, types={'f0': 'locus<' + args.reference_genome + '>', 'f1': 'array<str>', 'f2': 'str'}).key_by('f0', 'f1', 'f2')
		except:
			print("skipping empty file " + args.variants_extract)
		else:
			mt = mt.filter_rows(hl.is_defined(tbl[mt.row_key]), keep=True)

	print("key rows by locus, alleles and rsid")
	mt = mt.key_rows_by('locus','alleles')

	print("begin sample filtering")
	print("filter to only non-vcf-filtered, well-called, non-monomorphic, autosomal variants for sample qc")
	mt_sample_qc = mt.filter_rows((hl.is_missing(hl.len(mt.filters)) | (hl.len(mt.filters) == 0)) & mt.locus.in_autosome() & (mt.variant_qc_raw.AN > 1) & (mt.variant_qc_raw.AF > 0) & (mt.variant_qc_raw.AF < 1), keep=True)

	print("calculate sample qc stats")
	mt_sample_qc = hl.sample_qc(mt_sample_qc, name='sample_qc')

	print("calculate variant qc stats")
	mt_sample_qc = hl.variant_qc(mt_sample_qc, name='variant_qc')

	print("add additional sample qc stats")
	mt_sample_qc = hail_utils.add_sample_qc_stats(mt = mt_sample_qc, sample_qc = 'sample_qc', variant_qc = 'variant_qc')

	print("initialize sample qc filter table")
	tbl = mt_sample_qc.cols()
	tbl = tbl.annotate(
		ls_filters = hl.struct(
			exclude = 0
		)
	)

	if args.sample_filters:
		with hl.hadoop_open(args.sample_filters, 'r') as f:
			sfilters = f.read().splitlines()
		tbl = hail_utils.ht_add_filters(ht = tbl, filters = [f.split("\t") for f in sfilters], struct_name = "ls_filters")

	print("write sample qc metrics and exclusions to file")
	tbl.flatten().export(args.samples_stats_out, header=True)

	print("write failed sample ids to file")
	tbl.filter(tbl.ls_filters.exclude == 1, keep=True).select().export(args.samples_exclude_out, header=False)

	if args.samples_keep_out is not None:
		print("write clean sample ids to file")
		tbl.filter(tbl.ls_filters.exclude == 0, keep=True).select().export(args.samples_keep_out, header=False)

	print("begin variant filtering")
	mt = mt.annotate_cols(sample_qc_exclude = 0)
	mt = mt.annotate_cols(sample_qc_exclude = tbl[mt.s].ls_filters.exclude)
	mt = mt.filter_cols(mt.sample_qc_exclude == 0, keep=True)

	print("calculate variant qc stats")
	mt = hl.variant_qc(mt, name='variant_qc')

	print("calculate call_rate, AC, AN, AF, het_freq_hwe, p_value_hwe, het, avg_ab, and avg_het_ab accounting appropriately for sex chromosomes")
	mt = hail_utils.update_variant_qc(mt = mt, is_female = 'is_female', variant_qc = 'variant_qc')

	print("extract variant qc stats table")
	tbl = mt.rows()

	print("initialize variant filter table")
	tbl = tbl.annotate(
		ls_filters = hl.struct(
			filters = hl.if_else(hl.is_missing(tbl.filters), 0, hl.if_else(hl.len(tbl.filters) == 0, 0, 1)),
			AN = hl.if_else(tbl.variant_qc.AN > 1, 0, 1),
			is_monomorphic = hl.if_else((tbl.variant_qc.AF > 0) & (tbl.variant_qc.AF < 1), 0, 1)
		)
	)

	print("add exclude field and update for base filters")
	tbl = tbl.annotate(ls_filters = hl.struct(exclude = hl.if_else((tbl.ls_filters.filters == 1) | (tbl.ls_filters.AN == 1) | (tbl.ls_filters.is_monomorphic == 1), 1, 0)))

	if args.variant_filters:
		with hl.hadoop_open(args.variant_filters, 'r') as f:
			vfilters = f.read().splitlines()
		tbl = hail_utils.ht_add_filters(ht = tbl, filters = [f.split("\t") for f in vfilters], struct_name = "ls_filters")

	print("write variant qc metrics and exclusions to file")
	tbl = tbl.drop("info","variant_qc_raw")
	tbl.flatten().export(args.variants_stats_out, header=True)

	print("write failed variants to file")
	tbl.filter(tbl.ls_filters.exclude == 1, keep=True).select('rsid','uid').export(args.variants_exclude_out, header=False)

	if args.variants_keep_out is not None:
		print("write clean variants to file")
		tbl.filter(tbl.ls_filters.exclude == 0, keep=True).select('rsid','uid').export(args.variants_keep_out, header=False)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--sample-filters', help='filter id, column name, expression; exclude variants satisfying this expression')
	parser.add_argument('--variant-filters', help='filter id, column name, expression; exclude variants satisfying this expression')
	parser.add_argument('--pheno-in', help='a phenotype file name')
	parser.add_argument('--id-col', help='a sample id column name in phenotype file')
	parser.add_argument('--case-ctrl-col', help='a case/ctrl type column name in phenotype file (ie. coded as 1/0)')
	parser.add_argument('--ancestry-in', help='an inferred ancestry file')
	parser.add_argument('--ancestry-keep', help='a comma separated list of ancestry codes to keep')
	parser.add_argument('--strat-col', help='a column name for a categorical column in the phenotype file')
	parser.add_argument('--strat-codes', help='a comma separated list of strat column values to keep')
	parser.add_argument('--samples-remove', help='a comma separated list of files containing samples to remove before calculations')
	parser.add_argument('--samples-extract', help='a comma separated list of files containing samples to extract before calculations')
	parser.add_argument('--variants-remove', help='a comma separated list of files containing variants to remove before calculations')
	parser.add_argument('--variants-extract', help='a comma separated list of files containing variants to extract before calculations')
	parser.add_argument('--samples-keep-out', help='a base filename for samples to keep')
	parser.add_argument('--variants-keep-out', help='a base filename for variants to keep')
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--driver-memory', default="1g", help='spark driver memory')
	parser.add_argument('--executor-memory', default="1g", help='spark executor memory')
	parser.add_argument('--tmp-dir', help='a temporary path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail mt dataset name', required=True)
	requiredArgs.add_argument('--samples-stats-out', help='a base filename for sample qc', required=True)
	requiredArgs.add_argument('--samples-exclude-out', help='a base filename for failed samples', required=True)
	requiredArgs.add_argument('--variants-stats-out', help='a base filename for variant qc', required=True)
	requiredArgs.add_argument('--variants-exclude-out', help='a base filename for failed variants', required=True)
	args = parser.parse_args()
	main(args)
