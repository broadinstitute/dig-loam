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
		hl.init(log = args.log, tmp_dir = args.tmp_dir, idempotent=True)
	else:
		hl.init(idempotent=True)

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)
	hl.summarize_variants(mt)

	print("add sample cluster annotations")
	tbl = hl.import_table(args.clusters_in, delimiter="\t", no_header=True)
	tbl = tbl.annotate(IID = tbl.f0)
	tbl = tbl.key_by('IID')
	mt = mt.annotate_cols(GROUP = tbl[mt.s].f1)

	print("remove outlier samples")
	mt = mt.filter_cols(mt.GROUP == "OUTLIERS", keep=False)

	print("filter to only non-vcf-filtered, well-called, non-monomorphic, autosomal variants for sample qc")
	mt = mt.filter_rows((hl.is_missing(hl.len(mt.filters)) | (hl.len(mt.filters) == 0)) & mt.locus.in_autosome() & (mt.variant_qc_raw.AN > 1) & (mt.variant_qc_raw.AF > 0) & (mt.variant_qc_raw.AF < 1), keep=True)

	print("calculate sample qc stats")
	mt = hl.sample_qc(mt, name='sample_qc')

	print("calculate variant qc stats")
	mt = hl.variant_qc(mt, name='variant_qc')

	print("add additional sample qc stats")
	mt = hail_utils.add_sample_qc_stats(mt = mt, sample_qc = 'sample_qc', variant_qc = 'variant_qc')

	print("write sample qc stats results to file")
	tbl = mt.cols()
	tbl = tbl.rename({'s': 'IID'})
	tbl = tbl.select(
		n_non_ref = tbl.sample_qc.n_non_ref, 
		n_het = tbl.sample_qc.n_het, 
		n_called = tbl.sample_qc.n_called, 
		call_rate = tbl.sample_qc.call_rate, 
		n_singleton = tbl.sample_qc.n_singleton, 
		r_ti_tv = tbl.sample_qc.r_ti_tv, 
		het = tbl.sample_qc.het, 
		het_low = tbl.sample_qc.het_low, 
		het_high = tbl.sample_qc.het_high, 
		n_hom_var = tbl.sample_qc.n_hom_var, 
		r_het_hom_var = tbl.sample_qc.r_het_hom_var,
		avg_ab = tbl.sample_qc.avg_ab,
		avg_ab50 = tbl.sample_qc.avg_ab50)
	tbl.flatten().export(args.qc_out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--driver-memory', default="1g", help='spark driver memory')
	parser.add_argument('--executor-memory', default="1g", help='spark executor memory')
	parser.add_argument('--tmp-dir', help='a temporary path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--clusters-in', help='a tab delimited file consisting of sample IDs and their cluster assignment (eg: Sample1    EUR)', required=True)
	requiredArgs.add_argument('--qc-out', help='an output filename for sample qc statistics', required=True)
	args = parser.parse_args()
	main(args)
