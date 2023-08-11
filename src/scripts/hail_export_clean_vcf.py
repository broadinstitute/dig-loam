import hail as hl
import argparse
import os

def main(args=None):

	if not args.cloud:
		os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-memory ' + args.driver_memory + ' --executor-memory ' + args.executor_memory + ' pyspark-shell'
		hl.init(log = args.log, tmp_dir = args.tmp_dir, idempotent=True)
	else:
		hl.init(idempotent=True)

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)

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

	print("key rows by locus and alleles")
	mt = mt.key_rows_by('locus','alleles')

	print("write VCF file to disk")
	hl.export_vcf(mt, args.vcf_out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--samples-remove', help='a comma separated list of files containing samples to remove')
	parser.add_argument('--samples-extract', help='a comma separated list of files containing samples to extract')
	parser.add_argument('--variants-remove', help='a comma separated list of files containing variants to remove')
	parser.add_argument('--variants-extract', help='a comma separated list of files containing variants to extract')
	parser.add_argument('--driver-memory', default="1g", help='spark driver memory')
	parser.add_argument('--executor-memory', default="1g", help='spark executor memory')
	parser.add_argument('--tmp-dir', help='a temporary path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail mt dataset name', required=True)
	requiredArgs.add_argument('--vcf-out', help='a vcf filename', required=True)
	args = parser.parse_args()
	main(args)
