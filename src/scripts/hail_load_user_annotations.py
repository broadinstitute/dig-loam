import hail as hl
import argparse
import pandas as pd
import os
import tempfile

def main(args=None):

	print("making temporary directory for storing checkpoints")
	if args.tmp_dir and not args.cloud:
		tmpdir = tempfile.TemporaryDirectory(dir = args.tmp_dir)
	else:
		tmpdir = tempfile.TemporaryDirectory(dir = "./")

	if not args.cloud:
		os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-memory ' + args.driver_memory + ' --executor-memory ' + args.executor_memory + ' pyspark-shell'
		os.environ["SPARK_LOCAL_DIRS"] = tmpdir.name
		hl.init(log = args.log, tmp_dir = tmpdir.name, local_tmpdir = tmpdir.name, idempotent=True)
	else:
		hl.init(idempotent=True)

	print("import user supplied annotations")
	ht = hl.import_table(args.annotations, force_bgz = True, no_header=False, delimiter='\t', min_partitions=args.min_partitions, types = {'chr': hl.tstr, 'pos': hl.tstr, 'ref': hl.tstr, 'alt': hl.tstr})
	ht = ht.key_by('chr','pos','ref','alt')
	ht.describe()

	print("writing hail table to disk")
	ht.write(args.out, overwrite = True)

	if args.cloud:
		hl.copy_log(args.log)

	tmpdir.cleanup()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--min-partitions', type=int, default=100, help='number of min partitions')
	parser.add_argument('--driver-memory', default="1g", help='spark driver memory')
	parser.add_argument('--executor-memory', default="1g", help='spark executor memory')
	parser.add_argument('--tmp-dir', help='a temporary path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--annotations', help='an annotation file', required=True)
	requiredArgs.add_argument('--out', help='an output hail table name', required=True)
	args = parser.parse_args()
	main(args)
