import hail as hl
import argparse
import pandas as pd
import os

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log, idempotent=True)
		os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-memory ' + str(args.driver_memory) + 'g --executor-memory ' + str(args.executor_memory) + 'g pyspark-shell'
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

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--min-partitions', type=int, default=100, help='number of min partitions')
	parser.add_argument('--driver-memory', type=int, default=1, help='spark driver memory in GB (an integer)')
	parser.add_argument('--executor-memory', type=int, default=1, help='spark executor memory in GB (an integer)')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--annotations', help='an annotation file', required=True)
	requiredArgs.add_argument('--out', help='an output hail table name', required=True)
	args = parser.parse_args()
	main(args)
