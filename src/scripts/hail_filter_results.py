import hail as hl
import argparse

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

	print("import variant stats table")
	tbl = hl.import_table(args.variants_extract, no_header=True, types={'f0': 'locus<GRCh37>', 'f1': 'array<str>'}).key_by('f0', 'f1')

	print("initialize variant filter table with exclude field")
	tbl = tbl.annotate(variant_qc_filters = hl.struct(exclude = 0))

	if args.vfilter is not None:
		for f in args.vfilter:
			if f is not None:
				print("filter variants based on " + f[0])
				tbl = tbl.annotate(
					variant_qc_filters = tbl.variant_qc_filters.annotate(
						**{f[0]: hl.cond(eval(hl.eval(f[1].replace(f[0],"tbl.variant_qc." + f[0]))), 0, 1)}
					)
				)
			else:
				tbl = tbl.annotate(
					variant_qc_filters = tbl.variant_qc_filters.annotate(
						**{f[0]: 0}
					)
				)
			print("update exclusion column based on " + f[0])
			tbl = tbl.annotate(
				variant_qc_filters = tbl.variant_qc_filters.annotate(
					exclude = hl.cond(tbl.variant_qc_filters[f[0]] == 1, 1, tbl.variant_qc_filters.exclude)
				)
			)

	print(tbl.describe())
	return
	print("write variant qc metrics and exclusions to file")
	tbl = tbl.drop("info","variant_qc_raw")
	tbl.flatten().export(args.variants_stats_out, header=True)

	print("write failed variants to file")
	tbl.filter(tbl.variant_qc_filters.exclude == 1, keep=True).select().export(args.variants_exclude_out, header=False)

	if args.variants_keep_out is not None:
		print("write clean variants to file")
		tbl.filter(tbl.variant_qc_filters.exclude == 0, keep=True).select().export(args.variants_keep_out, header=False)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--variants-keep-out', help='a filename for variants that pass filters')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--stats-in', help='a variants stats file name', required=True)
	requiredArgs.add_argument('--vfilter', nargs=2, action='append', help='column name followed by expression; include variants satisfying this expression', required=True)
	requiredArgs.add_argument('--variants-stats-out', help='a base filename for variant qc', required=True)
	requiredArgs.add_argument('--variants-exclude-out', help='a base filename for failed variants', required=True)
	args = parser.parse_args()
	main(args)
