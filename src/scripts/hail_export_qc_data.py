import hail as hl
import argparse

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log)
	else:
		hl.init()

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)

	print("make hild region variants table")
	with hl.hadoop_open(args.regions_exclude, 'r') as f:
		hild = f.read().splitlines()
	tbl_hild = hl.filter_intervals(mt.rows().select(), [hl.parse_locus_interval(x) for x in hild], keep=True)

	print("add variant filter out table for QC")
	mt = mt.annotate_rows(
		qc_filters = hl.struct(
			vcf_filter = hl.cond(hl.len(mt.filters) == 0, 0, 1),
			in_autosome = hl.cond(mt.locus.in_autosome(), 0, 1),
			AN = hl.cond(mt.variant_qc_raw.AN > 1, 0, 1),
			is_monomorphic = hl.cond((mt.variant_qc_raw.AF > 0) & (mt.variant_qc_raw.AF < 1), 0, 1),
			is_snp = hl.cond(hl.is_snp(mt.alleles[0], mt.alleles[1]), 0, 1),
			is_mnp = hl.cond(~ hl.is_mnp(mt.alleles[0], mt.alleles[1]), 0, 1),
			is_indel = hl.cond(~ hl.is_indel(mt.alleles[0], mt.alleles[1]), 0, 1),
			is_complex = hl.cond(~ hl.is_complex(mt.alleles[0], mt.alleles[1]), 0, 1),
			in_hild_region = hl.cond(~ hl.is_defined(tbl_hild[mt.row_key]), 0, 1)
		)
	)

	for f in args.vfilter:
		if f is not None:
			print("filter variants based on " + f[0])
			mt = mt.annotate_rows(
				qc_filters = mt.qc_filters.annotate(
					**{f[0]: hl.cond(eval(hl.eval(f[1].replace(f[0],"mt.variant_qc_raw." + f[0]))), 0, 1)}
				)
			)
		else:
			mt = mt.annotate_rows(
				qc_filters = mt.qc_filters.annotate(
					**{f[0]: 0}
				)
			)
		print("update exclusion column based on " + f[0])
		mt = mt.annotate_rows(
			qc_exclude = hl.cond(mt.qc_filters[f[0]] == 1, 1, 0)
		)

	rows_filtered = mt.rows().select('qc_exclude')
	rows_filtered = rows_filtered.filter(rows_filtered.qc_exclude == 0, keep=True)
	n = rows_filtered.count()
	if args.sample_n is not None:
		if n > args.sample_n:
			prop = args.sample_n / n
			print("downsampling variants by " + str(100*(1-prop)) + "%")
			rows_filtered = rows_filtered.sample(p = prop, seed = args.sample_seed)
			mt = mt.annotate_rows(downsample_exclude = hl.cond(mt.qc_exclude == 0, hl.cond(hl.is_defined(rows_filtered[mt.row_key]), 0, 1), -1))
		else:
			print("skipping downsampling because the post-filter variant count " + str(n) + " <= " + str(args.sample_n))
			mt = mt.annotate_rows(downsample_exclude = hl.cond(mt.qc_exclude == 0, 0, -1))
	else:
		mt = mt.annotate_rows(downsample_exclude = hl.cond(mt.qc_exclude == 0, 0, -1))

	print("write variant table to file")
	mt.rows().flatten().export(args.variants_out, types_file=None)

	print("filtering matrix table for qc")
	mt = mt.filter_rows((mt.qc_exclude != 1) & (mt.downsample_exclude != 1), keep=True)

	print("write Plink files to disk")
	hl.export_plink(mt, args.plink_out, ind_id = mt.s, fam_id = mt.s)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--vfilter', nargs=2, action='append', help='column name followed by expression; include samples satisfying this expression')
	parser.add_argument('--sample-n', type=int, help='an integer indicating the number of desired variants in the final QC data set (will be ignored if remaining variant count is less than this number)')
	parser.add_argument('--sample-seed', type=int, default=1, help='an integer used as a seed to allow for reproducibility in sampling variants')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--regions-exclude', help='a list of Tabix formatted regions to exclude from QC', required=True)
	requiredArgs.add_argument('--variants-out', help='an output filename for pruned variant list', required=True)
	requiredArgs.add_argument('--plink-out', help='a pruned and filtered Plink dataset name', required=True)
	args = parser.parse_args()
	main(args)
