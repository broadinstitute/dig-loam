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
			in_autosome = hl.cond(mt.locus.in_autosome(), 0, 1),
			an = hl.cond(mt.variant_qc_raw.AN > 1, 0, 1),
			is_snp = hl.cond(hl.is_snp(mt.alleles[0], mt.alleles[1]), 0, 1),
			is_mnp = hl.cond(~ hl.is_mnp(mt.alleles[0], mt.alleles[1]), 0, 1),
			is_indel = hl.cond(~ hl.is_indel(mt.alleles[0], mt.alleles[1]), 0, 1),
			is_complex = hl.cond(~ hl.is_complex(mt.alleles[0], mt.alleles[1]), 0, 1),
			in_hild_region = hl.cond(~ hl.is_defined(tbl_hild[mt.row_key]), 0, 1)
		)
	)

	print("filter call_rate")
	if args.filter_call_rate is not None:
		mt = mt.annotate_rows(
			qc_filters = mt.qc_filters.annotate(
				call_rate = hl.cond(eval(hl.eval(args.filter_call_rate.replace("call_rate","mt.variant_qc_raw.call_rate"))), 0, 1)
			)
		)
	else:
		mt = mt.annotate_rows(
			qc_filters = mt.qc_filters.annotate(
				call_rate = 0
			)
		)

	print("filter ac")
	if args.filter_ac is not None:
		mt = mt.annotate_rows(
			qc_filters = mt.qc_filters.annotate(
				ac = hl.cond(eval(hl.eval(args.filter_ac.replace("ac","mt.variant_qc_raw.AC[1]"))), 0, 1)
			)
		)
	else:
		mt = mt.annotate_rows(
			qc_filters = mt.qc_filters.annotate(
				ac = 0
			)
		)

	print("filter af")
	if args.filter_af is not None:
		mt = mt.annotate_rows(
			qc_filters = mt.qc_filters.annotate(
				af = hl.cond(eval(hl.eval(args.filter_af.replace("af","mt.variant_qc_raw.AF[1]"))), 0, 1)
			)
		)
	else:
		mt = mt.annotate_rows(
			qc_filters = mt.qc_filters.annotate(
				af = 0
			)
		)

	print("filter het")
	if args.filter_het is not None:
		mt = mt.annotate_rows(
			qc_filters = mt.qc_filters.annotate(
				het = hl.cond(eval(hl.eval(args.filter_het.replace("het","mt.variant_qc_raw.het"))), 0, 1)
			)
		)
	else:
		mt = mt.annotate_rows(
			qc_filters = mt.qc_filters.annotate(
				het = 0
			)
		)

	print("filter avg_het_ab")
	if args.filter_avg_het_ab is not None:
		mt = mt.annotate_rows(
			qc_filters = mt.qc_filters.annotate(
				avg_het_ab = hl.cond(eval(hl.eval(args.filter_avg_het_ab.replace("avg_het_ab","mt.variant_qc_raw.avg_het_ab"))), 0, 1)
			)
		)
	else:
		mt = mt.annotate_rows(
			qc_filters = mt.qc_filters.annotate(
				avg_het_ab = 0
			)
		)

	mt = mt.annotate_rows(
		qc_exclude = hl.cond(
			(mt.qc_filters.in_autosome == 1) |
				(mt.qc_filters.an == 1) |
				(mt.qc_filters.is_snp == 1) |
				(mt.qc_filters.is_mnp == 1) |
				(mt.qc_filters.is_indel == 1) |
				(mt.qc_filters.is_complex == 1) |
				(mt.qc_filters.af == 1) |
				(mt.qc_filters.call_rate == 1) |
				(mt.qc_filters.in_hild_region == 1) |
				(mt.qc_filters.call_rate == 1) |
				(mt.qc_filters.ac == 1) |
				(mt.qc_filters.af == 1) |
				(mt.qc_filters.het == 1) |
				(mt.qc_filters.avg_het_ab == 1), 
			1, 
			0
		)
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
	parser.add_argument('--filter-call-rate', help='include variants satisfying this expression')
	parser.add_argument('--filter-ac', help='include variants satisfying this expression')
	parser.add_argument('--filter-af', help='include variants satisfying this expression')
	parser.add_argument('--filter-het', help='include variants satisfying this expression')
	parser.add_argument('--filter-avg-het-ab', help='include variants satisfying this expression')
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
