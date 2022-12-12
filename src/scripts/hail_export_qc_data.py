import hail as hl
import argparse
import os

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log, tmp_dir = args.tmp_dir, idempotent=True)
		os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-memory ' + args.driver_memory + 'g --executor-memory ' + args.executor_memory + 'g pyspark-shell'
	else:
		hl.init(idempotent=True)

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)

	print("make hild region variants table")
	with hl.hadoop_open(args.regions_exclude, 'r') as f:
		hild = f.read().splitlines()
	tbl_hild = hl.filter_intervals(mt.rows().select(), [hl.parse_locus_interval(x, reference_genome=args.reference_genome) for x in hild], keep=True)

	print("add variant filter out table for QC")
	mt = mt.annotate_rows(
		ls_filters = hl.struct(
			vcf_filter = hl.if_else((hl.is_missing(hl.len(mt.filters)) | (hl.len(mt.filters) == 0)), 0, 1),
			in_autosome = hl.if_else(mt.locus.in_autosome(), 0, 1),
			AN = hl.if_else(mt.variant_qc_raw.AN > 1, 0, 1),
			is_monomorphic = hl.if_else((mt.variant_qc_raw.AF > 0) & (mt.variant_qc_raw.AF < 1), 0, 1),
			is_snp = hl.if_else(hl.is_snp(mt.alleles[0], mt.alleles[1]), 0, 1),
			is_mnp = hl.if_else(~ hl.is_mnp(mt.alleles[0], mt.alleles[1]), 0, 1),
			is_indel = hl.if_else(~ hl.is_indel(mt.alleles[0], mt.alleles[1]), 0, 1),
			is_complex = hl.if_else(~ hl.is_complex(mt.alleles[0], mt.alleles[1]), 0, 1),
			in_hild_region = hl.if_else(~ hl.is_defined(tbl_hild[mt.row_key]), 0, 1)
		)
	)

	print("add qc_exclude annotation for default qc filters")
	mt = mt.annotate_rows(
		ls_filters = mt.ls_filters.annotate(
			exclude = hl.if_else(
				((mt.ls_filters.vcf_filter == 1) |
				(mt.ls_filters.in_autosome == 1) |
				(mt.ls_filters.AN == 1) |
				(mt.ls_filters.is_monomorphic == 1) |
				(mt.ls_filters.is_snp == 1) |
				(mt.ls_filters.is_mnp == 1) |
				(mt.ls_filters.is_indel == 1) |
				(mt.ls_filters.is_complex == 1) |
				(mt.ls_filters.in_hild_region == 1)),
				1,
				0
			)
		)
	)

	if args.variant_filters:
		with hl.hadoop_open(args.variant_filters, 'r') as f:
			vfilters = f.read().splitlines()
			for vf in vfilters:
				f = vf.split("\t")
				fields = f[1].split(",")
				absent = False
				for field in fields:
					if field not in mt.rows().row_value.flatten():
						absent = True
					f[2] = f[2].replace(field,"mt." + field)
				if not absent:
					print("filter variants based on configuration filter " + f[0] + " for field/s " + f[1])
					mt = mt.annotate_rows(
						ls_filters = mt.ls_filters.annotate(
							**{f[0]: hl.if_else(eval(hl.eval(f[2])), 0, 1, missing_false = True)}
						)
					)
				else:
					print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... 1 or more fields do not exist")
					mt = mt.annotate_rows(
						ls_filters = mt.ls_filters.annotate(
							**{f[0]: 0}
						)
					)
				print("update exclusion column based on " + f[0])
				mt = mt.annotate_rows(
					ls_filters = mt.ls_filters.annotate(
						exclude = hl.if_else(
							mt.ls_filters[f[0]] == 1,
							1,
							mt.ls_filters.exclude
						)
					)
				)

	rows_filtered = mt.rows().select('ls_filters')
	rows_filtered = rows_filtered.filter(rows_filtered.ls_filters.exclude == 0, keep=True)
	n = rows_filtered.count()
	if args.sample_n is not None:
		if n > args.sample_n:
			prop = args.sample_n / n
			print("downsampling variants by " + str(100*(1-prop)) + "%")
			rows_filtered = rows_filtered.sample(p = prop, seed = args.sample_seed)
			mt = mt.annotate_rows(
				ls_filters = mt.ls_filters.annotate(
					downsample = hl.if_else(
						mt.ls_filters.exclude == 0,
						hl.if_else(
							hl.is_defined(rows_filtered[mt.row_key]),
							0,
							1
						),
						-1
					)
				)
			)
		else:
			print("skipping downsampling because the post-filter variant count " + str(n) + " <= " + str(args.sample_n))
			mt = mt.annotate_rows(
				ls_filters = mt.ls_filters.annotate(
					downsample = hl.if_else(
						mt.ls_filters.exclude == 0,
						0,
						-1
					)
				)
			)
	else:
		mt = mt.annotate_rows(
			ls_filters = mt.ls_filters.annotate(
				downsample = hl.if_else(
					mt.ls_filters.exclude == 0,
					0,
					-1
				)
			)
		)

	print("write variant table to file")
	mt.rows().drop("info").flatten().export(args.variants_out, types_file=None)

	print("filtering matrix table for qc")
	mt = mt.filter_rows((mt.ls_filters.exclude != 1) & (mt.ls_filters.downsample != 1), keep=True)

	print("write Plink files to disk")
	hl.export_plink(mt, args.plink_out, ind_id = mt.s, fam_id = mt.s, pheno = -9)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--variant-filters', help='an id, a column name, and an expression; include variants satisfying this expression')
	parser.add_argument('--sample-n', type=int, help='an integer indicating the number of desired variants in the final QC data set (will be ignored if remaining variant count is less than this number)')
	parser.add_argument('--sample-seed', type=int, default=1, help='an integer used as a seed to allow for reproducibility in sampling variants')
	parser.add_argument('--driver-memory', type=int, default=1, help='spark driver memory in GB (an integer)')
	parser.add_argument('--executor-memory', type=int, default=1, help='spark executor memory in GB (an integer)')
	parser.add_argument('--tmp-dir', help='a temporary path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--regions-exclude', help='a list of Tabix formatted regions to exclude from QC', required=True)
	requiredArgs.add_argument('--variants-out', help='an output filename for pruned variant list', required=True)
	requiredArgs.add_argument('--plink-out', help='a pruned and filtered Plink dataset name', required=True)
	args = parser.parse_args()
	main(args)
