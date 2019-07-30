import hail as hl
import argparse
import hail_utils

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log)
	else:
		hl.init()

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
			tbl = hl.import_table(sample_file, no_header=True).key_by('f0')
			mt = mt.filter_cols(hl.is_defined(tbl[mt.s]), keep=False)

	if args.samples_extract is not None:
		print("extract samples")
		tbl = hl.import_table(args.samples_extract, no_header=True).key_by('f0')
		mt = mt.filter_cols(hl.is_defined(tbl[mt.s]), keep=True)

	if args.variants_remove is not None:
		print("remove variants (ie variants that failed previous qc steps)")
		for variant_file in args.variants_remove.split(","):
			tbl = hl.import_table(variant_file, no_header=False).key_by('locus', 'alleles')
			mt = mt.filter_rows(hl.is_defined(tbl[mt.row_key]), keep=False)
    
	if args.variants_extract is not None:
		print("extract variants")
		tbl = hl.import_table(args.variants_extract, no_header=False).key_by('locus', 'alleles')
		mt = mt.filter_rows(hl.is_defined(tbl[mt.row_key]), keep=True)

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
		sample_qc_filters = hl.struct(
			exclude = 0
		)
	)

	if args.sfilter is not None:
		for f in args.sfilter:
			if f is not None:
				print("filter samples based on " + f[0])
				tbl = tbl.annotate(
					sample_qc_filters = tbl.sample_qc_filters.annotate(
						**{f[0]: hl.cond(eval(hl.eval(f[1].replace(f[0],"tbl.sample_qc." + f[0]))), 0, 1)}
					)
				)
			else:
				tbl = tbl.annotate(
					sample_qc_filters = tbl.sample_qc_filters.annotate(
						**{f[0]: 0}
					)
				)
			print("update exclusion column based on " + f[0])
			tbl = tbl.annotate(
				sample_qc_filters = tbl.sample_qc_filters.annotate(
					exclude = hl.cond(tbl.sample_qc_filters[f[0]] == 1, 1, tbl.sample_qc_filters.exclude)
				)
			)

	print("write sample qc metrics and exclusions to file")
	tbl.flatten().export(args.samples_stats_out, header=True)

	print("write failed sample ids to file")
	tbl.filter(tbl.sample_qc_filters.exclude == 1, keep=True).select().export(args.samples_exclude_out, header=False)

	if args.samples_keep_out is not None:
		print("write clean sample ids to file")
		tbl.filter(tbl.sample_qc_filters.exclude == 0, keep=True).select().export(args.samples_keep_out, header=False)

	print("begin variant filtering")
	mt = mt.annotate_cols(sample_qc_exclude = 0)
	mt = mt.annotate_cols(sample_qc_exclude = tbl[mt.s].sample_qc_filters.exclude)
	mt = mt.filter_cols(mt.sample_qc_exclude == 0, keep=True)

	print("calculate variant qc stats")
	mt = hl.variant_qc(mt, name='variant_qc')

	print("calculate call_rate, AC, AN, AF, het_freq_hwe, p_value_hwe, het, avg_ab, and avg_het_ab accounting appropriately for sex chromosomes")
	mt = hail_utils.adjust_variant_qc_sex(mt = mt, is_female = 'is_female', variant_qc = 'variant_qc')

	print("extract variant qc stats table")
	tbl = mt.rows()
	tbl = tbl.drop(tbl.variant_qc_raw)

	print("initialize variant filter table")
	tbl = tbl.annotate(
		variant_qc_filters = hl.struct(
			filters = hl.cond(hl.len(tbl.filters) == 0, 0, 1),
			AN = hl.cond(tbl.variant_qc.AN > 1, 0, 1),
			is_monomorphic = hl.cond((tbl.variant_qc.AF > 0) & (tbl.variant_qc.AF < 1), 0, 1)
		)
	)

	print("add exclude field and update for base filters")
	tbl = tbl.annotate(variant_qc_filters = hl.struct(exclude = hl.cond((tbl.variant_qc_filters.filters == 1) | (tbl.variant_qc_filters.AN == 1) | (tbl.variant_qc_filters.is_monomorphic == 1), 1, 0)))

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

	print("write variant qc metrics and exclusions to file")
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
	parser.add_argument('--sfilter', nargs=2, action='append', help='column name followed by expression; include samples satisfying this expression')
	parser.add_argument('--vfilter', nargs=2, action='append', help='column name followed by expression; include samples satisfying this expression')
	parser.add_argument('--pheno-in', help='a phenotype file name')
	parser.add_argument('--id-col', help='a sample id column name in phenotype file')
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
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail mt dataset name', required=True)
	requiredArgs.add_argument('--samples-stats-out', help='a base filename for sample qc', required=True)
	requiredArgs.add_argument('--samples-exclude-out', help='a base filename for failed samples', required=True)
	requiredArgs.add_argument('--variants-stats-out', help='a base filename for variant qc', required=True)
	requiredArgs.add_argument('--variants-exclude-out', help='a base filename for failed variants', required=True)
	args = parser.parse_args()
	main(args)
