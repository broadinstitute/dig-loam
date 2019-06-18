import hail as hl
import argparse

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log)
	else:
		hl.init()

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)

	print("add imputed sex annotation")
	tbl = hl.import_table(args.sexcheck_in, no_header=False, types={'is_female': hl.tbool})
	tbl = tbl.key_by('IID')
	mt = mt.annotate_cols(is_female = tbl[mt.s].is_female)

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
	mt_sample_qc = mt.filter_rows((hl.len(mt.filters) == 0) & mt.locus.in_autosome() & (mt.variant_qc_raw.AN > 1) & (mt.variant_qc_raw.AF[1] > 0) & (mt.variant_qc_raw.AF[1] < 1), keep=True)

	print("calculate sample qc stats")
	mt_sample_qc = hl.sample_qc(mt_sample_qc, name='sample_qc')

	print("calculate variant qc stats")
	mt_sample_qc = hl.variant_qc(mt_sample_qc, name='variant_qc')

	print("annotate sample qc stats")
	mt_sample_qc = mt_sample_qc.annotate_cols(
		sample_qc = mt_sample_qc.sample_qc.annotate(
			n_het_low = hl.agg.count_where((mt_sample_qc.variant_qc.AF[1] < 0.03) & mt_sample_qc.GT.is_het()), 
			n_het_high = hl.agg.count_where((mt_sample_qc.variant_qc.AF[1] >= 0.03) & mt_sample_qc.GT.is_het()), 
			n_called_low = hl.agg.count_where((mt_sample_qc.variant_qc.AF[1] < 0.03) & ~hl.is_missing(mt_sample_qc.GT)), 
			n_called_high = hl.agg.count_where((mt_sample_qc.variant_qc.AF[1] >= 0.03) & ~hl.is_missing(mt_sample_qc.GT)),
			avg_ab = hl.cond('AD' in list(mt_sample_qc.entry), hl.agg.mean(mt_sample_qc.AB), hl.null(hl.tfloat64)),
			avg_ab50 = hl.cond('AD' in list(mt_sample_qc.entry), hl.agg.mean(mt_sample_qc.AB50), hl.null(hl.tfloat64))
		)
	)

	print("extract sample qc stats table")
	tbl = mt_sample_qc.cols()
	tbl = tbl.annotate(
		sample_qc = tbl.sample_qc.annotate(
			het = tbl.sample_qc.n_het / tbl.sample_qc.n_called,
			het_low = tbl.sample_qc.n_het_low / tbl.sample_qc.n_called_low,
			het_high = tbl.sample_qc.n_het_high / tbl.sample_qc.n_called_high
		)
	)

	print("initialize sample qc filter table")
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

	print("begin variant filtering")
	mt = mt.annotate_cols(sample_qc_exclude = 0)
	mt = mt.annotate_cols(sample_qc_exclude = tbl[mt.s].sample_qc_filters.exclude)
	mt = mt.filter_cols(mt.sample_qc_exclude == 0, keep=True)

	print("calculate variant qc stats")
	mt = hl.variant_qc(mt, name='variant_qc')

	print("count males and females")
	tbl = mt.cols()
	nMales = tbl.aggregate(hl.agg.count_where(~ tbl.is_female))
	nFemales = tbl.aggregate(hl.agg.count_where(tbl.is_female))

	gt_codes = list(mt.entry)

	print("count male/female hets, homvars and called")
	mt = mt.annotate_rows(
		variant_qc = mt.variant_qc.annotate(
			n_male_het = hl.agg.count_where(~ mt.is_female & mt.GT.is_het()),
			n_male_hom_var = hl.agg.count_where(~ mt.is_female & mt.GT.is_hom_var()),
			n_male_called = hl.agg.count_where(~ mt.is_female & hl.is_defined(mt.GT)),
			n_female_het = hl.agg.count_where(mt.is_female & mt.GT.is_het()),
			n_female_hom_var = hl.agg.count_where(mt.is_female & mt.GT.is_hom_var()),
			n_female_called = hl.agg.count_where(mt.is_female & hl.is_defined(mt.GT)),
			het = mt.variant_qc.n_het / mt.variant_qc.n_called,
			avg_ab = hl.cond('AD' in gt_codes, hl.agg.mean(mt.AB), hl.null(hl.tfloat64)),
			avg_het_ab = hl.cond('AD' in gt_codes, hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB)), hl.null(hl.tfloat64))
		)
	)

	print("calculate call_rate, AC, and AF (accounting appropriately for sex chromosomes)")
	mt = mt.annotate_rows(
		variant_qc = mt.variant_qc.annotate(
			call_rate = hl.cond(mt.locus.in_y_nonpar(), mt.variant_qc.n_male_called / nMales, hl.cond(mt.locus.in_x_nonpar(), (mt.variant_qc.n_male_called + 2*mt.variant_qc.n_female_called) / (nMales + 2*nFemales), (mt.variant_qc.n_male_called + mt.variant_qc.n_female_called) / (nMales + nFemales))),
			AC = hl.cond(mt.locus.in_y_nonpar(),  mt.variant_qc.n_male_hom_var, hl.cond(mt.locus.in_x_nonpar(), mt.variant_qc.n_male_hom_var + mt.variant_qc.n_female_het + 2*mt.variant_qc.n_female_hom_var, mt.variant_qc.n_male_het + 2*mt.variant_qc.n_male_hom_var + mt.variant_qc.n_female_het + 2*mt.variant_qc.n_female_hom_var)),
			AF = hl.cond(mt.locus.in_y_nonpar(), mt.variant_qc.n_male_hom_var / mt.variant_qc.n_male_called, hl.cond(mt.locus.in_x_nonpar(), (mt.variant_qc.n_male_hom_var + mt.variant_qc.n_female_het + 2*mt.variant_qc.n_female_hom_var) / (mt.variant_qc.n_male_called + 2*mt.variant_qc.n_female_called), (mt.variant_qc.n_male_het + 2*mt.variant_qc.n_male_hom_var + mt.variant_qc.n_female_het + 2*mt.variant_qc.n_female_hom_var) / (2*mt.variant_qc.n_male_called + 2*mt.variant_qc.n_female_called))),
			het_freq_hwe = hl.cond(mt.locus.in_x_nonpar(), hl.agg.filter(mt.is_female, hl.agg.hardy_weinberg_test(mt.GT)).het_freq_hwe, hl.cond(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.agg.filter(~ mt.is_female, hl.agg.hardy_weinberg_test(mt.GT)).het_freq_hwe, hl.agg.hardy_weinberg_test(mt.GT).het_freq_hwe)),
			p_value_hwe = hl.cond(mt.locus.in_x_nonpar(), hl.agg.filter(mt.is_female, hl.agg.hardy_weinberg_test(mt.GT)).p_value, hl.cond(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.agg.filter(~ mt.is_female, hl.agg.hardy_weinberg_test(mt.GT)).p_value, hl.agg.hardy_weinberg_test(mt.GT).p_value))
		)
	)

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

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--sfilter', nargs=2, action='append', help='column name followed by expression; include samples satisfying this expression')
	parser.add_argument('--vfilter', nargs=2, action='append', help='column name followed by expression; include samples satisfying this expression')
	parser.add_argument('--samples-remove', help='a list of samples to remove before calculations')
	parser.add_argument('--samples-extract', help='a list of samples to extract before calculations')
	parser.add_argument('--variants-remove', help='a list of variants to remove before calculations')
	parser.add_argument('--variants-extract', help='a list of variants to extract before calculations')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail mt dataset name', required=True)
	requiredArgs.add_argument('--sexcheck-in', help='an imputed sexcheck output file from Hail', required=True)
	requiredArgs.add_argument('--samples-stats-out', help='a base filename for sample qc', required=True)
	requiredArgs.add_argument('--samples-exclude-out', help='a base filename for failed samples', required=True)
	requiredArgs.add_argument('--variants-stats-out', help='a base filename for variant qc', required=True)
	requiredArgs.add_argument('--variants-exclude-out', help='a base filename for failed variants', required=True)
	args = parser.parse_args()
	main(args)
