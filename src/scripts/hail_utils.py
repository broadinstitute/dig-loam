import hail as hl

def unphase_genotypes(mt: hl.MatrixTable) -> hl.MatrixTable:

	return mt.annotate_entries(
		GT=hl.case()
			.when(mt.GT.is_diploid(), hl.call(mt.GT[0], mt.GT[1], phased=False))
			.when(mt.GT.is_haploid(), hl.call(mt.GT[0], mt.GT[0], phased=False))
			.default(hl.missing(hl.tcall))
	)

def adjust_sex_chromosomes(mt: hl.MatrixTable, is_female: hl.tstr) -> hl.MatrixTable:

	return mt.annotate_entries(
		GT = hl.case(missing_false=True)
			.when(mt[is_female] & (mt.locus.in_y_par() | mt.locus.in_y_nonpar()), hl.missing(hl.tcall))
			.when((~ mt[is_female]) & (mt.locus.in_x_nonpar() | mt.locus.in_y_nonpar()) & mt.GT.is_het(), hl.missing(hl.tcall))
			.default(mt.GT)
	)

def annotate_sex(mt: hl.MatrixTable, ref_genome: hl.tstr, pheno_struct: hl.tstr = None, pheno_sex: hl.tstr = None, male_code: hl.tstr = None, female_code: hl.tstr = None) -> hl.MatrixTable:

	if pheno_sex is not None:

		mt = mt.annotate_cols(
			pheno_female = hl.if_else(~ hl.is_missing(mt[pheno_struct][pheno_sex]), (mt[pheno_struct][pheno_sex] == 'female') | (mt[pheno_struct][pheno_sex] == 'Female') | (mt[pheno_struct][pheno_sex] == 'f') | (mt[pheno_struct][pheno_sex] == 'F') | (mt[pheno_struct][pheno_sex] == female_code), False),
			pheno_male = hl.if_else(~ hl.is_missing(mt[pheno_struct][pheno_sex]), (mt[pheno_struct][pheno_sex] == 'male') | (mt[pheno_struct][pheno_sex] == 'Male') | (mt[pheno_struct][pheno_sex] == 'm') | (mt[pheno_struct][pheno_sex] == 'M') | (mt[pheno_struct][pheno_sex] == male_code), False)
		)

	if hl.filter_intervals(mt, [hl.parse_locus_interval(x, reference_genome=ref_genome) for x in hl.get_reference(ref_genome).x_contigs], keep=True).count()[0] > 0:
		tbl = hl.impute_sex(
			mt.filter_rows(
				(hl.is_missing(hl.len(mt.filters)) | (hl.len(mt.filters) == 0)) & 
				(mt.variant_qc_raw.AN > 1) & 
				((mt.variant_qc_raw.AF[1] > 0) & (mt.variant_qc_raw.AF[1] < 1)) & 
				(hl.is_snp(mt.alleles[0], mt.alleles[1])) & 
				(~ hl.is_mnp(mt.alleles[0], mt.alleles[1])) & 
				(~ hl.is_indel(mt.alleles[0], mt.alleles[1])) & 
				(~ hl.is_complex(mt.alleles[0], mt.alleles[1])),
				keep=True
			).GT
		)
	else:
		print("skipping sex imputation due to missing X chromosome data")
		tbl = mt.cols().select()
		tbl = tbl.annotate(
			is_female = hl.missing(hl.tbool),
			f_stat = hl.missing(hl.tfloat64),
			n_called = hl.missing(hl.tint64),
			expected_homs = hl.missing(hl.tfloat64),
			observed_homs = hl.missing(hl.tint64)
		)

	mt = mt.annotate_cols(impute_sex = tbl[mt.s])

	if pheno_sex is not None:
		mt = mt.annotate_cols(sexcheck = hl.if_else(~ hl.is_missing(mt[pheno_struct][pheno_sex]) & ~ hl.is_missing(mt.impute_sex.is_female), hl.if_else((mt.pheno_female & mt.impute_sex.is_female) | (mt.pheno_male & ~ mt.impute_sex.is_female), "OK", "PROBLEM"), "OK"))
		mt = mt.annotate_cols(is_female = hl.if_else(mt.pheno_female & hl.is_missing(mt.impute_sex.is_female), True, hl.if_else(mt.pheno_male & hl.is_missing(mt.impute_sex.is_female), False, mt.impute_sex.is_female)))
	else:
		mt = mt.annotate_cols(sexcheck = "OK")
		mt = mt.annotate_cols(is_female = hl.if_else(hl.is_missing(mt.impute_sex.is_female), hl.missing(hl.tbool), mt.impute_sex.is_female))

	return mt

def update_variant_qc(mt: hl.MatrixTable, is_female: hl.tstr, variant_qc: hl.tstr) -> hl.MatrixTable:

	gt_codes = list(mt.entry)

	num_males = mt.aggregate_cols(hl.agg.count_where(~ mt[is_female]))
	num_females = mt.aggregate_cols(hl.agg.count_where(mt[is_female]))
	
	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			n_male_het = hl.agg.count_where(mt.GT.is_het() & (~ mt[is_female])),
			n_male_homvar = hl.agg.count_where(mt.GT.is_hom_var() & (~ mt[is_female])),
			n_male_homref = hl.agg.count_where(mt.GT.is_hom_ref() & (~ mt[is_female])),
			n_male_called = hl.agg.count_where(hl.is_defined(mt.GT) & (~ mt[is_female])),
			n_female_het = hl.agg.count_where(mt.GT.is_het() & (mt[is_female])),
			n_female_homvar = hl.agg.count_where(mt.GT.is_hom_var() & (mt[is_female])),
			n_female_homref = hl.agg.count_where(mt.GT.is_hom_ref() & (mt[is_female])),
			n_female_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt[is_female]))
		)}
	)

	# if males are switched to haploid in_x_nonpar and in_y_nonpar use this:
	#		call_rate = (hl.case()
	#			.when(mt.locus.in_y_nonpar(), (mt[variant_qc].n_male_called / num_males))
	#			.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called) / (num_males + 2*num_females))
	#			.default((mt[variant_qc].n_male_called + mt[variant_qc].n_female_called) / (num_males + num_females))),
	#		AC = (hl.case()
	#			.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_male_homvar)
	#			.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar)
	#			.default(mt[variant_qc].n_male_het + 2*mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar)),
	#		AN = (hl.case()
	#			.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_male_called)
	#			.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called)
	#			.default(2*mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called)),
	#		AF = (hl.case()
	#			.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_male_homvar / mt[variant_qc].n_male_called)
	#			.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar) / (mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called))
	#			.default((mt[variant_qc].n_male_het + 2*mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar) / (2*mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called))),

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			call_rate = (hl.case()
				.when(mt.locus.in_y_nonpar(), (mt[variant_qc].n_male_called / num_males))
				.default((mt[variant_qc].n_male_called + mt[variant_qc].n_female_called) / (num_males + num_females))),
			AC = (hl.case()
				.when(mt.locus.in_y_nonpar(), 2*mt[variant_qc].n_male_homvar)
				.when(mt.locus.in_x_nonpar(), 2*mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar)
				.default(mt[variant_qc].n_male_het + 2*mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar)),
			AN = (hl.case()
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), 2*mt[variant_qc].n_male_called)
				.default(2*mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called)),
			AF = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_male_homvar / mt[variant_qc].n_male_called)
				.when(mt.locus.in_x_nonpar(), (2*mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar) / (2*mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called))
				.default((mt[variant_qc].n_male_het + 2*mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar) / (2*mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called))),
			het_freq_hwe = (hl.case()
				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female], hl.agg.hardy_weinberg_test(mt.GT)).het_freq_hwe)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
				.default(hl.agg.hardy_weinberg_test(mt.GT).het_freq_hwe)),
			p_value_hwe = (hl.case()
				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female], hl.agg.hardy_weinberg_test(mt.GT)).p_value)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
				.default(hl.agg.hardy_weinberg_test(mt.GT).p_value)),
			het = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_female_het / mt[variant_qc].n_female_called)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
				.default(mt[variant_qc].n_het / mt[variant_qc].n_called)),
			n_hom_var = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_female_homvar)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), mt[variant_qc].n_male_homvar)
				.default(mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_homvar)),
			n_hom_ref = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_female_homref)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), mt[variant_qc].n_male_homref)
				.default(mt[variant_qc].n_male_homref + mt[variant_qc].n_female_homref)),
			avg_ab = hl.if_else(
				'AD' in gt_codes,
				(hl.case()
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female], hl.agg.mean(mt.AB)))
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
					.default(hl.agg.mean(mt.AB))),
				hl.missing(hl.tfloat64)
			),
			avg_het_ab = hl.if_else(
				'AD' in gt_codes,
				(hl.case()
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female], hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
					.default(hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB)))),
				hl.missing(hl.tfloat64)
			),
			avg_alt_gq = hl.if_else(
				'GQ' in gt_codes,
				hl.agg.filter(mt.GT.is_non_ref(), hl.agg.mean(mt.GQ)),
				hl.missing(hl.tfloat64)
			),
			min_alt_gq = hl.if_else(
				'GQ' in gt_codes,
				hl.agg.filter(mt.GT.is_non_ref(), hl.agg.stats(mt.GQ).min),
				hl.missing(hl.tfloat64)
			),
			stddev_alt_gq = hl.if_else(
				'GQ' in gt_codes,
				hl.agg.filter(mt.GT.is_non_ref(), hl.agg.stats(mt.GQ).stdev),
				hl.missing(hl.tfloat64)
			),
			avg_hom_alt_gq = hl.if_else(
				'GQ' in gt_codes,
				hl.agg.filter((mt.GT.is_non_ref()) & (~ mt.GT.is_het()), hl.agg.mean(mt.GQ)),
				hl.missing(hl.tfloat64)
			),
			avg_het_alt_gq = hl.if_else(
				'GQ' in gt_codes,
				hl.agg.filter((mt.GT.is_non_ref()) & (mt.GT.is_het()), hl.agg.mean(mt.GQ)),
				hl.missing(hl.tfloat64)
			)
		)}
	)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			MAC = hl.if_else(
				mt[variant_qc].AF <= 0.5,
				mt[variant_qc].AC,
				2*mt[variant_qc].n_called - mt[variant_qc].AC
			),
			MAF = hl.if_else(
				mt[variant_qc].AF <= 0.5,
				mt[variant_qc].AF,
				1 - mt[variant_qc].AF
			)
		)}
	)

	return mt

#def add_case_ctrl_stats(mt: hl.MatrixTable, is_female: hl.tstr, variant_qc: hl.tstr, is_case: hl.tstr = None, diff_miss_min_expected_cell_count: hl.tint32 = 5) -> hl.MatrixTable:
#
#	gt_codes = list(mt.entry)
#
#	num_case_males = mt.aggregate_cols(hl.agg.count_where((~ mt[is_female]) & (mt.pheno[is_case] == 1)))
#	num_case_females = mt.aggregate_cols(hl.agg.count_where((mt[is_female]) & (mt.pheno[is_case] == 1)))
#	num_ctrl_males = mt.aggregate_cols(hl.agg.count_where((~ mt[is_female]) & (mt.pheno[is_case] == 0)))
#	num_ctrl_females = mt.aggregate_cols(hl.agg.count_where((mt[is_female]) & (mt.pheno[is_case] == 0)))
#
#	if variant_qc not in list(mt.row_value):
#		mt = mt.annotate_rows(
#			variant_qc = hl.struct()
#		)
#
#	mt = mt.annotate_rows(
#		**{variant_qc: mt[variant_qc].annotate(
#			n_case_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt.pheno[is_case] == 1)),
#			n_case_not_called = hl.agg.count_where((~ hl.is_defined(mt.GT)) & (mt.pheno[is_case] == 1)),
#			n_case_male_het = hl.agg.count_where(mt.GT.is_het() & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
#			n_case_male_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
#			n_case_male_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
#			n_case_male_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
#			n_case_male_called = hl.agg.count_where(hl.is_defined(mt.GT) & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
#			n_case_female_het = hl.agg.count_where(mt.GT.is_het() & (mt[is_female]) & (mt.pheno[is_case] == 1)),
#			n_case_female_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (mt[is_female]) & (mt.pheno[is_case] == 1)),
#			n_case_female_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (mt[is_female]) & (mt.pheno[is_case] == 1)),
#			n_case_female_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (mt[is_female]) & (mt.pheno[is_case] == 1)),
#			n_case_female_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt[is_female]) & (mt.pheno[is_case] == 1)),
#			n_ctrl_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt.pheno[is_case] == 0)),
#			n_ctrl_not_called = hl.agg.count_where((~ hl.is_defined(mt.GT)) & (mt.pheno[is_case] == 0)),
#			n_ctrl_male_het = hl.agg.count_where(mt.GT.is_het() & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
#			n_ctrl_male_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
#			n_ctrl_male_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
#			n_ctrl_male_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
#			n_ctrl_male_called = hl.agg.count_where(hl.is_defined(mt.GT) & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
#			n_ctrl_female_het = hl.agg.count_where(mt.GT.is_het() & (mt[is_female]) & (mt.pheno[is_case] == 0)),
#			n_ctrl_female_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (mt[is_female]) & (mt.pheno[is_case] == 0)),
#			n_ctrl_female_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (mt[is_female]) & (mt.pheno[is_case] == 0)),
#			n_ctrl_female_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (mt[is_female]) & (mt.pheno[is_case] == 0)),
#			n_ctrl_female_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt[is_female]) & (mt.pheno[is_case] == 0))
#		)}
#	)
#
#	mt = mt.annotate_rows(
#		**{variant_qc: mt[variant_qc].annotate(
#			diff_miss_row1_sum = mt[variant_qc].n_case_called + mt[variant_qc].n_ctrl_called,
#			diff_miss_row2_sum = mt[variant_qc].n_case_not_called + mt[variant_qc].n_ctrl_not_called,
#			diff_miss_col1_sum = mt[variant_qc].n_case_called + mt[variant_qc].n_case_not_called,
#			diff_miss_col2_sum = mt[variant_qc].n_ctrl_called + mt[variant_qc].n_ctrl_not_called,
#			diff_miss_tbl_sum = mt[variant_qc].n_case_called + mt[variant_qc].n_ctrl_called + mt[variant_qc].n_case_not_called + mt[variant_qc].n_ctrl_not_called,
#			call_rate_case = (hl.case()
#				.when(mt.locus.in_y_nonpar(), (mt[variant_qc].n_case_male_called / num_case_males))
#				.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called) / (num_case_males + 2*num_case_females))
#				.default((mt[variant_qc].n_case_male_called + mt[variant_qc].n_case_female_called) / (num_case_males + num_case_females))),
#			call_rate_ctrl = (hl.case()
#				.when(mt.locus.in_y_nonpar(), (mt[variant_qc].n_ctrl_male_called / num_ctrl_males))
#				.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called) / (num_ctrl_males + 2*num_ctrl_females))
#				.default((mt[variant_qc].n_ctrl_male_called + mt[variant_qc].n_ctrl_female_called) / (num_ctrl_males + num_ctrl_females))),
#			AC_case = (hl.case()
#				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_hom_var)
#				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var)
#				.default(mt[variant_qc].n_case_male_het + 2*mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var)),
#			AC_ctrl = (hl.case()
#				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_hom_var)
#				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var)
#				.default(mt[variant_qc].n_ctrl_male_het + 2*mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var)),
#			AC_ref_case = (hl.case()
#				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_hom_ref)
#				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_case_male_hom_ref + mt[variant_qc].n_case_female_het_ref + 2*mt[variant_qc].n_case_female_hom_ref)
#				.default(mt[variant_qc].n_case_male_het_ref + 2*mt[variant_qc].n_case_male_hom_ref + mt[variant_qc].n_case_female_het_ref + 2*mt[variant_qc].n_case_female_hom_ref)),
#			AC_ref_ctrl = (hl.case()
#				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_hom_ref)
#				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_ctrl_male_hom_ref + mt[variant_qc].n_ctrl_female_het_ref + 2*mt[variant_qc].n_ctrl_female_hom_ref)
#				.default(mt[variant_qc].n_ctrl_male_het_ref + 2*mt[variant_qc].n_ctrl_male_hom_ref + mt[variant_qc].n_ctrl_female_het_ref + 2*mt[variant_qc].n_ctrl_female_hom_ref)),
#			AN_case = (hl.case()
#				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_called)
#				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called)
#				.default(2*mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called)),
#			AN_ctrl = (hl.case()
#				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_called)
#				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called)
#				.default(2*mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called)),
#			AF_case = (hl.case()
#				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_hom_var / mt[variant_qc].n_case_male_called)
#				.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var) / (mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called))
#				.default((mt[variant_qc].n_case_male_het + 2*mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var) / (2*mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called))),
#			AF_ctrl = (hl.case()
#				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_hom_var / mt[variant_qc].n_ctrl_male_called)
#				.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var) / (mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called))
#				.default((mt[variant_qc].n_ctrl_male_het + 2*mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var) / (2*mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called))),
#			het_freq_hwe_case = (hl.case()
#				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 1), hl.agg.hardy_weinberg_test(mt.GT)).het_freq_hwe)
#				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
#				.default(hl.agg.filter(mt.pheno[is_case] == 1, hl.agg.hardy_weinberg_test(mt.GT).het_freq_hwe))),
#			het_freq_hwe_ctrl = (hl.case()
#				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 0), hl.agg.hardy_weinberg_test(mt.GT)).het_freq_hwe)
#				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
#				.default(hl.agg.filter(mt.pheno[is_case] == 0, hl.agg.hardy_weinberg_test(mt.GT).het_freq_hwe))),
#			p_value_hwe_case = (hl.case()
#				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 1), hl.agg.hardy_weinberg_test(mt.GT)).p_value)
#				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
#				.default(hl.agg.filter(mt.pheno[is_case] == 1, hl.agg.hardy_weinberg_test(mt.GT).p_value))),
#			p_value_hwe_ctrl = (hl.case()
#				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 0), hl.agg.hardy_weinberg_test(mt.GT)).p_value)
#				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
#				.default(hl.agg.filter(mt.pheno[is_case] == 0, hl.agg.hardy_weinberg_test(mt.GT).p_value))),
#			het_case = (hl.case()
#				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_case_female_het / mt[variant_qc].n_case_female_called)
#				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
#				.default((mt[variant_qc].n_case_male_het + mt[variant_qc].n_case_female_het) / (mt[variant_qc].n_case_male_called + mt[variant_qc].n_case_female_called))),
#			het_ctrl = (hl.case()
#				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_ctrl_female_het / mt[variant_qc].n_ctrl_female_called)
#				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
#				.default((mt[variant_qc].n_ctrl_male_het + mt[variant_qc].n_ctrl_female_het) / (mt[variant_qc].n_ctrl_male_called + mt[variant_qc].n_ctrl_female_called))),
#			avg_ab_case = hl.if_else(
#				'AD' in gt_codes,
#				(hl.case()
#					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 1), hl.agg.mean(mt.AB)))
#					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
#					.default(hl.agg.filter(mt.pheno[is_case] == 1, hl.agg.mean(mt.AB)))),
#				hl.missing(hl.tfloat64)
#			),
#			avg_ab_ctrl = hl.if_else(
#				'AD' in gt_codes,
#				(hl.case()
#					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 0), hl.agg.mean(mt.AB)))
#					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
#					.default(hl.agg.filter(mt.pheno[is_case] == 0, hl.agg.mean(mt.AB)))),
#				hl.missing(hl.tfloat64)
#			),
#			avg_het_ab_case = hl.if_else(
#				'AD' in gt_codes,
#				(hl.case()
#					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 1), hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))
#					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
#					.default(hl.agg.filter(mt.pheno[is_case] == 1, hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))),
#				hl.missing(hl.tfloat64)
#			),
#			avg_het_ab_ctrl = hl.if_else(
#				'AD' in gt_codes,
#				(hl.case()
#					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 0), hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))
#					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.missing(hl.tfloat64))
#					.default(hl.agg.filter(mt.pheno[is_case] == 0, hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))),
#				hl.missing(hl.tfloat64)
#			),
#			avg_alt_gq_case = hl.if_else(
#				'GQ' in gt_codes,
#				hl.agg.filter((mt.pheno[is_case] == 1) & mt.GT.is_non_ref(), hl.agg.mean(mt.GQ)),
#				hl.missing(hl.tfloat64)
#			),
#			avg_alt_gq_ctrl = hl.if_else(
#				'GQ' in gt_codes,
#				hl.agg.filter((mt.pheno[is_case] == 0) & mt.GT.is_non_ref(), hl.agg.mean(mt.GQ)),
#				hl.missing(hl.tfloat64)
#			)
#		)}
#	)
#
#	mt = mt.annotate_rows(
#		**{variant_qc: mt[variant_qc].annotate(
#			MAC_case = hl.if_else(
#				mt[variant_qc].AF_case <= 0.5,
#				mt[variant_qc].AC_case,
#				2*mt[variant_qc].n_case_called - mt[variant_qc].AC_case
#			),
#			MAC_ctrl = hl.if_else(
#				mt[variant_qc].AF_ctrl <= 0.5,
#				mt[variant_qc].AC_ctrl,
#				2*mt[variant_qc].n_ctrl_called - mt[variant_qc].AC_ctrl
#			),
#			MAF_case = hl.if_else(
#				mt[variant_qc].AF_case <= 0.5,
#				mt[variant_qc].AF_case,
#				1 - mt[variant_qc].AF_case
#			),
#			MAF_ctrl = hl.if_else(
#				mt[variant_qc].AF_ctrl <= 0.5,
#				mt[variant_qc].AF_ctrl,
#				1 - mt[variant_qc].AF_ctrl
#			)
#		)}
#	)
#
#	mt = mt.annotate_rows(
#		**{variant_qc: mt[variant_qc].annotate(
#			diff_miss_expected_c1 = (mt[variant_qc].diff_miss_row1_sum * mt[variant_qc].diff_miss_col1_sum) / mt[variant_qc].diff_miss_tbl_sum,
#			diff_miss_expected_c2 = (mt[variant_qc].diff_miss_row1_sum * mt[variant_qc].diff_miss_col2_sum) / mt[variant_qc].diff_miss_tbl_sum,
#			diff_miss_expected_c3 = (mt[variant_qc].diff_miss_row2_sum * mt[variant_qc].diff_miss_col1_sum) / mt[variant_qc].diff_miss_tbl_sum,
#			diff_miss_expected_c4 = (mt[variant_qc].diff_miss_row2_sum * mt[variant_qc].diff_miss_col2_sum) / mt[variant_qc].diff_miss_tbl_sum
#		)}
#	)
#
#	mt = mt.annotate_rows(
#		**{variant_qc: mt[variant_qc].annotate(
#			diff_miss = hl.if_else(
#				((hl.int32(mt[variant_qc].n_case_not_called) == 0) & (hl.int32(mt[variant_qc].n_ctrl_not_called) == 0)), 
#				hl.struct(p_value = 1.0, odds_ratio = hl.missing(hl.tfloat64), ci_95_lower = hl.missing(hl.tfloat64), ci_95_upper = hl.missing(hl.tfloat64), test = 'NA'),
#				hl.if_else(
#					((mt[variant_qc].diff_miss_expected_c1 < diff_miss_min_expected_cell_count) | (mt[variant_qc].diff_miss_expected_c2 < diff_miss_min_expected_cell_count) | (mt[variant_qc].diff_miss_expected_c3 < diff_miss_min_expected_cell_count) | (mt[variant_qc].diff_miss_expected_c4 < diff_miss_min_expected_cell_count)),
#					hl.fisher_exact_test(hl.int32(mt[variant_qc].n_case_called), hl.int32(mt[variant_qc].n_case_not_called), hl.int32(mt[variant_qc].n_ctrl_called), hl.int32(mt[variant_qc].n_ctrl_not_called)).annotate(test = 'fisher_exact'),
#					hl.chi_squared_test(hl.int32(mt[variant_qc].n_case_called), hl.int32(mt[variant_qc].n_case_not_called), hl.int32(mt[variant_qc].n_ctrl_called), hl.int32(mt[variant_qc].n_ctrl_not_called)).annotate(ci_95_lower = hl.missing(hl.tfloat64), ci_95_upper = hl.missing(hl.tfloat64), test = 'chi_squared')
#				)
#			)
#		)}
#	)
#
#	return mt

def add_diff_miss(mt: hl.MatrixTable, is_female: hl.tstr, variant_qc: hl.tstr, is_case: hl.tstr = None, diff_miss_min_expected_cell_count: hl.tint32 = 5) -> hl.MatrixTable:

	if variant_qc not in list(mt.row_value):
		mt = mt.annotate_rows(
			variant_qc = hl.struct()
		)

	if 'n_case_called' not in list(mt[variant_qc].keys()):
		mt = mt.annotate_rows(**{variant_qc: mt[variant_qc].annotate(n_case_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt.pheno[is_case] == 1)))})

	if 'n_case_not_called' not in list(mt[variant_qc].keys()):
		mt = mt.annotate_rows(**{variant_qc: mt[variant_qc].annotate(n_case_not_called = hl.agg.count_where((~ hl.is_defined(mt.GT)) & (mt.pheno[is_case] == 1)))})

	if 'n_ctrl_called' not in list(mt[variant_qc].keys()):
		mt = mt.annotate_rows(**{variant_qc: mt[variant_qc].annotate(n_ctrl_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt.pheno[is_case] == 0)))})

	if 'n_ctrl_not_called' not in list(mt[variant_qc].keys()):
		mt = mt.annotate_rows(**{variant_qc: mt[variant_qc].annotate(n_ctrl_not_called = hl.agg.count_where((~ hl.is_defined(mt.GT)) & (mt.pheno[is_case] == 0)))})

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			diff_miss_row1_sum = mt[variant_qc].n_case_called + mt[variant_qc].n_ctrl_called,
			diff_miss_row2_sum = mt[variant_qc].n_case_not_called + mt[variant_qc].n_ctrl_not_called,
			diff_miss_col1_sum = mt[variant_qc].n_case_called + mt[variant_qc].n_case_not_called,
			diff_miss_col2_sum = mt[variant_qc].n_ctrl_called + mt[variant_qc].n_ctrl_not_called,
			diff_miss_tbl_sum = mt[variant_qc].n_case_called + mt[variant_qc].n_ctrl_called + mt[variant_qc].n_case_not_called + mt[variant_qc].n_ctrl_not_called
		)}
	)
	
	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			diff_miss_expected_c1 = (mt[variant_qc].diff_miss_row1_sum * mt[variant_qc].diff_miss_col1_sum) / mt[variant_qc].diff_miss_tbl_sum,
			diff_miss_expected_c2 = (mt[variant_qc].diff_miss_row1_sum * mt[variant_qc].diff_miss_col2_sum) / mt[variant_qc].diff_miss_tbl_sum,
			diff_miss_expected_c3 = (mt[variant_qc].diff_miss_row2_sum * mt[variant_qc].diff_miss_col1_sum) / mt[variant_qc].diff_miss_tbl_sum,
			diff_miss_expected_c4 = (mt[variant_qc].diff_miss_row2_sum * mt[variant_qc].diff_miss_col2_sum) / mt[variant_qc].diff_miss_tbl_sum
		)}
	)
	
	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			diff_miss = hl.if_else(
				((hl.int32(mt[variant_qc].n_case_not_called) == 0) & (hl.int32(mt[variant_qc].n_ctrl_not_called) == 0)), 
				hl.struct(p_value = 1.0, odds_ratio = hl.missing(hl.tfloat64), ci_95_lower = hl.missing(hl.tfloat64), ci_95_upper = hl.missing(hl.tfloat64), test = 'NA'),
				hl.if_else(
					((mt[variant_qc].diff_miss_expected_c1 < diff_miss_min_expected_cell_count) | (mt[variant_qc].diff_miss_expected_c2 < diff_miss_min_expected_cell_count) | (mt[variant_qc].diff_miss_expected_c3 < diff_miss_min_expected_cell_count) | (mt[variant_qc].diff_miss_expected_c4 < diff_miss_min_expected_cell_count)),
					hl.fisher_exact_test(hl.int32(mt[variant_qc].n_case_called), hl.int32(mt[variant_qc].n_case_not_called), hl.int32(mt[variant_qc].n_ctrl_called), hl.int32(mt[variant_qc].n_ctrl_not_called)).annotate(test = 'fisher_exact'),
					hl.chi_squared_test(hl.int32(mt[variant_qc].n_case_called), hl.int32(mt[variant_qc].n_case_not_called), hl.int32(mt[variant_qc].n_ctrl_called), hl.int32(mt[variant_qc].n_ctrl_not_called)).annotate(ci_95_lower = hl.missing(hl.tfloat64), ci_95_upper = hl.missing(hl.tfloat64), test = 'chi_squared')
				)
			)
		)}
	)

	return mt

def add_case_ctrl_stats_results(mt: hl.MatrixTable, is_female: hl.tstr, variant_qc: hl.tstr, is_case: hl.tstr = None) -> hl.MatrixTable:

	if variant_qc not in list(mt.row_value):
		mt = mt.annotate_rows(
			variant_qc = hl.struct()
		)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			n_case_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt.pheno[is_case] == 1)),
			n_case_male_het = hl.agg.count_where(mt.GT.is_het() & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_male_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_male_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_male_called = hl.agg.count_where(hl.is_defined(mt.GT) & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_female_het = hl.agg.count_where(mt.GT.is_het() & (mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_female_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_female_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_female_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_ctrl_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt.pheno[is_case] == 0)),
			n_ctrl_male_het = hl.agg.count_where(mt.GT.is_het() & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_male_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_male_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_male_called = hl.agg.count_where(hl.is_defined(mt.GT) & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_female_het = hl.agg.count_where(mt.GT.is_het() & (mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_female_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_female_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_female_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt[is_female]) & (mt.pheno[is_case] == 0))
		)}
	)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			AC_case = (hl.case()
				.when(mt.locus.in_y_nonpar(), 2*mt[variant_qc].n_case_male_hom_var)
				.when(mt.locus.in_x_nonpar(), 2*mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var)
				.default(mt[variant_qc].n_case_male_het + 2*mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var)),
			AC_ctrl = (hl.case()
				.when(mt.locus.in_y_nonpar(), 2*mt[variant_qc].n_ctrl_male_hom_var)
				.when(mt.locus.in_x_nonpar(), 2*mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var)
				.default(mt[variant_qc].n_ctrl_male_het + 2*mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var)),
			AF_case = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_hom_var / mt[variant_qc].n_case_male_called)
				.when(mt.locus.in_x_nonpar(), (2*mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var) / (2*mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called))
				.default((mt[variant_qc].n_case_male_het + 2*mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var) / (2*mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called))),
			AF_ctrl = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_hom_var / mt[variant_qc].n_ctrl_male_called)
				.when(mt.locus.in_x_nonpar(), (2*mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var) / (2*mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called))
				.default((mt[variant_qc].n_ctrl_male_het + 2*mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var) / (2*mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called))),
			n_hom_var_case = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_case_female_hom_var)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_hom_var)
				.default(mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_hom_var)),
			n_hom_var_ctrl = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_ctrl_female_hom_var)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_hom_var)
				.default(mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_hom_var)),
			n_hom_ref_case = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_case_female_hom_ref)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_hom_ref)
				.default(mt[variant_qc].n_case_male_hom_ref + mt[variant_qc].n_case_female_hom_ref)),
			n_hom_ref_ctrl = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_ctrl_female_hom_ref)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_hom_ref)
				.default(mt[variant_qc].n_ctrl_male_hom_ref + mt[variant_qc].n_ctrl_female_hom_ref)),
			n_het_case = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_case_female_het)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_het)
				.default(mt[variant_qc].n_case_male_het + mt[variant_qc].n_case_female_het)),
			n_het_ctrl = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_ctrl_female_het)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_het)
				.default(mt[variant_qc].n_ctrl_male_het + mt[variant_qc].n_ctrl_female_het))
		)}
	)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			MAC_case = hl.if_else(
				mt[variant_qc].AF_case <= 0.5,
				mt[variant_qc].AC_case,
				2*mt[variant_qc].n_case_called - mt[variant_qc].AC_case
			),
			MAC_ctrl = hl.if_else(
				mt[variant_qc].AF_ctrl <= 0.5,
				mt[variant_qc].AC_ctrl,
				2*mt[variant_qc].n_ctrl_called - mt[variant_qc].AC_ctrl
			),
			MAF_case = hl.if_else(
				mt[variant_qc].AF_case <= 0.5,
				mt[variant_qc].AF_case,
				1 - mt[variant_qc].AF_case
			),
			MAF_ctrl = hl.if_else(
				mt[variant_qc].AF_ctrl <= 0.5,
				mt[variant_qc].AF_ctrl,
				1 - mt[variant_qc].AF_ctrl
			)
		)}
	)

	return mt

def add_fet_assoc(mt: hl.MatrixTable, variant_qc: hl.tstr, is_case: hl.tstr = None) -> hl.MatrixTable:

	if 'variant_qc' not in list(mt.row_value):
		print("ERROR: must run methods 'variant_qc' and 'add_case_ctrl_stats' prior to running 'add_fet_assoc'")
	elif 'AC_case' not in list(mt[variant_qc].keys()):
		print("ERROR: must run method 'add_case_ctrl_stats' prior to running 'add_fet_assoc'")
	else:
		mt = mt.annotate_rows(
			**{variant_qc: mt[variant_qc].annotate(
				fet_assoc = hl.fisher_exact_test(hl.int32(mt[variant_qc].AC_case), hl.int32(mt[variant_qc].AC_ref_case), hl.int32(mt[variant_qc].AC_ctrl), hl.int32(mt[variant_qc].AC_ref_ctrl))
			)}
		)

	return mt

def add_sample_qc_stats(mt: hl.MatrixTable, sample_qc: hl.tstr, variant_qc: hl.tstr) -> hl.MatrixTable:

	mt = mt.annotate_cols(
		**{sample_qc: mt[sample_qc].annotate(
			n_het_low = hl.agg.count_where((mt[variant_qc].AF[1] < 0.03) & mt.GT.is_het()), 
			n_het_high = hl.agg.count_where((mt[variant_qc].AF[1] >= 0.03) & mt.GT.is_het()), 
			n_called_low = hl.agg.count_where((mt[variant_qc].AF[1] < 0.03) & ~hl.is_missing(mt.GT)), 
			n_called_high = hl.agg.count_where((mt[variant_qc].AF[1] >= 0.03) & ~hl.is_missing(mt.GT)),
			avg_ab = hl.agg.mean(mt.AB),
			avg_ab50 = hl.agg.mean(mt.AB50)
		)}
	)

	return mt.annotate_cols(
		**{sample_qc: mt[sample_qc].annotate(
			het = mt[sample_qc].n_het / mt[sample_qc].n_called,
			het_low = mt[sample_qc].n_het_low / mt[sample_qc].n_called_low,
			het_high = mt[sample_qc].n_het_high / mt[sample_qc].n_called_high
		)}
	)

def mt_add_col_filters(mt: hl.MatrixTable, filters: hl.tarray, struct_name: hl.tstr, missing_false: hl.tbool = True) -> hl.MatrixTable:
	mt = mt.annotate_cols(**{struct_name: hl.struct(exclude = 0)})
	for f in filters:
		absent = False
		zero_stddev = False
		all_miss = False
		single_val = False
		for field in f[1].split(","):
			if field not in mt.cols().col_value.flatten():
				absent = True
			dt = eval("mt." + field).dtype
			if dt in [hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64]:
				field_stats = mt.aggregate_cols(hl.agg.stats(eval("mt." + field)))
				if field_stats.stdev == 0:
					zero_stddev = True
				if field_stats.n == 0:
					all_miss = True
			else:
				if len(mt.aggregate_cols(hl.agg.collect_as_set(eval("mt." + field)))):
					single_val = True
			f[2] = f[2].replace(field,"mt." + field)
		if not absent and not zero_stddev and not all_miss and not single_val:
			print("filter samples based on configuration filter " + f[0] + " for field/s " + f[1])
			mt = mt.annotate_cols(
				**{struct_name: mt[struct_name].annotate(
					**{f[0]: hl.if_else(eval(f[2]), 0, 1, missing_false = missing_false)}
				)}
			)
		else:
			if absent:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... 1 or more fields do not exist")
			if zero_stddev:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... standard deviation is zero")
			if all_miss:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... all records missing")
			if single_val:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... all records missing or single value")
			mt = mt.annotate_cols(
				**{struct_name: mt[struct_name].annotate(
					**{f[0]: 0}
				)}
			)
		print("update exclusion column based on " + f[0])
		mt = mt.annotate_cols(
			**{struct_name: mt[struct_name].annotate(
				exclude = hl.if_else(
					mt[struct_name][f[0]] == 1,
					1,
					mt[struct_name].exclude
				)
			)}
		)
	return mt

def mt_add_row_filters(mt: hl.MatrixTable, filters: hl.tarray, struct_name: hl.tstr, missing_false: hl.tbool = True) -> hl.MatrixTable:
	mt = mt.annotate_rows(**{struct_name: hl.struct(exclude = 0)})
	for f in filters:
		absent = False
		zero_stddev = False
		all_miss = False
		single_val = False
		for field in f[1].split(","):
			if field not in mt.rows().row_value.flatten():
				absent = True
			dt = eval("mt." + field).dtype
			if dt in [hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64]:
				field_stats = mt.aggregate_rows(hl.agg.stats(eval("mt." + field)))
				if field_stats.stdev == 0:
					zero_stddev = True
				if field_stats.n == 0:
					all_miss = True
			else:
				if len(mt.aggregate_rows(hl.agg.collect_as_set(eval("mt." + field)))):
					single_val = True
			f[2] = f[2].replace(field,"mt." + field)
		if not absent and not zero_stddev and not all_miss and not single_val:
			print("filter variants based on configuration filter " + f[0] + " for field/s " + f[1])
			mt = mt.annotate_rows(
				**{struct_name: mt[struct_name].annotate(
					**{f[0]: hl.if_else(eval(f[2]), 0, 1, missing_false = missing_false)}
				)}
			)
		else:
			if absent:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... 1 or more fields do not exist")
			if zero_stddev:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... standard deviation is zero")
			if all_miss:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... all records missing")
			if single_val:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... all records missing or single value")
			mt = mt.annotate_rows(
				**{struct_name: mt[struct_name].annotate(
					**{f[0]: 0}
				)}
			)
		print("update exclusion column based on " + f[0])
		mt = mt.annotate_rows(
			**{struct_name: mt[struct_name].annotate(
				exclude = hl.if_else(
					mt[struct_name][f[0]] == 1,
					1,
					mt[struct_name].exclude
				)
			)}
		)
	return mt

def ht_add_filters(ht: hl.Table, filters: hl.tarray, struct_name: hl.tstr, missing_false: hl.tbool = True) -> hl.Table:
	import hail as hl
	local_scope = locals()
	ht = ht.annotate(**{struct_name: hl.struct(exclude = 0)})
	for f in filters:
		absent = False
		zero_stddev = False
		all_miss = False
		single_val = False
		for field in list(set(f[1].split(","))):
			if field not in ht.row_value.flatten():
				print("missing field: " + field)
				absent = True
			f[1] = f[1].replace(field, field.split(".")[0] + "".join(["['" + x + "']" for x in field.split(".")[1:]]))
			f[2] = f[2].replace(field, field.split(".")[0] + "".join(["['" + x + "']" for x in field.split(".")[1:]]))
		for field in list(set(f[1].split(","))):
			dt = eval("ht." + field).dtype
			if dt in [hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64]:
				field_stats = ht.aggregate(hl.agg.stats(eval("ht." + field)))
				if field_stats.stdev == 0:
					zero_stddev = True
				if field_stats.n == 0:
					all_miss = True
			else:
				non_numeric_fields = ht.aggregate(hl.agg.collect_as_set(eval("ht." + field)))
				if len(non_numeric_fields) > 20:
					print("found > 20 values in non-numeric field " + field)
				else:
					print("found values " + str(non_numeric_fields) + " in non-numeric field " + field)
					if len(non_numeric_fields) == 1:
						single_val = True
			f[2] = f[2].replace(field,"ht." + field)
		if not absent and not zero_stddev and not all_miss and not single_val:
			print("filter table based on configuration filter " + f[0] + " for field/s " + f[1])
			ht = ht.annotate(
				**{struct_name: ht[struct_name].annotate(
					**{f[0]: hl.if_else(eval(f[2], local_scope), 0, 1, missing_false = missing_false)}
				)}
			)
		else:
			if absent:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... 1 or more fields do not exist")
			if zero_stddev:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... standard deviation is zero")
			if all_miss:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... all records missing")
			if single_val:
				print("skipping configuration filter " + f[0] + " for field/s " + f[1] + "... all records missing or single value")
			ht = ht.annotate(
				**{struct_name: ht[struct_name].annotate(
					**{f[0]: 0}
				)}
			)
		print("update exclusion column based on " + f[0])
		ht = ht.annotate(
			**{struct_name: ht[struct_name].annotate(
				exclude = hl.if_else(
					ht[struct_name][f[0]] == 1,
					1,
					ht[struct_name].exclude
				)
			)}
		)
	return ht
