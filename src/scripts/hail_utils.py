import hail as hl

def unphase_genotypes(mt: hl.MatrixTable) -> hl.MatrixTable:

	return mt.annotate_entries(
		GT=hl.case()
			.when(mt.GT.is_diploid(), hl.call(mt.GT[0], mt.GT[1], phased=False))
			.when(mt.GT.is_haploid(), hl.call(mt.GT[0], mt.GT[0], phased=False))
			.default(hl.null(hl.tcall))
	)

def adjust_sex_chromosomes(mt: hl.MatrixTable, is_female: hl.tstr) -> hl.MatrixTable:

	return mt.annotate_entries(
		GT = hl.case(missing_false=True)
			.when(mt[is_female] & (mt.locus.in_y_par() | mt.locus.in_y_nonpar()), hl.null(hl.tcall))
			.when((~ mt[is_female]) & (mt.locus.in_x_nonpar() | mt.locus.in_y_nonpar()) & mt.GT.is_het(), hl.null(hl.tcall))
			.default(mt.GT)
	)

def annotate_sex(mt: hl.MatrixTable, ref_genome: hl.genetics.ReferenceGenome, pheno_struct: hl.tstr, pheno_sex: hl.tstr, male_code: hl.tstr, female_code: hl.tstr) -> hl.MatrixTable:

	mt = mt.annotate_cols(
		pheno_female = hl.cond(~ hl.is_missing(mt[pheno_struct][pheno_sex]), (mt[pheno_struct][pheno_sex] == 'female') | (mt[pheno_struct][pheno_sex] == 'Female') | (mt[pheno_struct][pheno_sex] == 'f') | (mt[pheno_struct][pheno_sex] == 'F') | (mt[pheno_struct][pheno_sex] == female_code), False),
		pheno_male = hl.cond(~ hl.is_missing(mt[pheno_struct][pheno_sex]), (mt[pheno_struct][pheno_sex] == 'male') | (mt[pheno_struct][pheno_sex] == 'Male') | (mt[pheno_struct][pheno_sex] == 'm') | (mt[pheno_struct][pheno_sex] == 'M') | (mt[pheno_struct][pheno_sex] == male_code), False)
	)

	if hl.filter_intervals(mt, [hl.parse_locus_interval(x) for x in ref_genome.x_contigs], keep=True).count()[0] > 0:
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
			is_female = hl.null(hl.tbool),
			f_stat = hl.null(hl.tfloat64),
			n_called = hl.null(hl.tint64),
			expected_homs = hl.null(hl.tfloat64),
			observed_homs = hl.null(hl.tint64)
		)

	mt = mt.annotate_cols(impute_sex = tbl[mt.s])

	mt = mt.annotate_cols(sexcheck = hl.cond(~ hl.is_missing(mt[pheno_struct][pheno_sex]) & ~ hl.is_missing(mt.impute_sex.is_female), hl.cond((mt.pheno_female & mt.impute_sex.is_female) | (mt.pheno_male & ~ mt.impute_sex.is_female), "OK", "PROBLEM"), "OK"))

	return mt.annotate_cols(is_female = hl.cond(mt.pheno_female & hl.is_missing(mt.impute_sex.is_female), True, hl.cond(mt.pheno_male & hl.is_missing(mt.impute_sex.is_female), False, mt.impute_sex.is_female)))

def update_variant_qc(mt: hl.MatrixTable, is_female: hl.tstr, variant_qc: hl.tstr) -> hl.MatrixTable:

	gt_codes = list(mt.entry)

	num_males = mt.aggregate_cols(hl.agg.count_where(~ mt[is_female]))
	num_females = mt.aggregate_cols(hl.agg.count_where(mt[is_female]))
	
	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			n_male_het = hl.agg.count_where(mt.GT.is_het() & (~ mt[is_female])),
			n_male_homvar = hl.agg.count_where(mt.GT.is_hom_var() & (~ mt[is_female])),
			n_male_called = hl.agg.count_where(hl.is_defined(mt.GT) & (~ mt[is_female])),
			n_female_het = hl.agg.count_where(mt.GT.is_het() & (mt[is_female])),
			n_female_homvar = hl.agg.count_where(mt.GT.is_hom_var() & (mt[is_female])),
			n_female_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt[is_female]))
		)}
	)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			call_rate = (hl.case()
				.when(mt.locus.in_y_nonpar(), (mt[variant_qc].n_male_called / num_males))
				.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called) / (num_males + 2*num_females))
				.default((mt[variant_qc].n_male_called + mt[variant_qc].n_female_called) / (num_males + num_females))),
			AC = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_male_homvar)
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar)
				.default(mt[variant_qc].n_male_het + 2*mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar)),
			AN = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_male_called)
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called)
				.default(2*mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called)),
			AF = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_male_homvar / mt[variant_qc].n_male_called)
				.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar) / (mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called))
				.default((mt[variant_qc].n_male_het + 2*mt[variant_qc].n_male_homvar + mt[variant_qc].n_female_het + 2*mt[variant_qc].n_female_homvar) / (2*mt[variant_qc].n_male_called + 2*mt[variant_qc].n_female_called))),
			het_freq_hwe = (hl.case()
				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female], hl.agg.hardy_weinberg_test(mt.GT)).het_freq_hwe)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
				.default(hl.agg.hardy_weinberg_test(mt.GT).het_freq_hwe)),
			p_value_hwe = (hl.case()
				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female], hl.agg.hardy_weinberg_test(mt.GT)).p_value)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
				.default(hl.agg.hardy_weinberg_test(mt.GT).p_value)),
			het = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_female_het / mt[variant_qc].n_female_called)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
				.default(mt[variant_qc].n_het / mt[variant_qc].n_called)),
			avg_ab = hl.cond(
				'AD' in gt_codes,
				(hl.case()
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female], hl.agg.mean(mt.AB)))
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
					.default(hl.agg.mean(mt.AB))),
				hl.null(hl.tfloat64)
			),
			avg_het_ab = hl.cond(
				'AD' in gt_codes,
				(hl.case()
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female], hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
					.default(hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB)))),
				hl.null(hl.tfloat64)
			),
			avg_alt_gq = hl.cond(
				'GQ' in gt_codes,
				hl.agg.filter(mt.GT.is_non_ref(), hl.agg.mean(mt.GQ)),
				hl.null(hl.tfloat64)
			)
		)}
	)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			MAC = hl.cond(
				mt[variant_qc].AF <= 0.5,
				mt[variant_qc].AC,
				2*mt[variant_qc].n_called - mt[variant_qc].AC
			),
			MAF = hl.cond(
				mt[variant_qc].AF <= 0.5,
				mt[variant_qc].AF,
				1 - mt[variant_qc].AF
			)
		)}
	)

	return mt

def add_case_ctrl_stats(mt: hl.MatrixTable, is_female: hl.tstr, variant_qc: hl.tstr, is_case: hl.tstr = None) -> hl.MatrixTable:

	num_case_males = mt.aggregate_cols(hl.agg.count_where((~ mt[is_female]) & (mt.pheno[is_case] == 1)))
	num_case_females = mt.aggregate_cols(hl.agg.count_where((mt[is_female]) & (mt.pheno[is_case] == 1)))
	num_ctrl_males = mt.aggregate_cols(hl.agg.count_where((~ mt[is_female]) & (mt.pheno[is_case] == 0)))
	num_ctrl_females = mt.aggregate_cols(hl.agg.count_where((mt[is_female]) & (mt.pheno[is_case] == 0)))

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			n_case_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt.pheno[is_case] == 1)),
			n_case_not_called = hl.agg.count_where((~ hl.is_defined(mt.GT)) & (mt.pheno[is_case] == 1)),
			n_case_male_het = hl.agg.count_where(mt.GT.is_het() & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_male_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_male_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_male_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_male_called = hl.agg.count_where(hl.is_defined(mt.GT) & (~ mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_female_het = hl.agg.count_where(mt.GT.is_het() & (mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_female_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_female_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_female_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_case_female_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt[is_female]) & (mt.pheno[is_case] == 1)),
			n_ctrl_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt.pheno[is_case] == 0)),
			n_ctrl_not_called = hl.agg.count_where((~ hl.is_defined(mt.GT)) & (mt.pheno[is_case] == 0)),
			n_ctrl_male_het = hl.agg.count_where(mt.GT.is_het() & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_male_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_male_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_male_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_male_called = hl.agg.count_where(hl.is_defined(mt.GT) & (~ mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_female_het = hl.agg.count_where(mt.GT.is_het() & (mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_female_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_female_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_female_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (mt[is_female]) & (mt.pheno[is_case] == 0)),
			n_ctrl_female_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt[is_female]) & (mt.pheno[is_case] == 0))
		)}
	)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			call_rate_case = (hl.case()
				.when(mt.locus.in_y_nonpar(), (mt[variant_qc].n_case_male_called / num_case_males))
				.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called) / (num_case_males + 2*num_case_females))
				.default((mt[variant_qc].n_case_male_called + mt[variant_qc].n_case_female_called) / (num_case_males + num_case_females))),
			call_rate_ctrl = (hl.case()
				.when(mt.locus.in_y_nonpar(), (mt[variant_qc].n_ctrl_male_called / num_ctrl_males))
				.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called) / (num_ctrl_males + 2*num_ctrl_females))
				.default((mt[variant_qc].n_ctrl_male_called + mt[variant_qc].n_ctrl_female_called) / (num_ctrl_males + num_ctrl_females))),
			AC_case = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_hom_var)
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var)
				.default(mt[variant_qc].n_case_male_het + 2*mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var)),
			AC_ctrl = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_hom_var)
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var)
				.default(mt[variant_qc].n_ctrl_male_het + 2*mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var)),
			AC_ref_case = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_hom_ref)
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_case_male_hom_ref + mt[variant_qc].n_case_female_het_ref + 2*mt[variant_qc].n_case_female_hom_ref)
				.default(mt[variant_qc].n_case_male_het_ref + 2*mt[variant_qc].n_case_male_hom_ref + mt[variant_qc].n_case_female_het_ref + 2*mt[variant_qc].n_case_female_hom_ref)),
			AC_ref_ctrl = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_hom_ref)
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_ctrl_male_hom_ref + mt[variant_qc].n_ctrl_female_het_ref + 2*mt[variant_qc].n_ctrl_female_hom_ref)
				.default(mt[variant_qc].n_ctrl_male_het_ref + 2*mt[variant_qc].n_ctrl_male_hom_ref + mt[variant_qc].n_ctrl_female_het_ref + 2*mt[variant_qc].n_ctrl_female_hom_ref)),
			AN_case = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_called)
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called)
				.default(2*mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called)),
			AN_ctrl = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_called)
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called)
				.default(2*mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called)),
			AF_case = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_case_male_hom_var / mt[variant_qc].n_case_male_called)
				.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var) / (mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called))
				.default((mt[variant_qc].n_case_male_het + 2*mt[variant_qc].n_case_male_hom_var + mt[variant_qc].n_case_female_het + 2*mt[variant_qc].n_case_female_hom_var) / (2*mt[variant_qc].n_case_male_called + 2*mt[variant_qc].n_case_female_called))),
			AF_ctrl = (hl.case()
				.when(mt.locus.in_y_nonpar(), mt[variant_qc].n_ctrl_male_hom_var / mt[variant_qc].n_ctrl_male_called)
				.when(mt.locus.in_x_nonpar(), (mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var) / (mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called))
				.default((mt[variant_qc].n_ctrl_male_het + 2*mt[variant_qc].n_ctrl_male_hom_var + mt[variant_qc].n_ctrl_female_het + 2*mt[variant_qc].n_ctrl_female_hom_var) / (2*mt[variant_qc].n_ctrl_male_called + 2*mt[variant_qc].n_ctrl_female_called))),
			het_freq_hwe_case = (hl.case()
				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 1), hl.agg.hardy_weinberg_test(mt.GT)).het_freq_hwe)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
				.default(hl.agg.filter(mt.pheno[is_case] == 1, hl.agg.hardy_weinberg_test(mt.GT).het_freq_hwe))),
			het_freq_hwe_ctrl = (hl.case()
				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 0), hl.agg.hardy_weinberg_test(mt.GT)).het_freq_hwe)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
				.default(hl.agg.filter(mt.pheno[is_case] == 0, hl.agg.hardy_weinberg_test(mt.GT).het_freq_hwe))),
			p_value_hwe_case = (hl.case()
				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 1), hl.agg.hardy_weinberg_test(mt.GT)).p_value)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
				.default(hl.agg.filter(mt.pheno[is_case] == 1, hl.agg.hardy_weinberg_test(mt.GT).p_value))),
			p_value_hwe_ctrl = (hl.case()
				.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 0), hl.agg.hardy_weinberg_test(mt.GT)).p_value)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
				.default(hl.agg.filter(mt.pheno[is_case] == 0, hl.agg.hardy_weinberg_test(mt.GT).p_value))),
			het_case = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_case_female_het / mt[variant_qc].n_case_female_called)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
				.default((mt[variant_qc].n_case_male_het + mt[variant_qc].n_case_female_het) / (mt[variant_qc].n_case_male_called + mt[variant_qc].n_case_female_called))),
			het_ctrl = (hl.case()
				.when(mt.locus.in_x_nonpar(), mt[variant_qc].n_ctrl_female_het / mt[variant_qc].n_ctrl_female_called)
				.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
				.default((mt[variant_qc].n_ctrl_male_het + mt[variant_qc].n_ctrl_female_het) / (mt[variant_qc].n_ctrl_male_called + mt[variant_qc].n_ctrl_female_called))),
			avg_ab_case = hl.cond(
				'AD' in gt_codes,
				(hl.case()
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 1), hl.agg.mean(mt.AB)))
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
					.default(hl.agg.filter(mt.pheno[is_case] == 1, hl.agg.mean(mt.AB)))),
				hl.null(hl.tfloat64)
			),
			avg_ab_ctrl = hl.cond(
				'AD' in gt_codes,
				(hl.case()
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 0), hl.agg.mean(mt.AB)))
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
					.default(hl.agg.filter(mt.pheno[is_case] == 0, hl.agg.mean(mt.AB)))),
				hl.null(hl.tfloat64)
			),
			avg_het_ab_case = hl.cond(
				'AD' in gt_codes,
				(hl.case()
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 1), hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
					.default(hl.agg.filter(mt.pheno[is_case] == 1, hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))),
				hl.null(hl.tfloat64)
			),
			avg_het_ab_ctrl = hl.cond(
				'AD' in gt_codes,
				(hl.case()
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (mt.pheno[is_case] == 0), hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
					.default(hl.agg.filter(mt.pheno[is_case] == 0, hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))),
				hl.null(hl.tfloat64)
			),
			avg_alt_gq_case = hl.cond(
				'GQ' in gt_codes,
				hl.agg.filter((mt.pheno[is_case] == 1) & mt.GT.is_non_ref(), hl.agg.mean(mt.GQ)),
				hl.null(hl.tfloat64)
			),
			avg_alt_gq_ctrl = hl.cond(
				'GQ' in gt_codes,
				hl.agg.filter((mt.pheno[is_case] == 0) & mt.GT.is_non_ref(), hl.agg.mean(mt.GQ)),
				hl.null(hl.tfloat64)
			)
		)}
	)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			MAC_case = hl.cond(
				mt[variant_qc].AF_case <= 0.5,
				mt[variant_qc].AC_case,
				2*mt[variant_qc].n_case_called - mt[variant_qc].AC_case
			),
			MAC_ctrl = hl.cond(
				mt[variant_qc].AF_ctrl <= 0.5,
				mt[variant_qc].AC_ctrl,
				2*mt[variant_qc].n_ctrl_called - mt[variant_qc].AC_ctrl
			),
			MAF_case = hl.cond(
				mt[variant_qc].AF_case <= 0.5,
				mt[variant_qc].AF_case,
				1 - mt[variant_qc].AF_case
			),
			MAF_ctrl = hl.cond(
				mt[variant_qc].AF_ctrl <= 0.5,
				mt[variant_qc].AF_ctrl,
				1 - mt[variant_qc].AF_ctrl
			)
		)}
	)

	return mt

def add_diff_miss(mt: hl.MatrixTable, variant_qc: hl.tstr, is_case: hl.tstr = None, diff_miss_min_expected_cell_count: hl.tint32 = 5) -> hl.MatrixTable:

	if 'variant_qc' not in list(mt.row_value):
		mt = mt.annotate_rows(
			variant_qc = hl.struct()
		)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			n_case_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt.pheno[is_case] == 1)),
			n_case_not_called = hl.agg.count_where((~ hl.is_defined(mt.GT)) & (mt.pheno[is_case] == 1)),
			n_ctrl_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt.pheno[is_case] == 0)),
			n_ctrl_not_called = hl.agg.count_where((~ hl.is_defined(mt.GT)) & (mt.pheno[is_case] == 0))
		)}
	)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			diff_miss_row1_sum = hl.int32(mt[variant_qc].n_case_called) + hl.int32(mt[variant_qc].n_ctrl_called),
			diff_miss_row2_sum = hl.int32(mt[variant_qc].n_case_not_called) + hl.int32(mt[variant_qc].n_ctrl_not_called),
			diff_miss_col1_sum = hl.int32(mt[variant_qc].n_case_called) + hl.int32(mt[variant_qc].n_case_not_called),
			diff_miss_col2_sum = hl.int32(mt[variant_qc].n_ctrl_called) + hl.int32(mt[variant_qc].n_ctrl_not_called),
			diff_miss_tbl_sum = hl.int32(mt[variant_qc].n_case_called) + hl.int32(mt[variant_qc].n_ctrl_called) + hl.int32(mt[variant_qc].n_case_not_called) + hl.int32(mt[variant_qc].n_ctrl_not_called)
		)}
	)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			diff_miss_expected_c1 = (hl.int32(mt[variant_qc].diff_miss_row1_sum) * hl.int32(mt[variant_qc].diff_miss_col1_sum)) / hl.int32(mt[variant_qc].diff_miss_tbl_sum),
			diff_miss_expected_c2 = (hl.int32(mt[variant_qc].diff_miss_row1_sum) * hl.int32(mt[variant_qc].diff_miss_col2_sum)) / hl.int32(mt[variant_qc].diff_miss_tbl_sum),
			diff_miss_expected_c3 = (hl.int32(mt[variant_qc].diff_miss_row2_sum) * hl.int32(mt[variant_qc].diff_miss_col1_sum)) / hl.int32(mt[variant_qc].diff_miss_tbl_sum),
			diff_miss_expected_c4 = (hl.int32(mt[variant_qc].diff_miss_row2_sum) * hl.int32(mt[variant_qc].diff_miss_col2_sum)) / hl.int32(mt[variant_qc].diff_miss_tbl_sum),
		)}
	)

	mt = mt.annotate_rows(
		**{variant_qc: mt[variant_qc].annotate(
			diff_miss = hl.cond(
				((hl.int32(mt[variant_qc].n_case_not_called) == 0) & (hl.int32(mt[variant_qc].n_ctrl_not_called) == 0)), 
				hl.struct(p_value = 1.0, odds_ratio = hl.null(hl.tfloat64), ci_95_lower = hl.null(hl.tfloat64), ci_95_upper = hl.null(hl.tfloat64), test = 'NA'),
				hl.cond(
					((hl.int32(mt[variant_qc].diff_miss_expected_c1) < diff_miss_min_expected_cell_count) | (hl.int32(mt[variant_qc].diff_miss_expected_c2) < diff_miss_min_expected_cell_count) | (hl.int32(mt[variant_qc].diff_miss_expected_c3) < diff_miss_min_expected_cell_count) | (hl.int32(mt[variant_qc].diff_miss_expected_c4) < diff_miss_min_expected_cell_count)),
					hl.fisher_exact_test(hl.int32(mt[variant_qc].n_case_called), hl.int32(mt[variant_qc].n_case_not_called), hl.int32(mt[variant_qc].n_ctrl_called), hl.int32(mt[variant_qc].n_ctrl_not_called)).annotate(test = 'fisher_exact'),
					hl.chi_squared_test(hl.int32(mt[variant_qc].n_case_called), hl.int32(mt[variant_qc].n_case_not_called), hl.int32(mt[variant_qc].n_ctrl_called), hl.int32(mt[variant_qc].n_ctrl_not_called)).annotate(ci_95_lower = hl.null(hl.tfloat64), ci_95_upper = hl.null(hl.tfloat64), test = 'chi_squared')
				)
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

def mt_add_col_filters(mt: hl.MatrixTable, filters: hl.tarray, struct_name: hl.tstr) -> hl.MatrixTable:
	mt = mt.annotate_cols(**{struct_name: hl.struct(exclude = 0)})
	for f in filters:
		absent = False
		zero_stddev = False
		all_miss = False
		single_val = False
		for field in f[1].split(","):
			if field not in mt.cols().col_value.flatten():
				absent = True
			dt = eval("ht." + field).dtype
			if dt in [hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64]:
				field_stats = ht.aggregate(hl.agg.stats(eval("ht." + field)))
				if field_stats.stdev == 0:
					zero_stddev = True
				if field_stats.n == 0:
					all_miss = True
			else:
				if len(ht.aggregate(hl.agg.collect_as_set(eval("ht." + field)))):
					single_val = True
			f[2] = f[2].replace(field,"mt." + field)
		if not absent and not zero_stddev and not all_miss and not single_val:
			print("filter samples based on configuration filter " + f[0] + " for field/s " + f[1])
			mt = mt.annotate_cols(
				**{struct_name: mt[struct_name].annotate(
					**{f[0]: hl.cond(eval(hl.eval(f[2])), 0, 1, missing_false = True)}
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
				exclude = hl.cond(
					mt[struct_name][f[0]] == 1,
					1,
					mt[struct_name].exclude
				)
			)}
		)
	return mt

def mt_add_row_filters(mt: hl.MatrixTable, filters: hl.tarray, struct_name: hl.tstr) -> hl.MatrixTable:
	mt = mt.annotate_rows(**{struct_name: hl.struct(exclude = 0)})
	for f in filters:
		absent = False
		zero_stddev = False
		all_miss = False
		single_val = False
		for field in f[1].split(","):
			if field not in mt.rows().row_value.flatten():
				absent = True
			dt = eval("ht." + field).dtype
			if dt in [hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64]:
				field_stats = ht.aggregate(hl.agg.stats(eval("ht." + field)))
				if field_stats.stdev == 0:
					zero_stddev = True
				if field_stats.n == 0:
					all_miss = True
			else:
				if len(ht.aggregate(hl.agg.collect_as_set(eval("ht." + field)))):
					single_val = True
			f[2] = f[2].replace(field,"mt." + field)
		if not absent and not zero_stddev and not all_miss and not single_val:
			print("filter variants based on configuration filter " + f[0] + " for field/s " + f[1])
			mt = mt.annotate_rows(
				**{struct_name: mt[struct_name].annotate(
					**{f[0]: hl.cond(eval(hl.eval(f[2])), 0, 1, missing_false = True)}
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
				exclude = hl.cond(
					mt[struct_name][f[0]] == 1,
					1,
					mt[struct_name].exclude
				)
			)}
		)
	return mt

def ht_add_filters(ht: hl.Table, filters: hl.tarray, struct_name: hl.tstr) -> hl.Table:
	ht = ht.annotate(**{struct_name: hl.struct(exclude = 0)})
	for f in filters:
		absent = False
		zero_stddev = False
		all_miss = False
		single_val = False
		for field in f[1].split(","):
			if field not in ht.row_value.flatten():
				absent = True
			dt = eval("ht." + field).dtype
			if dt in [hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64]:
				field_stats = ht.aggregate(hl.agg.stats(eval("ht." + field)))
				if field_stats.stdev == 0:
					zero_stddev = True
				if field_stats.n == 0:
					all_miss = True
			else:
				print("found values " + str(ht.aggregate(hl.agg.collect_as_set(eval("ht." + field)))) + " in non-numeric field " + field)
				if len(ht.aggregate(hl.agg.collect_as_set(eval("ht." + field)))) == 1:
					single_val = True
			f[2] = f[2].replace(field,"ht." + field)
		if not absent and not zero_stddev and not all_miss and not single_val:
			print("filter table based on configuration filter " + f[0] + " for field/s " + f[1])
			ht = ht.annotate(
				**{struct_name: ht[struct_name].annotate(
					**{f[0]: hl.cond(eval(hl.eval(f[2])), 0, 1, missing_false = True)}
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
				exclude = hl.cond(
					ht[struct_name][f[0]] == 1,
					1,
					ht[struct_name].exclude
				)
			)}
		)
	return ht
