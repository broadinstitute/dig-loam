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

def update_variant_qc(mt: hl.MatrixTable, is_female: hl.tstr, variant_qc: hl.tstr, is_case: hl.tstr = None) -> hl.MatrixTable:

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

	if is_case is not None:

		num_case_males = mt.aggregate_cols(hl.agg.count_where((~ mt[is_female]) & (mt[is_case])))
		num_case_females = mt.aggregate_cols(hl.agg.count_where((mt[is_female]) & (mt[is_case])))
		num_ctrl_males = mt.aggregate_cols(hl.agg.count_where((~ mt[is_female]) & (~ mt[is_case])))
		num_ctrl_females = mt.aggregate_cols(hl.agg.count_where((mt[is_female]) & (~ mt[is_case])))

		mt = mt.annotate_rows(
			**{variant_qc: mt[variant_qc].annotate(
				n_case_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt[is_case])),
				n_case_male_het = hl.agg.count_where(mt.GT.is_het() & (~ mt[is_female]) & (mt[is_case])),
				n_case_male_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (~ mt[is_female]) & (mt[is_case])),
				n_case_male_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (~ mt[is_female]) & (mt[is_case])),
				n_case_male_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (~ mt[is_female]) & (mt[is_case])),
				n_case_male_called = hl.agg.count_where(hl.is_defined(mt.GT) & (~ mt[is_female]) & (mt[is_case])),
				n_case_female_het = hl.agg.count_where(mt.GT.is_het() & (mt[is_female]) & (mt[is_case])),
				n_case_female_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (mt[is_female]) & (mt[is_case])),
				n_case_female_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (mt[is_female]) & (mt[is_case])),
				n_case_female_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (mt[is_female]) & (mt[is_case])),
				n_case_female_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt[is_female]) & (mt[is_case])),
				n_ctrl_called = hl.agg.count_where(hl.is_defined(mt.GT) & (~ mt[is_case])),
				n_ctrl_male_het = hl.agg.count_where(mt.GT.is_het() & (~ mt[is_female]) & (~ mt[is_case])),
				n_ctrl_male_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (~ mt[is_female]) & (~ mt[is_case])),
				n_ctrl_male_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (~ mt[is_female]) & (~ mt[is_case])),
				n_ctrl_male_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (~ mt[is_female]) & (~ mt[is_case])),
				n_ctrl_male_called = hl.agg.count_where(hl.is_defined(mt.GT) & (~ mt[is_female]) & (~ mt[is_case])),
				n_ctrl_female_het = hl.agg.count_where(mt.GT.is_het() & (mt[is_female]) & (~ mt[is_case])),
				n_ctrl_female_het_ref = hl.agg.count_where(mt.GT.is_het_ref() & (mt[is_female]) & (~ mt[is_case])),
				n_ctrl_female_hom_var = hl.agg.count_where(mt.GT.is_hom_var() & (mt[is_female]) & (~ mt[is_case])),
				n_ctrl_female_hom_ref = hl.agg.count_where(mt.GT.is_hom_ref() & (mt[is_female]) & (~ mt[is_case])),
				n_ctrl_female_called = hl.agg.count_where(hl.is_defined(mt.GT) & (mt[is_female]) & (~ mt[is_case]))
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
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & mt[is_case], hl.agg.hardy_weinberg_test(mt.GT)).het_freq_hwe)
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
					.default(hl.agg.filter(mt[is_case], hl.agg.hardy_weinberg_test(mt.GT).het_freq_hwe))),
				het_freq_hwe_ctrl = (hl.case()
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (~ mt[is_case]), hl.agg.hardy_weinberg_test(mt.GT)).het_freq_hwe)
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
					.default(hl.agg.filter(~ mt[is_case], hl.agg.hardy_weinberg_test(mt.GT).het_freq_hwe))),
				p_value_hwe_case = (hl.case()
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & mt[is_case], hl.agg.hardy_weinberg_test(mt.GT)).p_value)
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
					.default(hl.agg.filter(mt[is_case], hl.agg.hardy_weinberg_test(mt.GT).p_value))),
				p_value_hwe_ctrl = (hl.case()
					.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (~ mt[is_case]), hl.agg.hardy_weinberg_test(mt.GT)).p_value)
					.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
					.default(hl.agg.filter(~ mt[is_case], hl.agg.hardy_weinberg_test(mt.GT).p_value))),
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
						.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & mt[is_case], hl.agg.mean(mt.AB)))
						.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
						.default(hl.agg.filter(mt[is_case], hl.agg.mean(mt.AB)))),
					hl.null(hl.tfloat64)
				),
				avg_ab_ctrl = hl.cond(
					'AD' in gt_codes,
					(hl.case()
						.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (~ mt[is_case]), hl.agg.mean(mt.AB)))
						.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
						.default(hl.agg.filter(~ mt[is_case], hl.agg.mean(mt.AB)))),
					hl.null(hl.tfloat64)
				),
				avg_het_ab_case = hl.cond(
					'AD' in gt_codes,
					(hl.case()
						.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & mt[is_case], hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))
						.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
						.default(hl.agg.filter(mt[is_case], hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))),
					hl.null(hl.tfloat64)
				),
				avg_het_ab_ctrl = hl.cond(
					'AD' in gt_codes,
					(hl.case()
						.when(mt.locus.in_x_nonpar(), hl.agg.filter(mt[is_female] & (~ mt[is_case]), hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))
						.when(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.null(hl.tfloat64))
						.default(hl.agg.filter(~ mt[is_case], hl.agg.filter(mt.GT.is_het(), hl.agg.mean(mt.AB))))),
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
				),
				diff_miss_p_value = fisher_exact_test(mt[variant_qc].AC_case, mt[variant_qc].AC_ref_case, mt[variant_qc].AC_ctrl, mt[variant_qc].AC_ref_ctrl).p_value,
				diff_miss_odds_ratio = fisher_exact_test(mt[variant_qc].AC_case, mt[variant_qc].AC_ref_case, mt[variant_qc].AC_ctrl, mt[variant_qc].AC_ref_ctrl).odds_ratio,
				diff_miss_ci_95_lower = fisher_exact_test(mt[variant_qc].AC_case, mt[variant_qc].AC_ref_case, mt[variant_qc].AC_ctrl, mt[variant_qc].AC_ref_ctrl).ci_95_lower,
				diff_miss_ci_95_upper = fisher_exact_test(mt[variant_qc].AC_case, mt[variant_qc].AC_ref_case, mt[variant_qc].AC_ctrl, mt[variant_qc].AC_ref_ctrl).ci_95_upper
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
