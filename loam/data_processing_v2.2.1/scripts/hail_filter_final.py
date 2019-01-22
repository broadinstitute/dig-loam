import hail as hl
import argparse
hl.init()

def main(args=None):

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)
	hl.summarize_variants(mt)

	print("add ancestry annotation")
	tbl = hl.import_table(args.ancestry_in, delimiter="\t", no_header=True)
	tbl = tbl.annotate(IID = tbl.f0)
	tbl = tbl.key_by('IID')
	mt = mt.annotate_cols(GROUP = tbl[mt.s].f1)

	print("add imputed sex annotation")
	tbl = hl.import_table(args.sexcheck_in, no_header=False, types={'is_female': hl.tbool})
	tbl = tbl.key_by('IID')
	mt = mt.annotate_cols(is_female = tbl[mt.s].is_female)

	print("add case/control status annotation")
	tbl = hl.import_table(args.pheno_in, no_header=False, types={args.case_ctrl_col: hl.tint})
	tbl = tbl.key_by(args.iid_col)
	mt = mt.annotate_cols(is_case = tbl[mt.s][args.case_ctrl_col] == 1)
	mt.describe()

	print("calculate pre sampleqc genotype call rate")
	pre_sampleqc_callrate = mt.aggregate_entries(hl.agg.fraction(hl.is_defined(mt.GT)))
	print('pre sampleqc call rate is %.3f' % pre_sampleqc_callrate)

	print("remove samples that failed QC")
	tbl = hl.import_table(args.samples_remove, no_header=True).key_by('f0')
	mt = mt.filter_cols(hl.is_defined(tbl[mt.s]), keep=False)

	print("calculate post sampleqc genotype call rate")
	post_sampleqc_callrate = mt.aggregate_entries(hl.agg.fraction(hl.is_defined(mt.GT)))
	print('post sampleqc call rate is %.3f' % post_sampleqc_callrate)

	samples_df = mt.cols().to_pandas()
	samples_df['is_case'] = samples_df['is_case'].astype('bool')
	samples_df['is_female'] = samples_df['is_female'].astype('bool')
	group_counts = samples_df['GROUP'][~samples_df['is_case']].value_counts().to_dict()

	mt = mt.annotate_rows(failed = 0)
    
	print("count males and females")
	nMales = samples_df[~samples_df['is_female']].shape[0]
	nFemales = samples_df[samples_df['is_female']].shape[0]

	print("count male/female hets, homvars and called")
	mt = mt.annotate_rows(
		n_male_het = hl.agg.count_where(~ mt.is_female & mt.GT.is_het()),
		n_male_hom_var = hl.agg.count_where(~ mt.is_female & mt.GT.is_hom_var()),
		n_male_called = hl.agg.count_where(~ mt.is_female & hl.is_defined(mt.GT)),
		n_female_het = hl.agg.count_where(mt.is_female & mt.GT.is_het()),
		n_female_hom_var = hl.agg.count_where(mt.is_female & mt.GT.is_hom_var()),
		n_female_called = hl.agg.count_where(mt.is_female & hl.is_defined(mt.GT)))

	print("calculate callRate, MAC, and MAF (accounting appropriately for sex chromosomes)")
	mt = mt.annotate_rows(
		call_rate = hl.cond(mt.locus.in_y_nonpar(), mt.n_male_called / nMales, hl.cond(mt.locus.in_x_nonpar(), (mt.n_male_called + 2*mt.n_female_called) / (nMales + 2*nFemales), (mt.n_male_called + mt.n_female_called) / (nMales + nFemales))),
		mac = hl.cond(mt.locus.in_y_nonpar(),  mt.n_male_hom_var, hl.cond(mt.locus.in_x_nonpar(), mt.n_male_hom_var + mt.n_female_het + 2*mt.n_female_hom_var, mt.n_male_het + 2*mt.n_male_hom_var + mt.n_female_het + 2*mt.n_female_hom_var)),
		maf = hl.cond(mt.locus.in_y_nonpar(), mt.n_male_hom_var / mt.n_male_called, hl.cond(mt.locus.in_x_nonpar(), (mt.n_male_hom_var + mt.n_female_het + 2*mt.n_female_hom_var) / (mt.n_male_called + 2*mt.n_female_called), (mt.n_male_het + 2*mt.n_male_hom_var + mt.n_female_het + 2*mt.n_female_hom_var) / (2*mt.n_male_called + 2*mt.n_female_called))))

	print("filter variants for callrate")
	mt = mt.annotate_rows(failed = hl.cond(mt.call_rate < 0.98, 1, mt.failed))

	groups_used = []
	for group in group_counts:
		if group_counts[group] > 100 and group_counts[group] != 'AMR':
			groups_used.extend([group])
			print("filter autosomal variants with pHWE <= 1e-6 in " + group + " male and female controls")
			mt = mt.annotate_rows(**{
				'p_hwe_ctrl_' + group: hl.cond(mt.locus.in_x_nonpar(), hl.agg.filter(mt.is_female & ~ mt.is_case & (mt.GROUP == group), hl.agg.hardy_weinberg_test(mt.GT)), hl.cond(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.agg.filter(~ mt.is_female & ~ mt.is_case & (mt.GROUP == group), hl.agg.hardy_weinberg_test(mt.GT)), hl.agg.filter(~ mt.is_case & (mt.GROUP == group), hl.agg.hardy_weinberg_test(mt.GT))))})
			mt = mt.annotate_rows(failed = hl.cond((mt.maf >= 0.01) & (mt['p_hwe_ctrl_' + group].p_value <= 1e-6), 1, mt.failed))

	print("write variant qc results to file")
	mt = mt.annotate_rows(id = mt.locus.contig + ':' + hl.str(mt.locus.position) + ':' + hl.str(mt.alleles[0]) + ':' + hl.str(mt.alleles[1]))
	mt.rows().flatten().export(args.variantqc_out)

	print("write variant exclusions to file")
	tbl = mt.rows().flatten()
	tbl = tbl.filter(tbl.failed == 1, keep=True)
	tbl.export(args.variants_exclude_out)

	print("filter failed variants out of mt")
	mt = mt.filter_rows(mt.failed == 1, keep=False)

	print("write mt to disk")
	mt.write(args.mt_out, overwrite=True)

	print("write VCF file to disk")
	hl.export_vcf(mt, args.vcf_out)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--mt-in', help='a hail mt dataset name', required=True)
	requiredArgs.add_argument('--ancestry-in', help='an inferred ancestry file', required=True)
	requiredArgs.add_argument('--sexcheck-in', help='an imputed sexcheck output file from Hail', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--case-ctrl-col', help='column name for case/control status in phenotype file', required=True)
	requiredArgs.add_argument('--samples-remove', help='a file containing sample IDs that failed QC', required=True)
	requiredArgs.add_argument('--variantqc-out', help='a base filename for variantqc', required=True)
	requiredArgs.add_argument('--variants-exclude-out', help='a base filename for failed variants', required=True)
	requiredArgs.add_argument('--vcf-out', help='a vcf file name for output', required=True)
	requiredArgs.add_argument('--mt-out', help='a mt directory name for output', required=True)
	args = parser.parse_args()
	main(args)
