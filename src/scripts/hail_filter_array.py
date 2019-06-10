import hail as hl
import argparse

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log)
	else:
		hl.init()

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)
	hl.summarize_variants(mt)

	#print("add ancestry annotation")
	#tbl = hl.import_table(args.ancestry_in, delimiter="\t", no_header=True)
	#tbl = tbl.annotate(IID = tbl.f0)
	#tbl = tbl.key_by('IID')
	#mt = mt.annotate_cols(GROUP = tbl[mt.s].f1)

	print("add imputed sex annotation")
	tbl = hl.import_table(args.sexcheck_in, no_header=False, types={'is_female': hl.tbool})
	tbl = tbl.key_by('IID')
	mt = mt.annotate_cols(is_female = tbl[mt.s].is_female)

	print("remove samples that failed QC")
	tbl = hl.import_table(args.samples_remove, no_header=True).key_by('f0')
	mt = mt.filter_cols(hl.is_defined(tbl[mt.s]), keep=False)

	#remove the previous filter_cols() command and filter only for the calculation of the metrics. annotate_cols() with sampleqc failure indicator
	#from paper: After	exclusion	of	samples,	we	calculate	an	additional	set	of	variant	metrics	and	
	# excluded	any	variant	with	overall	call	rate	<0.3,	heterozygosity	of	1,	or	heterozygote allele	balance	of	0	or	1
	#read in array level filters from the user
	#  be sure to allow for variant metric filters, sample metric filters, and allow lists to be uploaded as well
	#generate a failed samples by filters table and and write it to file, indicating which samples should be removed, adding in the samples that were removed from sample qc process
	#generate a failed variants by filters table and and write it to file, indicating which variants should be removed
	#change name of this file to hail_filter_array.py

	#create a separate hail_filter_cohort.py that will be run for each cohort
	#read in cohort level filters from the user and read in hail_filter_array.py tables
	#calculate metrics within the cohort
	#generate a failed samples by filters table, including any calculated metrics that were used in filtering, and adding in the array level filters
	#also generate a failed variants by filters table, including any calculated metrics that were used in filtering, and adding in the array level filters

	#?samples_df = mt.cols().to_pandas()
	#?samples_df['is_case'] = samples_df['is_case'].astype('bool')
	#?samples_df['is_female'] = samples_df['is_female'].astype('bool')
	#?group_counts = samples_df['GROUP'][~samples_df['is_case']].value_counts().to_dict()
	#?mt = mt.annotate_rows(failed = 0)

	print("calculate variant qc stats")
	mt = hl.variant_qc(mt, name='variant_qc')

	print("count males and females")
	tbl = mt.cols()
	nMales = tbl.aggregate(hl.agg.count_where(~ tbl.is_female))
	nFemales = tbl.aggregate(hl.agg.count_where(tbl.is_female))

	print("count male/female hets, homvars and called")
	mt = mt.annotate_rows(
		n_male_het = hl.agg.count_where(~ mt.is_female & mt.GT.is_het()),
		n_male_hom_var = hl.agg.count_where(~ mt.is_female & mt.GT.is_hom_var()),
		n_male_called = hl.agg.count_where(~ mt.is_female & hl.is_defined(mt.GT)),
		n_female_het = hl.agg.count_where(mt.is_female & mt.GT.is_het()),
		n_female_hom_var = hl.agg.count_where(mt.is_female & mt.GT.is_hom_var()),
		n_female_called = hl.agg.count_where(mt.is_female & hl.is_defined(mt.GT)))

	print("calculate callRate, AC, and AF (accounting appropriately for sex chromosomes)")
	mt = mt.annotate_rows(
		call_rate = hl.cond(mt.locus.in_y_nonpar(), mt.n_male_called / nMales, hl.cond(mt.locus.in_x_nonpar(), (mt.n_male_called + 2*mt.n_female_called) / (nMales + 2*nFemales), (mt.n_male_called + mt.n_female_called) / (nMales + nFemales))),
		AC = hl.cond(mt.locus.in_y_nonpar(),  mt.n_male_hom_var, hl.cond(mt.locus.in_x_nonpar(), mt.n_male_hom_var + mt.n_female_het + 2*mt.n_female_hom_var, mt.n_male_het + 2*mt.n_male_hom_var + mt.n_female_het + 2*mt.n_female_hom_var)),
		AF = hl.cond(mt.locus.in_y_nonpar(), mt.n_male_hom_var / mt.n_male_called, hl.cond(mt.locus.in_x_nonpar(), (mt.n_male_hom_var + mt.n_female_het + 2*mt.n_female_hom_var) / (mt.n_male_called + 2*mt.n_female_called), (mt.n_male_het + 2*mt.n_male_hom_var + mt.n_female_het + 2*mt.n_female_hom_var) / (2*mt.n_male_called + 2*mt.n_female_called))))




	if 'AD' in list(mt.entry):
		print("add allele balance to entries")
		mt = mt.annotate_entries(
			AB = hl.cond(hl.is_defined(mt.AD), hl.cond(hl.sum(mt.AD) > 0, mt.AD[1] / hl.sum(mt.AD), hl.null(hl.tfloat64)) , hl.null(hl.tfloat64)),
			AB_dist50 = hl.cond(hl.is_defined(mt.AD), hl.cond(hl.sum(mt.AD) > 0, hl.abs((mt.AD[1] / hl.sum(mt.AD)) - 0.5), hl.null(hl.tfloat64)), hl.null(hl.tfloat64))
		)

	print("annotate sample qc stats")
	mt = mt.annotate_cols(sample_qc = mt.sample_qc.annotate(
		n_het_low = hl.agg.count_where((mt.variant_qc.AF[1] < 0.03) & mt.GT.is_het()), 
		n_het_high = hl.agg.count_where((mt.variant_qc.AF[1] >= 0.03) & mt.GT.is_het()), 
		n_called_low = hl.agg.count_where((mt.variant_qc.AF[1] < 0.03) & ~hl.is_missing(mt.GT)), 
		n_called_high = hl.agg.count_where((mt.variant_qc.AF[1] >= 0.03) & ~hl.is_missing(mt.GT)),
		avg_ab = hl.agg.mean(mt.AB),
		avg_ab_dist50 = hl.agg.mean(mt.AB_dist50))
	)



	##### add HET and AB filters...
	##### filter out overall call rate < 0.3, heterozygosity of 1, or heterozygote allele balance of 0 or 1
	print("filter variants for callrate, heterozygosity, and allele balance in the case of sequence data")
	mt = mt.annotate_rows(failed = hl.cond(mt.call_rate < 0.98, 1, mt.failed))





	groups_used = []
	for group in group_counts:
		if group_counts[group] > 100 and group_counts[group] != 'AMR':
			groups_used.extend([group])
			print("filter autosomal variants with pHWE <= 1e-6 in " + group + " male and female controls")
			mt = mt.annotate_rows(**{
				'p_hwe_ctrl_' + group: hl.cond(mt.locus.in_x_nonpar(), hl.agg.filter(mt.is_female & ~ mt.is_case & (mt.GROUP == group), hl.agg.hardy_weinberg_test(mt.GT)), hl.cond(mt.locus.in_y_par() | mt.locus.in_y_nonpar(), hl.agg.filter(~ mt.is_female & ~ mt.is_case & (mt.GROUP == group), hl.agg.hardy_weinberg_test(mt.GT)), hl.agg.filter(~ mt.is_case & (mt.GROUP == group), hl.agg.hardy_weinberg_test(mt.GT))))})
			mt = mt.annotate_rows(failed = hl.cond(((mt.AF >= 0.01) & (mt.AF <= 0.9)) & (mt['p_hwe_ctrl_' + group].p_value <= 1e-6), 1, mt.failed))

	print("write variant qc results to file")
	mt = mt.annotate_rows(id = mt.locus.contig + ':' + hl.str(mt.locus.position) + ':' + hl.str(mt.alleles[0]) + ':' + hl.str(mt.alleles[1]))
	mt.rows().flatten().export(args.variantqc_out, types_file=None)

	print("write variant exclusions to file")
	tbl = mt.rows().flatten()
	tbl = tbl.filter(tbl.failed == 1, keep=True)
	tbl.export(args.variants_exclude_out, types_file=None)

	print("filter failed variants out of mt")
	mt = mt.filter_rows(mt.failed == 1, keep=False)

	print("write mt to disk")
	mt.write(args.mt_out, overwrite=True)

	print("write VCF file to disk")
	hl.export_vcf(mt, args.vcf_out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail mt dataset name', required=True)
	requiredArgs.add_argument('--ancestry-in', help='an inferred ancestry file', required=True)
	requiredArgs.add_argument('--sexcheck-in', help='an imputed sexcheck output file from Hail', required=True)
	requiredArgs.add_argument('--sample-in', help='a sample file', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--case-ctrl-col', help='column name for case/control status in phenotype file', required=True)
	requiredArgs.add_argument('--samples-remove', help='a file containing sample IDs that failed QC', required=True)
	requiredArgs.add_argument('--variantqc-out', help='a base filename for variantqc', required=True)
	requiredArgs.add_argument('--variants-exclude-out', help='a base filename for failed variants', required=True)
	requiredArgs.add_argument('--vcf-out', help='a vcf file name for output', required=True)
	requiredArgs.add_argument('--mt-out', help='a mt directory name for output', required=True)
	args = parser.parse_args()
	main(args)
