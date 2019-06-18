import hail as hl
import argparse

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log)
	else:
		hl.init()

	print("reading matrix table")
	mt = hl.read_matrix_table(args.mt_in)

	print("filter to only non-vcf-filtered, well-called, non-monomorphic variants for sexcheck")
	mt = mt.filter_rows(
		(hl.len(mt.filters) == 0) & 
		(mt.variant_qc_raw.AN > 1) & 
		(mt.variant_qc_raw.AF[1] > 0) & (mt.variant_qc_raw.AF[1] < 1) & 
		(hl.is_snp(mt.alleles[0], mt.alleles[1])) & 
		(~ hl.is_mnp(mt.alleles[0], mt.alleles[1])) & 
		(~ hl.is_indel(mt.alleles[0], mt.alleles[1])) & 
		(~ hl.is_complex(mt.alleles[0], mt.alleles[1])),
		keep=True
	)

	print("add pheno annotations")
	tbl = hl.import_table(args.sample_in, delimiter="\t", no_header=False, types={args.sex_col: hl.tstr})
	tbl = tbl.key_by(args.id_col)
	mt = mt.annotate_cols(pheno = tbl[mt.s])

	print("annotate samples with pheno male and female indicators")
	mt = mt.annotate_cols(pheno_female = hl.cond(~ hl.is_missing(mt.pheno[args.sex_col]), (mt.pheno[args.sex_col] == 'female') | (mt.pheno[args.sex_col] == 'Female') | (mt.pheno[args.sex_col] == 'f') | (mt.pheno[args.sex_col] == 'F') | (mt.pheno[args.sex_col] == args.female_code), False))
	mt = mt.annotate_cols(pheno_male = hl.cond(~ hl.is_missing(mt.pheno[args.sex_col]), (mt.pheno[args.sex_col] == 'male') | (mt.pheno[args.sex_col] == 'Male') | (mt.pheno[args.sex_col] == 'm') | (mt.pheno[args.sex_col] == 'M') | (mt.pheno[args.sex_col] == args.male_code), False))

	if hl.filter_intervals(mt, [hl.parse_locus_interval(x) for x in hl.get_reference(args.reference_genome).x_contigs], keep=True).count()[0] > 0:
		print("impute sex")
		tbl = hl.impute_sex(mt.GT)
	else:
		print("skipping impute_sex due to missing X chromosome data")
		tbl = mt.cols().select()
		tbl = tbl.annotate(is_female = hl.null(hl.tbool), f_stat = hl.null(hl.tfloat64), n_called = hl.null(hl.tint64),	expected_homs = hl.null(hl.tfloat64), observed_homs = hl.null(hl.tint64))

	mt = mt.annotate_cols(impute_sex = tbl[mt.s])

	print("annotate samples with sexcheck status")
	mt = mt.annotate_cols(sexcheck = hl.cond(~ hl.is_missing(mt.pheno[args.sex_col]) & ~ hl.is_missing(mt.impute_sex.is_female), hl.cond((mt.pheno_female & mt.impute_sex.is_female) | (mt.pheno_male & ~ mt.impute_sex.is_female), "OK", "PROBLEM"), "OK"))
    
	print("replace is_female annotation with self report if imputed sex failed")
	mt = mt.annotate_cols(is_female = hl.cond(mt.pheno_female & hl.is_missing(mt.impute_sex.is_female), True, hl.cond(mt.pheno_male & hl.is_missing(mt.impute_sex.is_female), False, mt.impute_sex.is_female)))
    
	print("write sexcheck results to file")
	tbl = mt.cols()
	tbl = tbl.rename({'s': 'IID'})
	tbl_out = tbl.select(pheno_sex = tbl.pheno[args.sex_col], sexcheck = tbl.sexcheck, is_female = tbl.is_female, f_stat = tbl.impute_sex.f_stat, n_called = tbl.impute_sex.n_called, expected_homs = tbl.impute_sex.expected_homs, observed_homs = tbl.impute_sex.observed_homs)
	tbl_out.export(args.sexcheck_out)
    
	print("write sexcheck problems to file")
	tbl_out = tbl_out.filter(tbl_out.sexcheck == "PROBLEM", keep=True)
	tbl_out.flatten().export(args.sexcheck_problems_out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--sample-in', help='a tab delimited sample file', required=True)
	requiredArgs.add_argument('--id-col', help='a column name for sample id in the sample file', required=True)
	requiredArgs.add_argument('--sex-col', help='a column name for sex in the sample file', required=True)
	requiredArgs.add_argument('--male-code', help='a code for male', required=True)
	requiredArgs.add_argument('--female-code', help='a code for female', required=True)
	requiredArgs.add_argument('--sexcheck-out', help='an output filename for sexcheck results', required=True)
	requiredArgs.add_argument('--sexcheck-problems-out', help='an output filename for sexcheck results that were problems', required=True)
	args = parser.parse_args()
	main(args)
