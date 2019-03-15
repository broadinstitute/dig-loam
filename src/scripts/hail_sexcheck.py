import hail as hl
import argparse

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log)
	else:
		hl.init()

	print("reading matrix table")
	mt = hl.read_matrix_table(args.mt_in)
	hl.summarize_variants(mt)

	print("add pheno annotations")
	tbl = hl.import_table(args.pheno_in, delimiter="\t", no_header=False, types={args.sex_col: hl.tstr})
	tbl = tbl.key_by(args.id_col)
	mt = mt.annotate_cols(pheno = tbl[mt.s])

	print("filter variants for QC")
	mt = mt.filter_rows(hl.is_snp(mt.alleles[0], mt.alleles[1]))
	mt = mt.filter_rows(~ hl.is_mnp(mt.alleles[0], mt.alleles[1]))
	mt = mt.filter_rows(~ hl.is_indel(mt.alleles[0], mt.alleles[1]))
	mt = mt.filter_rows(~ hl.is_complex(mt.alleles[0], mt.alleles[1]))
    
	print("impute sex")
	tbl = hl.impute_sex(mt.GT)
	mt = mt.annotate_cols(impute_sex = tbl[mt.s])

	print("annotate samples with sexcheck status")
	mt = mt.annotate_cols(pheno_female = hl.cond(~ hl.is_missing(mt.pheno[args.sex_col]), (mt.pheno[args.sex_col] == 'female') | (mt.pheno[args.sex_col] == 'Female') | (mt.pheno[args.sex_col] == 'f') | (mt.pheno[args.sex_col] == 'F') | (mt.pheno[args.sex_col] == args.female_code), False))
	mt = mt.annotate_cols(pheno_male = hl.cond(~ hl.is_missing(mt.pheno[args.sex_col]), (mt.pheno[args.sex_col] == 'male') | (mt.pheno[args.sex_col] == 'Male') | (mt.pheno[args.sex_col] == 'm') | (mt.pheno[args.sex_col] == 'M') | (mt.pheno[args.sex_col] == args.male_code), False))
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
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--pheno-in', help='a tab delimited phenotype file', required=True)
	requiredArgs.add_argument('--id-col', help='a column name for sample id in the phenotype file', required=True)
	requiredArgs.add_argument('--sex-col', help='a column name for sex in the phenotype file', required=True)
	requiredArgs.add_argument('--male-code', help='a code for male', required=True)
	requiredArgs.add_argument('--female-code', help='a code for female', required=True)
	requiredArgs.add_argument('--sexcheck-out', help='an output filename for sexcheck results', required=True)
	requiredArgs.add_argument('--sexcheck-problems-out', help='an output filename for sexcheck results that were problems', required=True)
	args = parser.parse_args()
	main(args)
