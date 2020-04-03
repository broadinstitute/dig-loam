import hail as hl
import argparse
import pandas as pd

def main(args=None):

	if args.hail_utils:
		import importlib.util
		with hl.hadoop_open(args.hail_utils, 'r') as f:
			script = f.read()
		with open("hail_utils.py", 'w') as f:
			f.write(script)
		spec = importlib.util.spec_from_file_location('hail_utils', 'hail_utils.py')
		hail_utils = importlib.util.module_from_spec(spec)   
		spec.loader.exec_module(hail_utils)
	else:
		import hail_utils

	if not args.cloud:
		hl.init(log = args.log, idempotent=True)
	else:
		hl.init(idempotent=True)

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)

	if args.extract:
		print("extract variants not yet implemented!")
		return 1

	if args.extract_ld:
		print("extract hi ld known variants")
		ld_vars = []
		try:
			tbl = hl.import_table(
				args.extract_ld,
				no_header=False,
				types={'SNP_A': hl.tstr, 'SNP_B': hl.tstr, 'R2': hl.tfloat, 'CLOSEST_GENE': hl.tstr}
			)
		except:
			print("skipping hi ld known variants extraction due to empty file")
		else:
			tbl1 = tbl.key_by('SNP_A')
			tbl2 = tbl.key_by('SNP_B')
			mt = mt.annotate_rows(in_hild = hl.cond((hl.is_defined(tbl1[mt.rsid])) | (hl.is_defined(tbl2[mt.rsid])), True, False))
			mt = mt.filter_rows(mt.in_hild, keep=True)

	print("annotate samples with phenotype file")
	tbl = hl.import_table(
		args.pheno_in,
		no_header=False,
		missing="NA",
		impute=True,
		types={args.iid_col: hl.tstr}
	)
	tbl = tbl.key_by(args.iid_col)
	mt = mt.annotate_cols(pheno = tbl[mt.s])

	if args.ancestry_in:
		print("add ancestry annotation")
		tbl = hl.import_table(
			args.ancestry_in,
			delimiter="\t",
			no_header=True,
			types={'f0': hl.tstr, 'f1': hl.tstr}
		)
		tbl = tbl.rename({'f0': 'IID', 'f1': 'ANCESTRY_INFERRED'})
		tbl = tbl.key_by('IID')
		mt = mt.annotate_cols(ANCESTRY_INFERRED = tbl[mt.s].ANCESTRY_INFERRED)

	print("reduce to samples with non-missing phenotype")
	mt = mt.filter_cols(hl.is_missing(mt.pheno[args.pheno_col]), keep=False)

	if args.pops:
		print("reduce to samples in populations " + args.pops)
		pops = args.pops.split(",")
		mt = mt.filter_cols(mt.ANCESTRY_INFERRED in pops, keep=True)

	print("read in list of PCs to include in test")
	with hl.hadoop_open(args.pcs_include, "r") as f:
		pcs = f.read().splitlines()

	covars = [x for x in args.covars.split("+")] if args.covars != "" else []
	if args.trans == "invn":
		pheno_analyzed = args.pheno_col + '_invn_' + "_".join([x.replace("[","").replace("]","") for x in args.covars.split("+")])
		covars = pcs
	else:
		pheno_analyzed = args.pheno_col
		pheno_df = mt.cols().to_pandas()
		for cov in covars:
			if cov[0] == "[" and cov[-1] == "]":
				for val in sorted(pheno_df['pheno.' + cov[1:-1]].unique())[1:]:
					covars = covars + [cov[1:-1] + str(val)]
				covars = [x for x in covars if x != cov]
		covars = covars + pcs

	n_all = mt.rows().count()
	print(str(n_all) + " variants remaining for analysis")

	print("calculate global variant qc")
	mt = hl.variant_qc(mt, name="variant_qc")
	mt = hail_utils.update_variant_qc(mt, is_female = "is_female", variant_qc = "variant_qc")
	if args.test != 'hail.q.lm':
		print("add case/ctrl variant qc")
		mt = hail_utils.add_case_ctrl_stats_results(mt, is_female = "is_female", variant_qc = "variant_qc", is_case = pheno_analyzed)

	print("generate Y and non-Y chromosome sets (to account for male only Y chromosome)")
	mt_nony = hl.filter_intervals(mt, [hl.parse_locus_interval(str(x)) for x in range(1,23)] + [hl.parse_locus_interval(x) for x in ['X','MT']], keep=True)
	mt_y = hl.filter_intervals(mt, [hl.parse_locus_interval('Y')], keep=True)
	mt_y = mt_y.filter_cols(mt_y.is_female, keep=False)

	def linear_regression(mt):
		tbl = hl.linear_regression_rows(
			y = mt.pheno[pheno_analyzed],
			x = mt.GT.n_alt_alleles(),
			covariates = [1] + [mt.pheno[x] for x in covars],
			pass_through = [
				mt.rsid,
				mt.variant_qc.n_called,
				mt.variant_qc.n_male_called,
				mt.variant_qc.n_female_called,
				mt.variant_qc.call_rate,
				mt.variant_qc.AC,
				mt.variant_qc.AF,
				mt.variant_qc.MAC,
				mt.variant_qc.MAF
			]
		)
		tbl = tbl.select(
			chr = tbl.locus.contig,
			pos = tbl.locus.position,
			id = tbl.rsid,
			ref = tbl.alleles[0],
			alt = tbl.alleles[1],
			n = tbl.n_called,
			male = tbl.n_male_called,
			female = tbl.n_female_called,
			call_rate = tbl.call_rate,
			ac = tbl.AC,
			af = tbl.AF,
			mac = tbl.MAC,
			maf = tbl.MAF,
			sum_x = tbl.sum_x,
			y_transpose_x = tbl.y_transpose_x,
			beta = tbl.beta,
			se = tbl.standard_error,
			t_stat = tbl.t_stat,
			pval = tbl.p_value
		)
		return tbl

	def logistic_regression(mt, test):
		tbl = hl.logistic_regression_rows(
			test = test.replace("hail.b.",""),
			y = mt.pheno[pheno_analyzed],
			x = mt.GT.n_alt_alleles(),
			covariates = [1] + [mt.pheno[x] for x in covars],
			pass_through = [
				mt.rsid,
				mt.variant_qc.n_called,
				mt.variant_qc.n_male_called,
				mt.variant_qc.n_female_called,
				mt.variant_qc.n_case_called,
				mt.variant_qc.n_ctrl_called,
				mt.variant_qc.call_rate,
				mt.variant_qc.AC,
				mt.variant_qc.AF,
				mt.variant_qc.AF_case,
				mt.variant_qc.AF_ctrl,
				mt.variant_qc.MAC,
				mt.variant_qc.MAF
			]
		)

		if test == 'hail.b.wald':
			tbl = tbl.select(
				chr = tbl.locus.contig,
				pos = tbl.locus.position,
				id = tbl.rsid,
				ref = tbl.alleles[0],
				alt = tbl.alleles[1],
				n = tbl.n_called,
				male = tbl.n_male_called,
				female = tbl.n_female_called,
				case = tbl.n_case_called,
				ctrl = tbl.n_ctrl_called,
				call_rate = tbl.call_rate,
				ac = tbl.AC,
				af = tbl.AF,
				af_case = tbl.AF_case,
				af_ctrl = tbl.AF_ctrl,
				mac = tbl.MAC,
				maf = tbl.MAF,
				beta = tbl.beta,
				se = tbl.standard_error,
				z_stat = tbl.z_stat,
				pval = tbl.p_value,
				n_iter = tbl.fit.n_iterations,
				converged = tbl.fit.converged,
				exploded = tbl.fit.exploded
			)
		elif test == 'hail.b.firth':
			tbl = tbl.select(
				chr = tbl.locus.contig,
				pos = tbl.locus.position,
				id = tbl.rsid,
				ref = tbl.alleles[0],
				alt = tbl.alleles[1],
				n = tbl.n_called,
				male = tbl.n_male_called,
				female = tbl.n_female_called,
				case = tbl.n_case_called,
				ctrl = tbl.n_ctrl_called,
				call_rate = tbl.call_rate,
				ac = tbl.AC,
				af = tbl.AF,
				af_case = tbl.AF_case,
				af_ctrl = tbl.AF_ctrl,
				mac = tbl.MAC,
				maf = tbl.MAF,
				beta = tbl.beta,
				chi_sq_stat = tbl.chi_sq_stat,
				pval = tbl.p_value,
				n_iter = tbl.fit.n_iterations,
				converged = tbl.fit.converged,
				exploded = tbl.fit.exploded
			)
		elif test == 'hail.b.lrt':
			tbl = tbl.select(
				chr = tbl.locus.contig,
				pos = tbl.locus.position,
				id = tbl.rsid,
				ref = tbl.alleles[0],
				alt = tbl.alleles[1],
				n = tbl.n_called,
				male = tbl.n_male_called,
				female = tbl.n_female_called,
				case = tbl.n_case_called,
				ctrl = tbl.n_ctrl_called,
				call_rate = tbl.call_rate,
				ac = tbl.AC,
				af = tbl.AF,
				af_case = tbl.AF_case,
				af_ctrl = tbl.AF_ctrl,
				mac = tbl.MAC,
				maf = tbl.MAF,
				beta = tbl.beta,
				chi_sq_stat = tbl.chi_sq_stat,
				pval = tbl.p_value,
				n_iter = tbl.fit.n_iterations,
				converged = tbl.fit.converged,
				exploded = tbl.fit.exploded
			)
		elif test == 'hail.b.score':
			tbl = tbl.select(
				chr = tbl.locus.contig,
				pos = tbl.locus.position,
				id = tbl.rsid,
				ref = tbl.alleles[0],
				alt = tbl.alleles[1],
				n = tbl.n_called,
				male = tbl.n_male_called,
				female = tbl.n_female_called,
				case = tbl.n_case_called,
				ctrl = tbl.n_ctrl_called,
				call_rate = tbl.call_rate,
				ac = tbl.AC,
				af = tbl.AF,
				af_case = tbl.AF_case,
				af_ctrl = tbl.AF_ctrl,
				mac = tbl.MAC,
				maf = tbl.MAF,
				chi_sq_stat = tbl.chi_sq_stat,
				pval = tbl.p_value
			)
		return tbl

	if args.test == 'hail.q.lm':
		mt_nony_results = linear_regression(mt_nony)
		mt_y_results = linear_regression(mt_y)
		mt_results = mt_nony_results.union(mt_y_results)

	elif args.test in ['hail.b.wald','hail.b.firth','hail.b.lrt','hail.b.score']:
		mt_nony_results = logistic_regression(mt_nony, args.test)
		mt_y_results = logistic_regression(mt_y, args.test)
		mt_results = mt_nony_results.union(mt_y_results)

	else:
		print("association test " + args.test + " not yet available!")
		return 1

	mt_results = mt_results.key_by()
	mt_results = mt_results.annotate(chr_idx = hl.cond(mt_results.locus.in_autosome(), hl.int(mt_results.chr), hl.cond(mt_results.locus.contig == "X", 23, hl.cond(mt_results.locus.contig == "Y", 24, hl.cond(mt_results.locus.contig == "MT", 25, 26)))))
	mt_results = mt_results.drop(mt_results.locus, mt_results.alleles)
	mt_results = mt_results.order_by(hl.int(mt_results.chr_idx), hl.int(mt_results.pos), mt_results.ref, mt_results.alt)
	mt_results = mt_results.drop(mt_results.chr_idx)
	mt_results = mt_results.rename({'chr': '#chr'})
	mt_results.export(args.out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--trans', help='a transformation code')
	parser.add_argument('--covars', help="a '+' separated list of covariates")
	parser.add_argument('--extract', help="a variant list to extract for analysis")
	parser.add_argument('--extract-ld', help="a file containing hild proxy results in the form (SNP_A	SNP_B	R2)")
	parser.add_argument('--ancestry-in', help='an inferred ancestry file')
	parser.add_argument('--pops', help='a comma separated list of populations to include in analysis')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a matrix table', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--pcs-include', help='a file containing a list of PCs to include in test', required=True)
	requiredArgs.add_argument('--variant-stats-in', help='a file containing variant stats to include in output', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--pheno-col', help='a column name for the phenotype', required=True)
	requiredArgs.add_argument('--test', choices=['hail.b.wald','hail.b.lrt','hail.b.firth','hail.b.score','hail.q.lm'], help='a regression test code', required=True)
	requiredArgs.add_argument('--out', help='an output file basename', required=True)
	args = parser.parse_args()
	main(args)
