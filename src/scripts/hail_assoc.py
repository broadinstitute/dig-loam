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
			types={'f0': hl.tint, 'f1': hl.tstr}
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

	if args.test != 'lmm':
		print("read in list of PCs to include in test")
		with hl.hadoop_open(args.pcs_include, "r") as f:
			pcs = f.read().splitlines()
	else:
		pcs = []

	covars = [x for x in args.covars.split("+")] if args.covars != "" else []
	if args.trans == "invn":
		pheno_analyzed = args.pheno_col + '_invn_' + "_".join([x.replace("[","").replace("]","") for x in args.covars.split("+")])
		covars = pcs
	else:
		pheno_analyzed = args.pheno_col
		pheno_df = mt.cols().to_pandas()
		for i in range(len(covars)):
			if covars[i][0] == "[" and covars[i][-1] == "]":
				for val in sorted(pheno_df['pheno.' + covars[i][1:-1]].unique())[1:]:
					covars = covars + [covars[i][1:-1] + str(val)]
				covars = [x for x in covars if x != covars[i]]
		covars = covars + pcs

	print("calculate variant qc")
	mt = hl.variant_qc(mt, name="variant_qc")

	if args.test == 'lm':
		mt = hail_utils.update_variant_qc(mt, is_female = "is_female", variant_qc = "variant_qc")
	elif args.test in ['wald','firth','lrt','score']:
		mt = hail_utils.update_variant_qc(mt, is_female = "is_female", variant_qc = "variant_qc", is_case = args.pheno_col)
	else:
		print("test " + args.test + " not currently supported!")
		return 1

	print("generate Y and non-Y chromosome sets (to account for male only Y chromosome)")
	mt_nony = hl.filter_intervals(mt, [hl.parse_locus_interval(str(x)) for x in range(1,23)] + [hl.parse_locus_interval(x) for x in ['X','MT']], keep=True)
	mt_y = hl.filter_intervals(mt, [hl.parse_locus_interval('Y')], keep=True)
	mt_y = mt_y.filter_cols(mt_y.is_female, keep=False)

	#def calc_variant_attributes(mt):
    #
	#	print("count males and females")
	#	tbl = mt.cols()
	#	mt = mt.annotate_globals(
	#		global_n = tbl.count(),
	#		global_n_males = tbl.aggregate(hl.agg.count_where(~ tbl.is_female)),
	#		global_n_females = tbl.aggregate(hl.agg.count_where(tbl.is_female))
	#	)
	#	
	#	print("count male/female hets, homvars and called")
	#	mt = mt.annotate_rows(
	#		results = hl.struct(
	#			n_called = hl.agg.count_where(hl.is_defined(mt.GT)),
	#			n_male_diploid = hl.agg.count_where((~ mt.is_female) & (mt.GT.is_diploid())),
	#			n_male_haploid = hl.agg.count_where((~ mt.is_female) & (mt.GT.is_haploid())),
	#			n_male_het = hl.agg.count_where((~ mt.is_female) & (mt.GT.is_het())),
	#			n_male_hom_var = hl.agg.count_where((~ mt.is_female) & (mt.GT.is_hom_var())),
	#			n_male_hom_ref = hl.agg.count_where((~ mt.is_female) & (mt.GT.is_hom_ref())),
	#			n_male_called = hl.agg.count_where((~ mt.is_female) & (hl.is_defined(mt.GT))),
	#			n_female_diploid = hl.agg.count_where((mt.is_female) & (mt.GT.is_diploid())),
	#			n_female_haploid = hl.agg.count_where((mt.is_female) & (mt.GT.is_haploid())),
	#			n_female_het = hl.agg.count_where((mt.is_female) & (mt.GT.is_het())),
	#			n_female_hom_var = hl.agg.count_where((mt.is_female) & (mt.GT.is_hom_var())),
	#			n_female_hom_ref = hl.agg.count_where((mt.is_female) & (mt.GT.is_hom_ref())),
	#			n_female_called = hl.agg.count_where((mt.is_female) & (hl.is_defined(mt.GT)))
	#		)
	#	)
	#
	#	if args.test in ["wald","firth","lrt","score"]:
	#		mt = mt.annotate_rows(
	#			results = mt.results.annotate(
	#				n_case_called = hl.agg.count_where((mt.pheno[pheno_analyzed] == 1) & (hl.is_defined(mt.GT))),
	#				n_male_case_called = hl.agg.count_where((mt.pheno[pheno_analyzed] == 1) & (hl.is_defined(mt.GT)) & (~ mt.is_female)),
	#				n_male_case_hom_var = hl.agg.count_where((mt.pheno[pheno_analyzed] == 1) & (mt.GT.is_hom_var()) & (~ mt.is_female)),
	#				n_male_case_het = hl.agg.count_where((mt.pheno[pheno_analyzed] == 1) & (mt.GT.is_het()) & (~ mt.is_female)),
	#				n_female_case_called = hl.agg.count_where((mt.pheno[pheno_analyzed] == 1) & (hl.is_defined(mt.GT)) & (mt.is_female)),
	#				n_female_case_hom_var = hl.agg.count_where((mt.pheno[pheno_analyzed] == 1) & (mt.GT.is_hom_var()) & (mt.is_female)),
	#				n_female_case_het = hl.agg.count_where((mt.pheno[pheno_analyzed] == 1) & (mt.GT.is_het()) & (mt.is_female)),
	#				n_ctrl_called = hl.agg.count_where((mt.pheno[pheno_analyzed] == 0) & (hl.is_defined(mt.GT))),
	#				n_male_ctrl_called = hl.agg.count_where((mt.pheno[pheno_analyzed] == 0) & (hl.is_defined(mt.GT)) & (~ mt.is_female)),
	#				n_male_ctrl_hom_var = hl.agg.count_where((mt.pheno[pheno_analyzed] == 0) & (mt.GT.is_hom_var()) & (~ mt.is_female)),
	#				n_male_ctrl_het = hl.agg.count_where((mt.pheno[pheno_analyzed] == 0) & (mt.GT.is_het()) & (~ mt.is_female)),
	#				n_female_ctrl_called = hl.agg.count_where((mt.pheno[pheno_analyzed] == 0) & (hl.is_defined(mt.GT)) & (mt.is_female)),
	#				n_female_ctrl_hom_var = hl.agg.count_where((mt.pheno[pheno_analyzed] == 0) & (mt.GT.is_hom_var()) & (mt.is_female)),
	#				n_female_ctrl_het = hl.agg.count_where((mt.pheno[pheno_analyzed] == 0) & (mt.GT.is_het()) & (mt.is_female))
	#			)
	#		)
	#
	#	print("calculate callRate, AC, and AF (accounting appropriately for sex chromosomes)")
	#	mt = mt.annotate_rows(
	#		results = mt.results.annotate(
	#			call_rate = hl.cond(
	#				mt.locus.in_y_nonpar(),
	#				mt.results.n_male_called / mt.global_n_males,
	#				hl.cond(
	#					mt.locus.in_x_nonpar(),
	#					(mt.results.n_male_called + 2*mt.results.n_female_called) / (mt.global_n_males + 2*mt.global_n_females),
	#					(mt.results.n_male_called + mt.results.n_female_called) / (mt.global_n_males + mt.global_n_females)
	#				)
	#			),
	#			ac = hl.cond(
	#				mt.locus.in_y_nonpar(),
	#				mt.results.n_male_hom_var,
	#				hl.cond(
	#					mt.locus.in_x_nonpar(),
	#					mt.results.n_male_hom_var + mt.results.n_female_het + 2*mt.results.n_female_hom_var,
	#					mt.results.n_male_het + 2*mt.results.n_male_hom_var + mt.results.n_female_het + 2*mt.results.n_female_hom_var
	#				)
	#			),
	#			af = hl.cond(
	#				mt.locus.in_y_nonpar(),
	#				mt.results.n_male_hom_var / mt.results.n_male_called,
	#				hl.cond(
	#					mt.locus.in_x_nonpar(),
	#					(mt.results.n_male_hom_var + mt.results.n_female_het + 2*mt.results.n_female_hom_var) / (mt.results.n_male_called + 2*mt.results.n_female_called),
	#					(mt.results.n_male_het + 2*mt.results.n_male_hom_var + mt.results.n_female_het + 2*mt.results.n_female_hom_var) / (2*mt.results.n_male_called + 2*mt.results.n_female_called)
	#				)
	#			)
	#		)
	#	)
    #
	#	if args.test in ["wald","firth","lrt","score"]:
	#		print("calculate AF in cases and ctrls (accounting appropriately for sex chromosomes)")
	#		mt = mt.annotate_rows(
	#			results = mt.results.annotate(
	#				af_case = hl.cond(
	#					mt.locus.in_y_nonpar(),
	#					mt.results.n_male_case_hom_var / mt.results.n_male_case_called,
	#					hl.cond(
	#						mt.locus.in_x_nonpar(),
	#						(mt.results.n_male_case_hom_var + mt.results.n_female_case_het + 2*mt.results.n_female_case_hom_var) / (mt.results.n_male_case_called + 2*mt.results.n_female_case_called),
	#						(mt.results.n_male_case_het + 2*mt.results.n_male_case_hom_var + mt.results.n_female_case_het + 2*mt.results.n_female_case_hom_var) / (2*mt.results.n_male_case_called + 2*mt.results.n_female_case_called)
	#					)
	#				),
	#				af_ctrl = hl.cond(
	#					mt.locus.in_y_nonpar(),
	#					mt.results.n_male_ctrl_hom_var / mt.results.n_male_ctrl_called,
	#					hl.cond(
	#						mt.locus.in_x_nonpar(),
	#						(mt.results.n_male_ctrl_hom_var + mt.results.n_female_ctrl_het + 2*mt.results.n_female_ctrl_hom_var) / (mt.results.n_male_ctrl_called + 2*mt.results.n_female_ctrl_called),
	#						(mt.results.n_male_ctrl_het + 2*mt.results.n_male_ctrl_hom_var + mt.results.n_female_ctrl_het + 2*mt.results.n_female_ctrl_hom_var) / (2*mt.results.n_male_ctrl_called + 2*mt.results.n_female_ctrl_called)
	#					)
	#				)
	#			)
	#		)
	#
	#	print("calculate mac and maf (accounting appropriately for sex chromosomes)")
	#	mt = mt.annotate_rows(
	#		results = mt.results.annotate(
	#			mac = hl.cond(
	#				mt.results.af <= 0.5,
	#				mt.results.ac,
	#				2*mt.results.n_called - mt.results.ac
	#			),
	#			maf = hl.cond(
	#				mt.results.af <= 0.5,
	#				mt.results.af,
	#				1 - mt.results.af)
	#		)
	#	)
	#	return mt

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
			test = test,
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

		if test == 'wald':
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
		elif test == 'firth':
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
		elif test == 'lrt':
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
		elif test == 'score':
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

	if args.test == 'lm':
		mt_nony_results = linear_regression(mt_nony)
		mt_y_results = linear_regression(mt_y)
		mt_results = mt_nony_results.union(mt_y_results)

	elif args.test in ['wald','firth','lrt','score']:
		mt_nony_results = logistic_regression(mt_nony, args.test)
		mt_y_results = logistic_regression(mt_y, args.test)
		mt_results = mt_nony_results.union(mt_y_results)

	mt_results = mt_results.key_by()
	mt_results = mt_results.annotate(chr_idx = hl.cond(mt_results.locus.in_autosome(), hl.int(mt_results.chr), hl.cond(mt_results.locus.contig == "X", 23, hl.cond(mt_results.locus.contig == "Y", 24, hl.cond(mt_results.locus.contig == "MT", 25, 26)))))
	mt_results = mt_results.drop(mt_results.locus, mt_results.alleles)
	mt_results = mt_results.order_by(hl.int(mt_results.chr_idx), hl.int(mt_results.pos), mt_results.ref, mt_results.alt)
	mt_results = mt_results.drop(mt_results.chr_idx)
	mt_results = mt_results.rename({'chr': '#chr'})
	mt_results.export(args.out)

	#elif args.test == 'lmm':
	#	print "extracting variants from previously filtered and pruned bim file"
	#	bim = hc.import_table(args.bim_in, no_header=True, types={'f1': TVariant()}).key_by('f1')
	#	kinship = vds.filter_variants_table(bim, keep=True).rrm()
	#	result_nony = vds.filter_intervals([Interval.parse(str(x)) for x in range(1,23)] + [Interval.parse(x) for x in ['X','MT']]).lmmreg(kinship, 'pheno.' + pheno_analyzed, covariates=covars_analyzed, global_root='global.' + args.test + '.' + pheno_analyzed, root='va.' + args.test + '.' + pheno_analyzed, use_dosages=False, dropped_variance_fraction=0.01, run_assoc=True, use_ml=False).variants_table()
	#	result_y = vds.filter_intervals(Interval.parse('Y')).filter_samples_expr('! sa.isFemale').lmmreg(kinship, 'pheno.' + pheno_analyzed, covariates=covars_analyzed, global_root='global.' + args.test + '.' + pheno_analyzed, root='va.' + args.test + '.' + pheno_analyzed, use_dosages=False, dropped_variance_fraction=0.01, run_assoc=True, use_ml=False).variants_table()
	#	vds.annotate_variants_table(result_nony.union(result_y), root='va').export_variants(args.out, expr="#chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.callRate, ac = va.AC, af = va.AF, mac = if (va.AF <= 0.5) (va.AC) else (2 * va.nCalled - va.AC), maf = if (va.AF <= 0.5) (va.AF) else (1 - va.AF), beta = va." + args.test + "." + pheno_analyzed + ".beta, sigmaG2 = va." + args.test + "." + pheno_analyzed + ".sigmaG2, chi2 = va." + args.test + "." + pheno_analyzed + ".chi2, pval = va." + args.test + "." + pheno_analyzed + ".pval", types=False)
	#else:
	#	return 1

	if args.variants_stats_out:
		print("write variant qc metrics to file")
		tbl = mt.rows()
		tbl = tbl.drop(tbl.variant_qc_raw)
		tbl.flatten().export(args.variants_stats_out, header=True)

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
	parser.add_argument('--variants-stats-out', help='a base filename for variant qc')
	parser.add_argument('--pops', help='a comma separated list of populations to include in analysis')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a matrix table', required=True)
	requiredArgs.add_argument('--bim-in', help='a filtered and pruned bim file', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--pcs-include', help='a file containing a list of PCs to include in test', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--pheno-col', help='a column name for the phenotype', required=True)
	requiredArgs.add_argument('--test', choices=['wald','lrt','firth','score','lm','lmm'], help='a regression test code', required=True)
	requiredArgs.add_argument('--out', help='an output file basename', required=True)
	args = parser.parse_args()
	main(args)
