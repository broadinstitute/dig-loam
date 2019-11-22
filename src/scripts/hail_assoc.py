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

	print("add cohort annotation")
	tbl = hl.import_table(
		args.cohorts_map_in,
		delimiter="\t",
		no_header=True,
		types={'f0': hl.tstr, 'f1': hl.tstr}
	)
	tbl = tbl.rename({'f0': 'IID', 'f1': 'COHORT'})
	tbl = tbl.key_by('IID')
	mt = mt.annotate_cols(COHORT = tbl[mt.s].COHORT)

	print("calculate global variant qc")
	mt = hl.variant_qc(mt, name="variant_qc")
	if args.test == 'lm':
		mt = hail_utils.update_variant_qc(mt, is_female = "is_female", variant_qc = "variant_qc")
	elif args.test in ['wald','firth','lrt','score']:
		mt = hail_utils.update_variant_qc(mt, is_female = "is_female", variant_qc = "variant_qc", is_case = pheno_analyzed)
	else:
		print("test " + args.test + " not currently supported!")
		return 1

	cohorts = []
	if args.filter:
		mt = hail_utils.add_filters(mt, args.filter, 'ls_filters')
	if args.cohort_filter:
		cohorts = cohorts + [x[0] for x in args.cohort_filter]
	if args.knockout_filter:
		cohorts = cohorts + [x[0] for x in args.knockout_filter]

	if len(cohorts) > 0:
		if len(cohorts) > 1:
			mt = mt.annotate_rows(**{'variant_qc': mt['variant_qc'].annotate(max_cohort_maf = 0)})
		for cohort in set(cohorts):
			print("calculate variant qc for cohort " + cohort)
			cohort_mt = mt.filter_cols(mt.COHORT == cohort)
			cohort_mt = hl.variant_qc(cohort_mt, name = 'variant_qc_' + cohort)
			if args.test == 'lm':
				cohort_mt = hail_utils.update_variant_qc(cohort_mt, is_female = "is_female", variant_qc = 'variant_qc_' + cohort)
			elif args.test in ['wald','firth','lrt','score']:
				cohort_mt = hail_utils.update_variant_qc(cohort_mt, is_female = "is_female", variant_qc = 'variant_qc_' + cohort, is_case = pheno_analyzed)
			else:
				print("test " + args.test + " not currently supported!")
				return 1
			mt = mt.annotate_rows(**{'variant_qc_' + cohort: cohort_mt.rows()[mt.row_key]['variant_qc_' + cohort]})
			if len(cohorts) > 1:
				mt = mt.annotate_rows(**{'variant_qc': mt['variant_qc'].annotate(max_cohort_maf = hl.cond( mt['variant_qc'].max_cohort_maf < mt['variant_qc_' + cohort].MAF, mt['variant_qc_' + cohort].MAF,  mt['variant_qc'].max_cohort_maf))})
			if args.cohort_filter:
				mt = hail_utils.add_filters(mt, [[x[1],x[2].replace('variant_qc','variant_qc_' + cohort),x[3].replace('variant_qc','variant_qc_' + cohort)] for x in args.cohort_filter], 'ls_filters_' + cohort)
			if args.knockout_filter:
				mt = hail_utils.add_filters(mt, [[x[1],x[2].replace('variant_qc','variant_qc_' + cohort),x[3].replace('variant_qc','variant_qc_' + cohort)] for x in args.knockout_filter], 'ls_knockouts_' + cohort)

	if args.mask:
		masks = [x[0] for x in args.mask]
		for m in set(masks):
			masks_row = [x for x in args.mask if x[0] == m]
			mt = hail_utils.add_filters(mt, [[x[1],x[2],x[3]] for x in masks_row], 'ls_mask_' + m)

	if args.variants_stats_out:
		print("write variant metrics and filters to file")
		tbl = mt.rows().key_by('locus','alleles')
		tbl = tbl.drop("info","variant_qc_raw")
		tbl.flatten().export(args.variants_stats_out, header=True)

	#if args.test in ['burden','skat']:
	#	if args.filters:
	#		# filter variants based on all samples
	#	if args.cohort_filters:
	#		# filter within cohort
	#	if args.knockout_filters:
	#		for f in args.knockout_filters:
	#			cohort_id = f[0]
	#			filter_id = f[1]
	#			filter_fields = f[2].split(",")
	#			filter_expression = f[3]
	#			absent = False
	#			for field in filter_fields:
	#				if field not in mt.row_value.flatten():
	#					absent = True
	#				filter_expression = filter_expression.replace(field,"mt['" + field + "']")
	#			if not absent:
	#				print("knockout genotypes based on configuration filter " + filter_id + " for field/s " + ",".join(filter_fields))
	#
	#				mt = mt.annotate_entries(GTT = hl.cond(hl.is_defined(mt.GQ) & hl.is_defined(mt.GT), hl.cond(mt.GQ >= args.gq_threshold, mt.GT, hl.null(hl.tcall)), hl.null(hl.tcall)))
	#
	#				tbl = tbl.annotate(
	#					ls_filters = tbl.ls_filters.annotate(
	#						**{filter_id: hl.cond(eval(hl.eval(filter_expression)), 1, 0, missing_false = True)}
	#					)
	#				)
	#			else:
	#				print("skipping configuration filter " + filter_id + " for field/s " + ",".join(filter_fields) + "... 1 or more fields do not exist")
	#				tbl = tbl.annotate(
	#					ls_filters = tbl.ls_filters.annotate(
	#						**{filter_id: 0}
	#					)
	#				)

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

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--trans', help='a transformation code')
	parser.add_argument('--covars', help="a '+' separated list of covariates")
	parser.add_argument('--extract', help="a variant list to extract for analysis")
	parser.add_argument('--extract-ld', help="a file containing hild proxy results in the form (SNP_A	SNP_B	R2)")
	parser.add_argument('--filter', nargs=3, action='append', help='filter id, column name, expression; exclude variants satisfying this expression')
	parser.add_argument('--cohort-filter', nargs=4, action='append', help='cohort id, filter id, column name, expression; exclude variants satisfying this expression')
	parser.add_argument('--knockout-filter', nargs=4, action='append', help='cohort id, filter id, column name, expression; exclude variants satisfying this expression')
	parser.add_argument('--mask', nargs=4, action='append', help='mask id, column name, expression; exclude variants satisfying this expression')
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
	requiredArgs.add_argument('--cohorts-map-in', help='a cohorts map file', required=True)
	requiredArgs.add_argument('--test', choices=['wald','lrt','firth','score','lm','lmm'], help='a regression test code', required=True)
	requiredArgs.add_argument('--out', help='an output file basename', required=True)
	args = parser.parse_args()
	main(args)
