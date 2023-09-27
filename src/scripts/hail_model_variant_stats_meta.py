import hail as hl
import argparse
import pandas as pd
import os

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
		os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-memory ' + args.driver_memory + ' --executor-memory ' + args.executor_memory + ' pyspark-shell'
		os.environ["SPARK_LOCAL_DIRS"] = args.tmp_dir
		hl.init(log = args.log, tmp_dir = args.tmp_dir, local_tmpdir = args.tmp_dir, idempotent=True)
	else:
		hl.init(idempotent=True)

	cohorts = []
	tests = {}
	files = {}
	for r in args.results.split(","):
		cohorts.append(r.split("___")[0])
		tests[r.split("___")[0]] = r.split("___")[1]
		files[r.split("___")[0]] = r.split("___")[2]

	exclusions = {}
	if args.exclusions:
		for r in args.exclusions.split(","):
			if r != "None":
				exclusions[r.split("___")[0]] = r.split("___")[1]

	if len(set(tests.values())) > 1:
		stop("inverse variance weighted meta analysis cannot be performed on results resulting from different statistical tests!")
	
	i = 0
	for c in cohorts:
		i = i + 1
		print("importing hail table for cohort " + c)
		tbl_temp = hl.import_table(files[c], impute=True, min_partitions=args.min_partitions)
		tbl_temp = tbl_temp.rename({'#chr': 'chr'})
		tbl_temp = tbl_temp.annotate(locus = hl.parse_locus(hl.str(tbl_temp.chr) + ":" + hl.str(tbl_temp.pos)), alleles =  [tbl_temp.ref, tbl_temp.alt])
		tbl_temp = tbl_temp.key_by('locus', 'alleles', 'id')
		tbl_temp = tbl_temp.drop(tbl_temp.chr, tbl_temp.pos, tbl_temp.ref, tbl_temp.alt)

		if c in exclusions:
			print("importing exclusions for cohort " + c)
			try:
				tbl_exclude = hl.import_table(exclusions[c], no_header=True, types={'f0': 'locus<GRCh37>', 'f1': 'array<str>', 'f2': 'str'}).key_by('f0', 'f1', 'f2')
			except:
				print("skipping empty file " + exclusions[c])
			else:
				tbl_temp = tbl_temp.filter(hl.is_defined(tbl_exclude[tbl_temp.key]), keep=False)

		# convert any numeric columns that imputed as string
		if 'beta' in list(tbl_temp.row): tbl_temp = tbl_temp.annotate(beta = hl.float(tbl_temp.beta))
		if 'se' in list(tbl_temp.row): tbl_temp = tbl_temp.annotate(se = hl.float(tbl_temp.se))
		if 't_stat' in list(tbl_temp.row): tbl_temp = tbl_temp.annotate(t_stat = hl.float(tbl_temp.t_stat))
		if 'z_stat' in list(tbl_temp.row): tbl_temp = tbl_temp.annotate(z_stat = hl.float(tbl_temp.z_stat))
		if 'chi_sq_stat' in list(tbl_temp.row): tbl_temp = tbl_temp.annotate(chi_sq_stat = hl.float(tbl_temp.chi_sq_stat))
		if 'pval' in list(tbl_temp.row): tbl_temp = tbl_temp.annotate(pval = hl.float(tbl_temp.pval))

		# filter out if pval missing
		tbl_temp = tbl_temp.filter(~ hl.is_missing(tbl_temp.pval))
		
		# add cohort specific calculations
		tbl_temp = tbl_temp.annotate(
			af_n = tbl_temp.af * tbl_temp.n,
			dir = hl.cond(
				~ hl.is_missing(tbl_temp.beta),
				hl.cond(
					hl.sign(tbl_temp.beta) == 1,
					"+",
					hl.cond(
						hl.sign(tbl_temp.beta) == -1,
						"-",
						"x"
					)
				),
				"x"
			)
		)

		# calculate standard errors if firth, lrt, or lmm
		if tests[c] in ['firth','lrt','lmm']:
			tbl_temp = tbl_temp.annotate(se = tbl_temp.beta / hl.sqrt(tbl_temp.chi_sq_stat))

		# add cohort weights
		tbl_temp = tbl_temp.annotate(w = 1 / (tbl_temp.se ** 2))
		tbl_temp = tbl_temp.annotate(bw = tbl_temp.beta * tbl_temp.w)

		tbl_temp = tbl_temp.rename(dict(zip(list(tbl_temp.row_value), [c + '_' + x for x in list(tbl_temp.row_value)])))
		if i == 1:
			tbl = tbl_temp
		else:
			tbl = tbl.join(tbl_temp, how='outer')
	
	# add n, male, female, case, and ctrl columns as sums
	n_cols = [x + "_n" for x in cohorts]
	male_cols = [x + "_male" for x in cohorts]
	female_cols = [x + "_female" for x in cohorts]
	case_cols = [x + "_case" for x in cohorts if x + "_case" in list(tbl.row_value)]
	ctrl_cols = [x + "_ctrl" for x in cohorts if x + "_ctrl" in list(tbl.row_value)]

	tbl = tbl.annotate(
		n = hl.sum([tbl[x] for x in n_cols]),
		male = hl.sum([tbl[x] for x in male_cols]),
		female = hl.sum([tbl[x] for x in female_cols])
	)

	if len(case_cols) > 0:
		tbl = tbl.annotate(case = hl.sum([tbl[x] for x in case_cols]))

	if len(ctrl_cols) > 0:
		tbl = tbl.annotate(ctrl = hl.sum([tbl[x] for x in ctrl_cols]))

	# calculate weighted average avgaf, minaf, and maxaf
	af_cols = [x + "_af" for x in cohorts]
	af_n_cols = [x + "_af_n" for x in cohorts]
	tbl = tbl.annotate(
		afmin = hl.min([tbl[x] for x in af_cols]),
		afmax = hl.max([tbl[x] for x in af_cols]),
		sum_af_n = hl.sum([tbl[x] for x in af_n_cols])
	)

	# calculate avg allele freq
	tbl = tbl.annotate(afavg = tbl.sum_af_n / tbl.n)

	# add dir
	dir_cols = [x + "_dir" for x in cohorts]
	tbl = tbl.annotate(dir = hl.delimit([tbl[x] for x in dir_cols], delimiter='').replace('null','x'))

	# score test has no notion of direction, so not yet supported
	if 'score' in tests.values():
		stop("score test is not yet supported!")

	# calculate meta stats
	w_cols = [x + "_w" for x in cohorts]
	bw_cols = [x + "_bw" for x in cohorts]
	tbl = tbl.annotate(
		beta = hl.sum([tbl[x] for x in bw_cols]) / hl.sum([tbl[x] for x in w_cols]),
		se = hl.or_missing(hl.sum([tbl[x] for x in w_cols]) != 0, hl.sqrt(1 / hl.sum([tbl[x] for x in w_cols]))) 
	)
	tbl = tbl.annotate(
		odds_ratio = hl.exp(tbl.beta),
		zscore = tbl.beta / tbl.se
	)
	tbl = tbl.annotate(pval = 2 * hl.pnorm(-1 * hl.abs(tbl.zscore)))
	
	# loop over cohort 2+ and fill in missing ids
	tbl = tbl.annotate(id = tbl[cohorts[0] + "_id"])
	for c in cohorts[1:]:
		tbl = tbl.annotate(id = hl.or_else(tbl.id, tbl[c + "_id"]))

	# add variant attribute columns and select output
	tbl = tbl.annotate(chr_idx = hl.cond(tbl.locus.in_autosome(), hl.int(tbl.locus.contig), hl.cond(tbl.locus.contig == "X", 23, hl.cond(tbl.locus.contig == "Y", 24, hl.cond(tbl.locus.contig == "MT", 25, 26)))), chr = tbl.locus.contig, pos = tbl.locus.position, ref = tbl.alleles[0], alt = tbl.alleles[1])
	cols_keep = ['chr_idx','chr','pos','id','ref','alt','n','male','female']
	if 'case' in list(tbl.row_value):
		cols_keep = cols_keep + ['case']
	if 'ctrl' in list(tbl.row_value):
		cols_keep = cols_keep + ['ctrl']
	cols_keep = cols_keep + ['afmin','afmax','afavg','dir','beta','se','odds_ratio','zscore','pval']
	tbl = tbl.select(*cols_keep)

	tbl = tbl.key_by()
	tbl = tbl.drop(tbl.locus, tbl.alleles)
	tbl = tbl.order_by(hl.int(tbl.chr_idx), hl.int(tbl.pos), tbl.ref, tbl.alt)
	tbl = tbl.drop(tbl.chr_idx)
	tbl = tbl.rename({'chr': '#chr', 'odds_ratio': 'or'})
	tbl.export(args.out)

	#print("read matrix table")
	#mt = hl.read_matrix_table(args.mt_in)
	#
	#print("annotate samples with phenotype file")
	#tbl = hl.import_table(
	#	args.pheno_in,
	#	no_header=False,
	#	missing="NA",
	#	impute=True,
	#	types={args.iid_col: hl.tstr}
	#)
	#tbl = tbl.key_by(args.iid_col)
	#mt = mt.annotate_cols(pheno = tbl[mt.s])
	#
	#print("reduce to samples with non-missing phenotype")
	#mt = mt.filter_cols(hl.is_missing(mt.pheno[args.pheno_analyzed]), keep=False)
	#
	#n_all = mt.rows().count()
	#print(str(n_all) + " variants remaining for analysis")
	#
	#print("calculate global variant qc")
	#mt = hl.variant_qc(mt, name="variant_qc")
	#mt = hail_utils.update_variant_qc(mt, is_female = "is_female", variant_qc = "variant_qc")
	#if args.binary:
	#	print("add case/ctrl variant qc")
	#	mt = hail_utils.add_case_ctrl_stats_results(mt, is_female = "is_female", variant_qc = "variant_qc", is_case = args.pheno_analyzed)
	#	print("add differential missingness")
	#	mt = hail_utils.add_diff_miss(mt, is_female = "is_female", variant_qc = "variant_qc", is_case = args.pheno_analyzed, diff_miss_min_expected_cell_count = 5)
	#
	#tbl = mt.rows()
	#tbl = tbl.annotate(
	#	chr = tbl.locus.contig,
	#	pos = tbl.locus.position,
	#	ref = tbl.alleles[0],
	#	alt = tbl.alleles[1]
	#)
	#tbl = tbl.select(*['chr', 'pos', 'ref', 'alt', 'rsid','uid','variant_qc'])
	#tbl = tbl.flatten()
	#tbl = tbl.rename(dict(zip(list(tbl.row),[x.replace('variant_qc.','') if x.startswith('variant_qc') else x for x in list(tbl.row)])))
	#tbl = tbl.key_by()
	#tbl = tbl.annotate(chr_idx = hl.if_else(tbl.locus.in_autosome(), hl.int(tbl.chr.replace("chr","")), hl.if_else(tbl.locus.contig.replace("chr","") == "X", 23, hl.if_else(tbl.locus.contig.replace("chr","") == "Y", 24, hl.if_else(tbl.locus.contig.replace("chr","") == "MT", 25, hl.if_else(tbl.locus.contig.replace("chr","") == "M", 25, 26))))))
	#tbl = tbl.order_by(hl.int(tbl.chr_idx), hl.int(tbl.pos), tbl.ref, tbl.alt)
	#tbl = tbl.drop(tbl.chr_idx)
	#tbl = tbl.rename({'chr': '#chr'})
	#
	#drop_cols = ['locus', 'alleles', 'homozygote_count']
	#if args.binary:
	#	drop_cols = drop_cols + ['diff_miss_row1_sum', 'diff_miss_row2_sum', 'diff_miss_col1_sum', 'diff_miss_col2_sum', 'diff_miss_tbl_sum', 'diff_miss_expected_c1', 'diff_miss_expected_c2', 'diff_miss_expected_c3', 'diff_miss_expected_c4']
	#
	#tbl = tbl.drop(*drop_cols)
	#
	#tbl.export(args.out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--min-partitions', type=int, default=None, help='number of partitions')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--exclusions', help='a comma separated list of cohort ids and exclusion files each separated by 3 underscores')
	parser.add_argument('--driver-memory', default="1g", help='spark driver memory')
	parser.add_argument('--executor-memory', default="1g", help='spark executor memory')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--results', help='a comma separated list of cohort ids, test codes, and results files each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--out', help='an output file basename', required=True)
	args = parser.parse_args()
	main(args)

	#parser = argparse.ArgumentParser()
	#parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	#parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	#parser.add_argument('--binary', action='store_true', default=False, help='flag indicates whether or not the phenotype analyzed is binary')
	#parser.add_argument('--tmp-dir', help='a temporary path')
	#requiredArgs = parser.add_argument_group('required arguments')
	#requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	#requiredArgs.add_argument('--mt-in', help='a matrix table', required=True)
	#requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	#requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	#requiredArgs.add_argument('--pheno-analyzed', help='a column name for the phenotype used in analysis', required=True)
	#requiredArgs.add_argument('--out', help='an output file basename', required=True)
	#args = parser.parse_args()
	#main(args)

#chr
pos
ref
alt
rsid
AF = AC/AN
AF_case = AC_case/AN_case
AF_ctrl = AC_ctrl/AN_ctrl
MAF = ifelse(AF<0.5, AF, 1-AF)
MAF_case = ifelse(AF_case<0.5, AF_case, 1-AF_case)
MAF_ctrl = ifelse(AF_ctrl<0.5, AF_ctrl, 1-AF_ctrl)
AC
AC_case
AC_ctrl
MAC
MAC_case
MAC_ctrl
AN
n_called
n_not_called
n_filtered
n_het
n_non_ref
n_male_het
n_male_homvar
n_male_homref
n_male_called
n_female_het
n_female_homvar
n_female_homref
n_female_called
n_hom_var
n_hom_ref
n_case_called
n_case_male_het
n_case_male_hom_var
n_case_male_hom_ref
n_case_male_called
n_case_female_het
n_case_female_hom_var
n_case_female_hom_ref
n_case_female_called
n_ctrl_called
n_ctrl_male_het
n_ctrl_male_hom_var
n_ctrl_male_hom_ref
n_ctrl_male_called
n_ctrl_female_het
n_ctrl_female_hom_var
n_ctrl_female_hom_ref
n_ctrl_female_called
n_hom_var_case
n_hom_var_ctrl
n_hom_ref_case
n_hom_ref_ctrl
n_het_case
n_het_ctrl
n_case_not_called
n_ctrl_not_called
