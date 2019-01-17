import hail as hl
import argparse
import pandas as pd
hl.init()

def main(args=None):

	cohorts = []
	tests = {}
	files = {}
	for r in args.results.split(","):
		cohorts.append(r.split("___")[0])
		tests[r.split("___")[0]] = r.split("___")[1]
		files[r.split("___")[0]] = r.split("___")[2]
	
	if len(set(tests.values())) > 1:
		stop("inverse variance weighted meta analysis cannot be performed on results resulting from different statistical tests!")
	
	i = 0
	for c in cohorts:
		i = i + 1
		print("importing hail table for cohort " + c)
		tbl_temp = hl.import_table(files[c], impute=True, min_partitions=args.partitions)
		tbl_temp = tbl_temp.rename({'#chr': 'chr'})
		tbl_temp = tbl_temp.annotate(locus = hl.parse_locus(hl.str(tbl_temp.chr) + ":" + hl.str(tbl_temp.pos)), alleles =  [tbl_temp.ref, tbl_temp.alt])
		tbl_temp = tbl_temp.key_by('locus', 'alleles')
		tbl_temp = tbl_temp.drop(tbl_temp.chr, tbl_temp.pos, tbl_temp.ref, tbl_temp.alt)

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
	tbl = tbl.annotate(chr = tbl.locus.contig, pos = tbl.locus.position, ref = tbl.alleles[0], alt = tbl.alleles[1])
	cols_keep = ['chr','pos','id','ref','alt','n','male','female']
	if 'case' in list(tbl.row_value):
		cols_keep = cols_keep + ['case']
	if 'ctrl' in list(tbl.row_value):
		cols_keep = cols_keep + ['ctrl']
	cols_keep = cols_keep + ['afmin','afmax','afavg','dir','beta','se','odds_ratio','zscore','pval']
	tbl = tbl.select(*cols_keep)

	tbl = tbl.key_by()
	tbl = tbl.drop(tbl.locus, tbl.alleles)
	tbl = tbl.order_by(hl.int(tbl.chr), hl.int(tbl.pos), tbl.ref, tbl.alt)
	tbl = tbl.rename({'chr': '#chr', 'odds_ratio': 'or'})
	tbl.export(args.out)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--partitions', type=int, default=100, help='number of partitions')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a comma separated list of test codes and results files each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--out', help='an output file basename', required=True)
	args = parser.parse_args()
	main(args)
