from hail import *
hc = HailContext()
import pandas as pd
import argparse


def main(args=None):

	cohorts = []
	tests = {}
	files = {}
	for r in args.results.split(","):
		cohorts.append(r.split("___")[0])
		tests[r.split("___")[0]] = r.split("___")[1]
		files[r.split("___")[0]] = r.split("___")[2]
	
	if len(set(tests.values())) > 1:
		stop("inverse variance weighted meta analysis cannot be performed on results from multiple different statistical tests!")
	
	i = 0
	for c in cohorts:
		i = i + 1
		print "importing keytable for cohort " + c
		kt_temp = hc.import_table(files[c], impute=True, min_partitions=8).key_by('uid')
		kt_temp = kt_temp.drop(["#chr","pos","ref","alt"])
		for col in [x for x in kt_temp.columns if x != 'uid']:
			kt_temp = kt_temp.rename({col: c + '_' + col})
		# if p value is missing, set all to missing
		kt_temp = kt_temp.annotate(", ".join([x + " = orMissing(! isMissing(" + c + "_pval), " + x + ")" for x in kt_temp.columns if x != 'uid']))
		if i == 1:
			kt = kt_temp
		else:
			kt = kt.join(kt_temp, how='outer')
	
	# add n, male, female, case, and ctrl columns as sums
	nCols = [x + "_n" for x in cohorts]
	maleCols = [x + "_male" for x in cohorts]
	femaleCols = [x + "_female" for x in cohorts]
	caseCols = [x + "_case" for x in cohorts if x in kt.columns]
	ctrlCols = [x + "_ctrl" for x in cohorts if x in kt.columns]
	kt = kt.annotate("n = [" + ",".join([x for x in nCols]) + "].sum(), male = [" + ",".join([x for x in maleCols]) + "].sum(), female = [" + ",".join([x for x in femaleCols]) + "].sum()")
	if len(caseCols) > 0:
		kt = kt.annotate("case = [" + ",".join([x for x in caseCols]) + "].sum()")
	
	if len(ctrlCols) > 0:
		kt = kt.annotate("ctrl = [" + "+".join([x for x in ctrlCols]) + "].sum()")
	
	# calculate weighted average avgaf, minaf, and maxaf
	afCols = [x + "_af" for x in cohorts]
	kt = kt.annotate("afmin = [" + ",".join([x + "_af" for x in cohorts]) + "].min()")
	kt = kt.annotate("afmax = [" + ",".join([x + "_af" for x in cohorts]) + "].max()")
	kt = kt.annotate(", ".join([x + "_af_n = " + x + "_af*" + x + "_n"  for x in cohorts]))
	kt = kt.annotate("sum_af_n = [" + ",".join([x + "_af_n" for x in cohorts]) + "].sum()")
	kt = kt.annotate("sum_n = [" + ",".join([x + "_n" for x in cohorts]) + "].sum()")
	kt = kt.annotate("afavg = sum_af_n / sum_n")
	
	# add dir
	kt = kt.annotate(", ".join([x + '_dir = if(! isMissing(' + x + '_beta)) ( if(signum(' + x + '_beta) == 1) ("+") else (if(signum(' + x + '_beta) == -1) ("-") else ("x"))) else ("x")'  for x in cohorts]))
	kt = kt.annotate('dir = [' + ",".join([x + '_dir' for x in cohorts]) + '].mkString("")')
	
	if 'score' in tests.values():
		stop("score test is not yet supported!")
	
	# calculate standard errors if firth, lrt, or lmm
	kt = kt.annotate(", ".join([x + "_se = " + x + "_beta / sqrt(" + x + "_chi2)" for x in cohorts if tests[x] in ['firth','lrt','lmm']]))
	
	# add cohort weights
	kt = kt.annotate(", ".join([x + "_w = 1 / pow(" + x + "_se,2)" for x in cohorts]))
	kt = kt.annotate(", ".join([x + "_bw = " + x + "_beta * " + x + "_w" for x in cohorts]))
	
	# calculate meta stats
	# values available for meta by assoc test
	# lm: beta, se, tstat, pval
	# wald: beta, se, zstat, pval
	# firth: beta, chi2, pval
	# lrt: beta, chi2, pval
	# score: chi2, pval
	# lmm: beta, sigmaG2, chi2, pval
	kt = kt.annotate("beta = ([" + ",".join([x + "_bw" for x in cohorts]) + "].sum()) / ([" + ",".join([x + "_w" for x in cohorts]) + "].sum())")
	kt = kt.annotate("se = sqrt(1 / ([" + ",".join([x + "_w" for x in cohorts]) + "].sum()))")
	kt = kt.annotate("or = exp(beta)")
	kt = kt.annotate("zscore = beta / se")
	kt = kt.annotate("pval = 2 * pnorm(-1 * abs(zscore))")
	
	# loop over cohort 2+ and fill in missing ids
	kt = kt.annotate("id = " + cohorts[0] + "_id")
	for c in cohorts[1:]:
		kt = kt.annotate("id = orElse(id, " + c + "_id)")
	
	# add variant attribute columns and select output
	kt = kt.annotate("uidstr = str(uid), chr = uid.contig, pos = uid.start, ref = uid.ref, alt = uid.alt")
	kt = kt.select(['chr','pos','uidstr','id','ref','alt','n','male','female','afmin','afmax','afavg','dir','beta','se','or','zscore','pval'])

	ktout = kt.to_pandas(expand=False)
	ktout[['chr','pos']] = ktout[['chr','pos']].astype(int)
	ktout = ktout.sort_values(['chr','pos'])
	ktout.rename(columns = {'chr': '#chr', 'uidstr': 'uid'}, inplace=True)

	with hadoop_write(args.out) as f:
		ktout.to_csv(f, header=True, index=False, sep="\t", na_rep="NA", float_format='%.5g', compression="gzip")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a comma separated list of test codes and results files each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--out', help='an output file basename', required=True)
	args = parser.parse_args()
	main(args)
