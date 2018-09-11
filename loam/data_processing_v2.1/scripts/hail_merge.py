from hail import *
hc = HailContext()
import argparse


def main(args=None):

	results=[]
	for r in args.results.split(","):
		results.append([r.split('___')[0], r.split('___')[1]])
	
	cols = {}
	for i in xrange(len(results)):
		kt = hc.import_table(results[i][1], no_header=False, missing="NA", impute=True, types={'uid': TString()}).key_by('uid')
		cols[results[i][0]] = [str(c).replace("#","") for c in kt.columns]
		kt = kt.rename([results[i][0] + '_' + x.replace("#","") if x != 'uid' else x for x in kt.columns])
		kt = kt.annotate(results[i][0] + '_cohort = "' + results[i][0] + '"')
		if i == 0:
			ktfinal = kt
		else:
			ktfinal = ktfinal.join(kt,how='outer')

	cols_keep = [results[0][0] + '_' + c if c != 'uid' else c for c in cols[results[0][0]]]
	it = iter(results[1:])
	for x in it:
		cols_shared = [c for c in cols[results[0][0]] if c in cols[x[0]]]
		cols_notshared = [c for c in cols[x[0]] if c not in cols[results[0][0]]]
		if len(cols_shared ) > 0:
			ktfinal = ktfinal.annotate(", ".join([results[0][0] + "_" + c + " = orElse(" + results[0][0] + "_" + c + ", " + x[0] + "_" + c + ")" for c in cols_shared if c != 'uid'] + [results[0][0] + "_cohort = orElse(" + results[0][0] + "_cohort, " + x[0] + "_cohort)"]))
		if len(cols_notshared ) > 0:
			ktfinal = ktfinal.annotate(", ".join([results[0][0] + "_" + c + " = " + x[0] + "_" + c for c in cols_notshared]))
		cols_keep = cols_keep + [results[0][0] + "_" + c for c in cols_shared if results[0][0] + "_" + c not in cols_keep if c != 'uid']
		cols_keep = cols_keep + [results[0][0] + "_" + c for c in cols_notshared]

	ktfinal = ktfinal.select(cols_keep + [results[0][0] + "_cohort"])
	ktfinal = ktfinal.rename([x.split("_")[-1] for x in ktfinal.columns])

	ktout = ktfinal.to_pandas()
	ktout = ktout.sort_values(['chr','pos'])
	ktout.rename(columns = {'chr': '#chr'}, inplace=True)

	with hadoop_write(args.out) as f:
		ktout.to_csv(f, header=True, index=False, sep="\t", na_rep="NA", compression="gzip")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a comma separated list of cohort codes followed by a file name containing results, each separated by "___"', required=True)
	requiredArgs.add_argument('--test', help='an association test code', required=True)
	requiredArgs.add_argument('--out', help='an output filename for merged results', required=True)
	args = parser.parse_args()
	main(args)
