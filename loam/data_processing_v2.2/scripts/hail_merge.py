import hail as hl
import argparse
hl.init()

def main(args=None):

	results=[]
	for r in args.results.split(","):
		results.append([r.split('___')[0], r.split('___')[1]])
	
	cols = {}
	for i in xrange(len(results)):
		tbl_temp = hl.import_table(results[i][1], no_header=False, missing="NA", impute=True, min_partitions=args.partitions)
		cols[results[i][0]] = [str(c).replace("#","") for c in tbl_temp.columns]
		tbl_temp = tbl_temp.rename({'#chr': 'chr'})
		tbl_temp = tbl_temp.annotate(locus = hl.parse_locus(hl.str(tbl_temp.chr) + ":" + hl.str(tbl_temp.pos)), alleles =  [tbl_temp.ref, tbl_temp.alt])
		tbl_temp = tbl_temp.key_by('locus', 'alleles')
		tbl_temp = tbl_temp.drop(tbl_temp.chr, tbl_temp.pos, tbl_temp.ref, tbl_temp.alt)
		tbl_temp = tbl_temp.rename(dict(zip(list(tbl_temp.row_value), [results[i][0] + '_' + x for x in list(tbl_temp.row_value)])))
		tbl_temp = tbl_temp.annotate({results[i][0] + '_cohort': results[i][0]})
		if i == 0:
			tbl = tbl_temp
		else:
			tbl = tbl.join(tbl_temp,how='outer')

	cols_keep = [results[0][0] + '_' + c for c in cols[results[0][0]]]
	it = iter(results[1:])
	for x in it:
		cols_shared = [c for c in cols[results[0][0]] if c in cols[x[0]]]
		cols_notshared = [c for c in cols[x[0]] if c not in cols[results[0][0]]]
		for c in cols_shared:
			tbl = tbl.annotate(tbl[results[0][0] + "_" + c] = hl.cond(~hl.is_missing(tbl[results[0][0] + "_" + c]), tbl[results[0][0] + "_" + c], tbl[x[0] + "_" + c]))
			tbl = tbl.annotate(tbl[results[0][0] + "_cohort"] = hl.cond(~hl.is_missing(tbl[results[0][0] + "_cohort"]), tbl[results[0][0] + "_cohort"], tbl[x[0] + "_cohort"]))
		for c in cols_notshared:
			tbl = tbl.annotate(tbl[results[0][0] + "_" + c] = tbl[x[0] + "_" + c])
		cols_keep = cols_keep + [results[0][0] + "_" + c for c in cols_shared if results[0][0] + "_" + c not in cols_keep]
		cols_keep = cols_keep + [results[0][0] + "_" + c for c in cols_notshared]

	tbl = tbl.select(cols_keep + [results[0][0] + "_cohort"])
	tbl = tbl.rename([x.split("_")[-1] for x in tbl.columns])

	tbl = tbl.key_by()
	tbl = tbl.drop(tbl.locus, tbl.alleles)
	tbl = tbl.annotate(chr = tbl.locus.contig, pos = tbl.locus.position, ref = tbl.alleles[0], alt = tbl.alleles[1])
	tbl = tbl.order_by(tbl.chr, tbl.pos, tbl.ref, tbl.alt)
	tbl = tbl.rename({'chr': '#chr'})
	tbl.export(args.out)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a comma separated list of cohort codes followed by a file name containing results, each separated by "___"', required=True)
	requiredArgs.add_argument('--test', help='an association test code', required=True)
	requiredArgs.add_argument('--out', help='an output filename for merged results', required=True)
	args = parser.parse_args()
	main(args)
