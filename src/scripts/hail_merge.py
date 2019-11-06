import hail as hl
import argparse

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log, idempotent=True)
	else:
		hl.init(idempotent=True)

	results=[]
	for r in args.results.split(","):
		results.append([r.split('___')[0], r.split('___')[1]])

	exclusions = {}
	if args.exclusions:
		for r in args.exclusions.split(","):
			if r != "None":
				exclusions[r.split("___")[0]] = r.split("___")[1]
	
	cols = {}
	for i in range(len(results)):
		tbl_temp = hl.import_table(results[i][1], no_header=False, missing="NA", impute=True)
		tbl_temp = tbl_temp.filter(~ hl.is_missing(tbl_temp.pval))
		tbl_temp = tbl_temp.rename({'#chr': 'chr'})
		tbl_temp = tbl_temp.annotate(locus = hl.parse_locus(hl.str(tbl_temp.chr) + ":" + hl.str(tbl_temp.pos)), alleles =  [tbl_temp.ref, tbl_temp.alt])
		tbl_temp = tbl_temp.key_by('locus', 'alleles', 'id')
		tbl_temp = tbl_temp.drop(tbl_temp.chr, tbl_temp.pos, tbl_temp.ref, tbl_temp.alt)

		if results[i][0] in exclusions:
			print("importing exclusions for cohort " + results[i][0])
			try:
				tbl_exclude = hl.import_table(exclusions[results[i][0]], no_header=True, types={'f0': 'locus<GRCh37>', 'f1': 'array<str>', 'f2': 'str'}).key_by('f0', 'f1', 'f2')
			except:
				print("skipping empty file " + exclusions[results[i][0]])
			else:
				tbl_temp = tbl_temp.filter(hl.is_defined(tbl_exclude[tbl_temp.key]), keep=False)

		cols[results[i][0]] = [str(c).replace("#","") for c in tbl_temp.row_value]
		tbl_temp = tbl_temp.rename(dict(zip(list(tbl_temp.row_value), [results[i][0] + '_' + x for x in list(tbl_temp.row_value)])))
		tbl_temp = tbl_temp.annotate(**{results[i][0] + '_cohort': results[i][0]})
		if i == 0:
			tbl = tbl_temp
		else:
			tbl = tbl.join(tbl_temp,how='outer')

	cols_keep = [results[0][0] + '_' + c for c in cols[results[0][0]]]

	it = iter(results[1:])
	for x in it:
		cols_shared = [c for c in cols[x[0]] if results[0][0] + '_' + c in cols_keep]
		cols_notshared = [c for c in cols[x[0]] if results[0][0] + '_' + c not in cols_keep]
		for c in cols_shared:
			tbl = tbl.annotate(**{results[0][0] + "_" + c: hl.cond(~hl.is_missing(tbl[results[0][0] + "_pval"]), tbl[results[0][0] + "_" + c], tbl[x[0] + "_" + c])})
			tbl = tbl.annotate(**{results[0][0] + "_cohort": hl.cond(~hl.is_missing(tbl[results[0][0] + "_pval"]), tbl[results[0][0] + "_cohort"], tbl[x[0] + "_cohort"])})
		for c in cols_notshared:
			tbl = tbl.annotate(**{results[0][0] + "_" + c: hl.or_missing(tbl[results[0][0] + "_cohort"] == x[0], tbl[x[0] + "_" + c])})
		cols_keep = cols_keep + [results[0][0] + "_" + c for c in cols_shared if results[0][0] + "_" + c not in cols_keep]
		cols_keep = cols_keep + [results[0][0] + "_" + c for c in cols_notshared]

	cols_select = cols_keep + [results[0][0] + "_cohort"]
	tbl = tbl.select(*cols_select)
	tbl = tbl.rename(dict(zip(list(tbl.row_value), [x.replace(results[0][0] + "_","") for x in list(tbl.row_value)])))

	cols_order = ['chr_idx','chr','pos','ref','alt'] + list(tbl.row_value)
	tbl = tbl.annotate(chr = tbl.locus.contig, pos = tbl.locus.position, ref = tbl.alleles[0], alt = tbl.alleles[1])
	tbl = tbl.key_by()
	tbl = tbl.annotate(chr_idx = hl.cond(tbl.locus.in_autosome(), hl.int(tbl.chr), hl.cond(tbl.locus.contig == "X", 23, hl.cond(tbl.locus.contig == "Y", 24, hl.cond(tbl.locus.contig == "MT", 25, 26)))))
	tbl = tbl.drop(tbl.locus, tbl.alleles)
	tbl = tbl.select(*cols_order)
	tbl = tbl.order_by(hl.int(tbl.chr_idx), hl.int(tbl.pos), tbl.ref, tbl.alt)
	tbl = tbl.drop(tbl.chr_idx)
	tbl = tbl.rename({'chr': '#chr'})
	tbl.export(args.out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--exclusions', help='a comma separated list of cohort ids and exclusion files each separated by 3 underscores')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--results', help='a comma separated list of cohort codes, results files, and exclusions files, each separated by "___"', required=True)
	requiredArgs.add_argument('--out', help='an output filename for merged results', required=True)
	args = parser.parse_args()
	main(args)
