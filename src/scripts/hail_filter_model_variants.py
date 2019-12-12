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

	print("reduce to samples with non-missing phenotype")
	mt = mt.filter_cols(hl.is_missing(mt.pheno[args.pheno_col]), keep=False)

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
	if not args.binary:
		mt = hail_utils.update_variant_qc(mt, is_female = "is_female", variant_qc = "variant_qc")
	else:
		mt = hail_utils.update_variant_qc(mt, is_female = "is_female", variant_qc = "variant_qc", is_case = args.pheno_col)

	cohorts = []
	filter_fields = []
	print(args.filters)
	print(args.cohort_filters)
	print(args.knockout_filters)
	print(args.masks)
	return 1



	if args.filter:
		for f in args.filter:
			filter_fields = filter_fields + [f[1]]
	if args.cohort_filter:
		cohorts = cohorts + [x[0] for x in args.cohort_filter]
		for f in args.cohort_filter:
			filter_fields = filter_fields + [f[2]]
	if args.knockout_filter:
		cohorts = cohorts + [x[0] for x in args.knockout_filter]
		for f in args.knockout_filter:
			filter_fields = filter_fields + [f[2]]
	if args.mask:
		for f in args.mask:
			filter_fields = filter_fields + [f[2]]

	annotation_fields = ['Uploaded_variation', 'Gene'] + list(set([x.replace("annotation.","") for x in filter_fields if x.startswith("annotation.")]))
	if args.annotation:
		print("add vep annotations")
		tbl = hl.read_table(args.annotation)
		tbl = tbl.select(*annotation_fields)
		tbl = tbl.key_by('Uploaded_variation')
		mt = mt.annotate_rows(annotation = tbl[mt.rsid])

	exclude_any_fields = []
	if args.filter:
		exclude_any_fields = exclude_any_fields + ['ls_filters.exclude']
		mt = hail_utils.add_filters(mt, args.filter, 'ls_filters')

	knockout_fields = []
	if len(cohorts) > 0:
		if len(cohorts) > 1:
			mt = mt.annotate_rows(**{'variant_qc': mt['variant_qc'].annotate(max_cohort_maf = 0)})
		for cohort in set(cohorts):
			print("calculate variant qc for cohort " + cohort)
			cohort_mt = mt.filter_cols(mt.COHORT == cohort)
			cohort_mt = hl.variant_qc(cohort_mt, name = 'variant_qc_' + cohort)
			if not args.binary:
				cohort_mt = hail_utils.update_variant_qc(cohort_mt, is_female = "is_female", variant_qc = 'variant_qc_' + cohort)
			else:
				cohort_mt = hail_utils.update_variant_qc(cohort_mt, is_female = "is_female", variant_qc = 'variant_qc_' + cohort, is_case = pheno_analyzed)
			mt = mt.annotate_rows(**{'variant_qc_' + cohort: cohort_mt.rows()[mt.row_key]['variant_qc_' + cohort]})
			if len(cohorts) > 1:
				mt = mt.annotate_rows(**{'variant_qc': mt['variant_qc'].annotate(max_cohort_maf = hl.cond( mt['variant_qc'].max_cohort_maf < mt['variant_qc_' + cohort].MAF, mt['variant_qc_' + cohort].MAF,  mt['variant_qc'].max_cohort_maf))})
			if args.cohort_filter:
				mt = hail_utils.add_filters(mt, [[x[1],x[2].replace('variant_qc','variant_qc_' + cohort),x[3].replace('variant_qc','variant_qc_' + cohort)] for x in args.cohort_filter], 'ls_filters_' + cohort)
				exclude_any_fields = exclude_any_fields + ['ls_filters_' + cohort + '.exclude']
			if args.knockout_filter:
				mt = hail_utils.add_filters(mt, [[x[1],x[2].replace('variant_qc','variant_qc_' + cohort),x[3].replace('variant_qc','variant_qc_' + cohort)] for x in args.knockout_filter], 'ls_knockouts_' + cohort)
				knockout_fields = knockout_fields + ['ls_knockouts_' + cohort + '.exclude']

	mask_fields = []
	if args.mask:
		masks = [x[0] for x in args.mask]
		for m in set(masks):
			masks_row = [x for x in args.mask if x[0] == m]
			mt = hail_utils.add_filters(mt, [[x[1],x[2],x[3]] for x in masks_row], 'ls_mask_' + m)
			mask_fields = mask_fields + ['ls_mask_' + m + '.exclude']

	mt = mt.annotate_rows(ls_global_exclude = 0)
	for f in exclude_any_fields:
		print("update global exclusion column based on " + f)
		ls_struct, ls_field = f.split(".")
		mt = mt.annotate_rows(ls_global_exclude = hl.cond(mt[ls_struct][ls_field] == 1, 1, mt.ls_global_exclude))
    
	if args.variants_stats_out:
		print("write variant metrics and filters to file")
		tbl = mt.rows().key_by('locus','alleles')
		tbl = tbl.drop("info","variant_qc_raw")
		tbl.flatten().export(args.variants_stats_out, header=True)

	n_all = mt.rows().count()
	if len(exclude_any_fields) > 0:
		print("filter variants that failed standard filters")
		mt = mt.filter_rows(mt.ls_global_exclude == 0, keep=True)
		n_post_filter = mt.rows().count()
		print(str(n_all - n_post_filter) + " variants flagged for removal via standard filters")
		print(str(n_post_filter) + " variants remaining for analysis")
	else:
		print(str(n_all) + " variants remaining for analysis")

	print("generate null group file")
	tbl = mt.rows().select(*['Gene'])
	tbl = tbl.annotate(groupfile_id = tbl.locus.contig + ":" + tbl.locus.pos + "_" + tbl.alleles[0] + "/" + tbl.alleles[1])
	tbl = tbl.key_by('Gene')
	tbl = tbl.select(*['groupfile_id'])
	tbl = tbl.collect_by_key()
	tbl = tbl.annotate(groupfile_id = tbl.groupfile_id.map(lambda x: "\t".join(x)))
	tbl.flatten().export(args.groupfile_out, header=True)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--annotation', help="a hail table containing annotations from vep")
	parser.add_argument('--filters', help='filter id, column name, expression; exclude variants satisfying this expression')
	parser.add_argument('--cohort-filters', help='cohort id, filter id, column name, expression; exclude variants satisfying this expression')
	parser.add_argument('--knockout-filters', help='cohort id, filter id, column name, expression; exclude variants satisfying this expression')
	parser.add_argument('--masks', help='mask id, column name, expression, groupfile; exclude variants satisfying this expression')
	parser.add_argument('--binary', action='store_true', default=False, help='flag indicates binary phenotype')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a matrix table', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--pheno-col', help='a column name for the phenotype', required=True)
	requiredArgs.add_argument('--cohorts-map-in', help='a cohorts map file', required=True)
	requiredArgs.add_argument('--variants-stats-out', help='a base filename for variant qc stats', required=True)
	requiredArgs.add_argument('--groupfile-out', help='an output file basename', required=True)
	args = parser.parse_args()
	main(args)
