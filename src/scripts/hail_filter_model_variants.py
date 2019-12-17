import hail as hl
import argparse
import pandas as pd
import csv
from pathlib import Path

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

	print("key rows by locus, alleles and rsid")
	mt = mt.key_rows_by('locus','alleles','rsid')

	if args.variants_remove is not None:
		print("flag variants for removal that failed previous qc steps")
		mt = mt.annotate_rows(ls_previous_exclude = 0)
		for variant_file in args.variants_remove.split(","):
			try:
				tbl = hl.import_table(variant_file, no_header=True, types={'f0': 'locus<' + args.reference_genome + '>', 'f1': 'array<str>', 'f2': 'str'}).key_by('f0', 'f1', 'f2')
			except:
				print("skipping empty file " + variant_file)
			else:
				mt = mt.annotate_rows(ls_previous_exclude = hl.cond(hl.is_defined(tbl[mt.row_key]), 1, mt.ls_previous_exclude))

	print("key rows by locus and alleles")
	mt = mt.key_rows_by('locus','alleles')

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
	filters = []
	cohort_filters = []
	knockout_filters = []
	masks = []
	if args.filters:
		with hl.hadoop_open(args.filters, 'r') as f:
			filters = f.read().splitlines()
			for x in filters:
				y = x.split("\t")
				if y not in filters:
					filters = filters + [y]
				filter_fields = filter_fields + [y[1]]
	if args.cohort_filters:
		with hl.hadoop_open(args.cohort_filters, 'r') as f:
			filters = f.read().splitlines()
			for x in filters:
				y = x.split("\t")
				if y not in cohort_filters:
					cohort_filters = cohort_filters + [y]
				cohorts = cohorts + [y[0]]
				filter_fields = filter_fields + [y[2]]
	if args.knockout_filters:
		with hl.hadoop_open(args.knockout_filters, 'r') as f:
			filters = f.read().splitlines()
			for x in filters:
				y = x.split("\t")
				if y not in knockout_filters:
					knockout_filters = knockout_filters + [y]
				cohorts = cohorts + [y[0]]
				filter_fields = filter_fields + [y[2]]
	if args.masks:
		with hl.hadoop_open(args.masks, 'r') as f:
			filters = f.read().splitlines()
			for x in filters:
				y = x.split("\t")
				if y not in masks:
					masks = masks + [y]
				filter_fields = filter_fields + [y[2]]

	cohorts = list(set(cohorts))
	filter_fields = list(set(filter_fields))
	
	annotation_fields = ['Uploaded_variation', 'Gene'] + [x.replace("annotation.","") for x in filter_fields if x.startswith("annotation.")]
	if args.annotation:
		print("add vep annotations")
		tbl = hl.read_table(args.annotation)
		tbl = tbl.select(*annotation_fields)
		tbl = tbl.key_by('Uploaded_variation')
		mt = mt.annotate_rows(annotation = tbl[mt.rsid])

	exclude_any_fields = []

	if len(filters) > 0:
		exclude_any_fields = exclude_any_fields + ['ls_filters.exclude']
		mt = hail_utils.add_filters(mt, filters, 'ls_filters')

	knockout_fields = []
	if len(cohorts) > 0:
		if len(cohorts) > 1:
			mt = mt.annotate_rows(**{'variant_qc': mt['variant_qc'].annotate(max_cohort_maf = 0)})
		for cohort in cohorts:
			print("calculate variant qc for cohort " + cohort)
			cohort_mt = mt.filter_cols(mt.COHORT == cohort)
			cohort_mt = hl.variant_qc(cohort_mt, name = 'variant_qc_' + cohort)
			if not args.binary:
				cohort_mt = hail_utils.update_variant_qc(cohort_mt, is_female = "is_female", variant_qc = 'variant_qc_' + cohort)
			else:
				cohort_mt = hail_utils.update_variant_qc(cohort_mt, is_female = "is_female", variant_qc = 'variant_qc_' + cohort, is_case = args.pheno_col)
			mt = mt.annotate_rows(**{'variant_qc_' + cohort: cohort_mt.rows()[mt.row_key]['variant_qc_' + cohort]})
			if len(cohorts) > 1:
				mt = mt.annotate_rows(**{'variant_qc': mt['variant_qc'].annotate(max_cohort_maf = hl.cond( mt['variant_qc'].max_cohort_maf < mt['variant_qc_' + cohort].MAF, mt['variant_qc_' + cohort].MAF,  mt['variant_qc'].max_cohort_maf))})
			if len(cohort_filters) > 0:
				mt = hail_utils.add_filters(mt, [[x[1],x[2].replace('variant_qc','variant_qc_' + cohort),x[3].replace('variant_qc','variant_qc_' + cohort)] for x in cohort_filters], 'ls_filters_' + cohort)
				exclude_any_fields = exclude_any_fields + ['ls_filters_' + cohort + '.exclude']
			if len(knockout_filters) > 0:
				mt = hail_utils.add_filters(mt, [[x[1],x[2].replace('variant_qc','variant_qc_' + cohort),x[3].replace('variant_qc','variant_qc_' + cohort)] for x in knockout_filters], 'ls_knockouts_' + cohort)
				knockout_fields = knockout_fields + ['ls_knockouts_' + cohort + '.exclude']

	mask_fields = []
	if len(masks) > 0:
		mask_ids = [x[0] for x in masks]
		for m in set(mask_ids):
			masks_row = [x for x in masks if x[0] == m]
			mt = hail_utils.add_filters(mt, [[x[1],x[2],x[3]] for x in masks_row], 'ls_mask_' + m)
			mask_fields = mask_fields + ['ls_mask_' + m + '.exclude']

	mt = mt.annotate_rows(ls_global_exclude = 0)
	mt = mt.annotate_rows(ls_global_exclude = hl.cond(mt.ls_previous_exclude == 1, 1, mt.ls_global_exclude))
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

	if args.groupfile_out:
		if n_all > 0:
			tbl = mt.rows().flatten()
			tbl = tbl.select(*['locus','alleles','annotation.Gene'])
			tbl = tbl.annotate(groupfile_id = tbl.locus.contig + ":" + hl.str(tbl.locus.position) + "_" + tbl.alleles[0] + "/" + tbl.alleles[1])
			tbl = tbl.key_by('annotation.Gene')
			tbl = tbl.select(*['groupfile_id'])
			tbl = tbl.collect_by_key()
			tbl = tbl.annotate(values=tbl.values.map(lambda x: x.groupfile_id))
			df = tbl.to_pandas()
			df = df.dropna()
			if df.shape[0] > 0:
				l = []
				for i, x in df.iterrows():                                    
					l = l + [x['annotation.Gene'] + "\t" + "\t".join(x['values'])]
				print("generate null group file with " + str(length(l)) + " genes")
				with open(args.groupfile_out, 'w') as f:
					f.write("\n".join(l))
			else:
				print("no genes found for remaining variants... writing empty group file")
				Path(args.groupfile_out).touch()      
		else:
			print("no variants remaining... writing empty group file")
			Path(args.groupfile_out).touch()

	if args.vcf_out:
		print("write VCF file to disk")
		hl.export_vcf(mt, args.vcf_out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--annotation', help="a hail table containing annotations from vep")
	parser.add_argument('--variants-remove', help="a comma separated list of files containing variants to remove")
	parser.add_argument('--filters', help='filter id, column name, expression; exclude variants satisfying this expression')
	parser.add_argument('--cohort-filters', help='cohort id, filter id, column name, expression; exclude variants satisfying this expression')
	parser.add_argument('--knockout-filters', help='cohort id, filter id, column name, expression; exclude variants satisfying this expression')
	parser.add_argument('--masks', help='mask id, column name, expression, groupfile; exclude variants satisfying this expression')
	parser.add_argument('--groupfile-out', help='an output file basename')
	parser.add_argument('--vcf-out', help='mask id, column name, expression, groupfile; exclude variants satisfying this expression')
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
	args = parser.parse_args()
	main(args)
