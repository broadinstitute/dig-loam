import hail as hl
import argparse
import pandas as pd
import csv
from pathlib import Path
import time

def main(args=None):

	global_start_time = time.time()

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

	print("read hail table")
	ht = hl.read_table(args.full_stats_in)

	print("key rows by locus, alleles and rsid")
	ht = ht.key_by('locus','alleles','rsid')

	if args.variants_remove is not None:
		print("flag variants for removal that failed previous qc steps")
		ht = ht.annotate(ls_previous_exclude = 0)
		for variant_file in args.variants_remove.split(","):
			try:
				tbl = hl.import_table(variant_file, no_header=True, types={'f0': 'locus<' + args.reference_genome + '>', 'f1': 'array<str>', 'f2': 'str'}).key_by('f0', 'f1', 'f2')
			except:
				print("skipping empty file " + variant_file)
			else:
				ht = ht.annotate(ls_previous_exclude = hl.cond(hl.is_defined(tbl[ht.key]), 1, ht.ls_previous_exclude))

	print("key rows by locus and alleles")
	ht = ht.key_by('locus','alleles')

	cohorts = []
	standard_filters = []
	cohort_filters = []
	knockout_filters = []
	masks = []
	variant_qc_fields = []
	variant_qc_cohort_fields = {}
	if args.filters:
		with hl.hadoop_open(args.filters, 'r') as f:
			filters = f.read().splitlines()
			for x in filters:
				y = x.split("\t")
				if y not in standard_filters:
					standard_filters = standard_filters + [y]
				variant_qc_fields = variant_qc_fields + y[1].split(",")
	if args.cohort_filters:
		with hl.hadoop_open(args.cohort_filters, 'r') as f:
			filters = f.read().splitlines()
			for x in filters:
				y = x.split("\t")
				if y not in cohort_filters:
					cohort_filters = cohort_filters + [y]
				cohorts = cohorts + [y[0]]
				if y[0] not in variant_qc_cohort_fields:
					variant_qc_cohort_fields[y[0]] = []
				variant_qc_cohort_fields[y[0]] = variant_qc_cohort_fields[y[0]] + y[1].split(",")
	if args.knockout_filters:
		with hl.hadoop_open(args.knockout_filters, 'r') as f:
			filters = f.read().splitlines()
			for x in filters:
				y = x.split("\t")
				if y not in knockout_filters:
					knockout_filters = knockout_filters + [y]
				cohorts = cohorts + [y[0]]
				if y[0] not in variant_qc_cohort_fields:
					variant_qc_cohort_fields[y[0]] = []
				variant_qc_cohort_fields[y[0]] = variant_qc_cohort_fields[y[0]] + y[1].split(",")
	if args.masks:
		with hl.hadoop_open(args.masks, 'r') as f:
			filters = f.read().splitlines()
			for x in filters:
				y = x.split("\t")
				if y not in masks:
					masks = masks + [y]
				variant_qc_fields = variant_qc_fields + y[2].split(",")
	cohorts = list(set(cohorts))
	variant_qc_fields = list(set(variant_qc_fields))

	annotation_fields = ['Uploaded_variation', 'Gene'] + [x.replace("annotation.","") for x in variant_qc_fields if x.startswith("annotation.")]
	for c in variant_qc_cohort_fields:
		variant_qc_cohort_fields[c] = list(set(variant_qc_cohort_fields[c]))
		annotation_fields = annotation_fields + [x for x in variant_qc_cohort_fields[c] if x.startswith("annotation.")]
	annotation_fields = list(set(annotation_fields))

	if args.annotation:
		print("read in vep annotations")
		tbl = hl.read_table(args.annotation)
		tbl = tbl.select(*annotation_fields)
		tbl = tbl.key_by('Uploaded_variation')
		ht = ht.annotate(annotation = tbl[ht.rsid])

	start_time = time.time()
	ht = ht.checkpoint(args.ht_checkpoint, overwrite=True)
	elapsed_time = time.time() - start_time
	print(time.strftime("write checkpoint hail table to disk - %H:%M:%S", time.gmtime(elapsed_time)))

	cohort_stats = {}
	if args.cohort_stats_in:
		for x in args.cohort_stats_in:
			cohort_stats[x.split(",")[0]] = x.split(",")[1]

	exclude_any_fields = []
	fields_out = ['rsid','annotation']

	# initialize max_cohort_maf
	if len(cohorts) > 1:
		start_time = time.time()
		ht = ht.annotate(max_cohort_maf = 0)
		fields_out = fields_out + ['max_cohort_maf']
		elapsed_time = time.time() - start_time
		print(time.strftime("initialize max_cohort_maf to 0 - %H:%M:%S", time.gmtime(elapsed_time)))

	# read in each cohort table, calculate max_cohort_maf, and add cohort_filters and knockout_filters
	knockout_fields = []
	if len(cohorts) > 0:
		for cohort in cohorts:
			start_time = time.time()
			cohort_ht = hl.read_table(cohort_stats[cohort])
			if len(cohort_filters) > 0:
				start_time = time.time()
				cohort_ht = hail_utils.ht_add_filters(cohort_ht, [[x[1],x[2],x[3]] for x in cohort_filters], 'ls_filters_' + cohort)
				exclude_any_fields = exclude_any_fields + ['ls_filters_' + cohort + '.exclude']
				elapsed_time = time.time() - start_time
				print(time.strftime("add cohort filters for cohort " + cohort + " - %H:%M:%S", time.gmtime(elapsed_time)))
			if len(knockout_filters) > 0:
				start_time = time.time()
				cohort_ht = hail_utils.ht_add_filters(cohort_ht, [[x[1],x[2],x[3]] for x in knockout_filters], 'ls_knockouts_' + cohort)
				knockout_fields = knockout_fields + ['ls_knockouts_' + cohort + '.exclude']
				elapsed_time = time.time() - start_time
				print(time.strftime("add knockout filters for cohort " + cohort + " - %H:%M:%S", time.gmtime(elapsed_time)))
			start_time = time.time()
			ht = ht.annotate(**{'variant_qc_' + cohort: cohort_ht[ht.key]['variant_qc'], 'ls_filters_' + cohort: cohort_ht[ht.key]['ls_filters_' + cohort], 'ls_knockouts_' + cohort: cohort_ht[ht.key]['ls_knockouts_' + cohort]})
			elapsed_time = time.time() - start_time
			print(time.strftime("annotate rows with cohort level variant qc in " + cohort + " - %H:%M:%S", time.gmtime(elapsed_time)))
			if len(cohorts) > 1:
				start_time = time.time()
				ht = ht.annotate(max_cohort_maf = hl.cond( ht.max_cohort_maf < ht['variant_qc_' + cohort].MAF, ht['variant_qc_' + cohort].MAF, ht.max_cohort_maf))
				elapsed_time = time.time() - start_time
				print(time.strftime("annotate rows with updated max_cohort_maf for cohort " + cohort + " - %H:%M:%S", time.gmtime(elapsed_time)))
			fields_out = fields_out + ['ls_filters_' + cohort, 'ls_knockouts_' + cohort]

	# add filters for full variant stats
	if len(standard_filters) > 0:
		exclude_any_fields = exclude_any_fields + ['ls_filters.exclude']
		ht = hail_utils.ht_add_filters(ht, standard_filters, 'ls_filters')
		fields_out = fields_out + ['ls_filters']

	mask_fields = []
	if len(masks) > 0:
		mask_ids = [x[0] for x in masks]
		for m in set(mask_ids):
			start_time = time.time()
			masks_row = [x for x in masks if x[0] == m]
			ht = hail_utils.ht_add_filters(ht, [[x[1],x[2],x[3]] for x in masks_row], 'ls_mask_' + m)
			mask_fields = mask_fields + ['ls_mask_' + m + '.exclude']
			fields_out = fields_out + ['ls_mask_' + m]
			elapsed_time = time.time() - start_time
			print(time.strftime("add mask " + m + " to filters - %H:%M:%S", time.gmtime(elapsed_time)))

	fields_out = fields_out + ['ls_previous_exclude','ls_global_exclude']
	ht = ht.annotate(ls_global_exclude = 0)
	ht = ht.annotate(ls_global_exclude = hl.cond(ht.ls_previous_exclude == 1, 1, ht.ls_global_exclude))
	for f in exclude_any_fields:
		start_time = time.time()
		ls_struct, ls_field = f.split(".")
		ht = ht.annotate(ls_global_exclude = hl.cond(ht[ls_struct][ls_field] == 1, 1, ht.ls_global_exclude))
		elapsed_time = time.time() - start_time
		print(time.strftime("update global exclusion column based on " + f + " - %H:%M:%S", time.gmtime(elapsed_time)))

	# select fields to keep from hail table and write to file
	ht = ht.select(*fields_out)
	if 'annotation' in fields_out:
		ht = ht.annotate(**{'annotation': ht.annotation.drop(*[x for x in annotation_fields if x not in ['Uploaded_variation','Gene']])})
	ht.write(args.variant_filters_ht_out, overwrite = True)

	if args.variant_filters_out:
		start_time = time.time()
		ht.flatten().export(args.variant_filters_out, header=True)
		elapsed_time = time.time() - start_time
		print(time.strftime("write variant filters to file - %H:%M:%S", time.gmtime(elapsed_time)))
    
	#n_all = ht.count()
	#if len(exclude_any_fields) > 0:
	#	start_time = time.time()
	#	ht = ht.filter(ht.ls_global_exclude == 1, keep=True)
	#	n_post_filter = ht.count()
	#	elapsed_time = time.time() - start_time
	#	print(time.strftime("filter variants that failed standard filters - %H:%M:%S", time.gmtime(elapsed_time)))
	#	print(str(n_post_filter) + " variants flagged for removal via standard filters")
	#	print(str(n_all - n_post_filter) + " variants remaining for analysis")
	#else:
	#	print("0 variants flagged for removal via standard filters")
	#	print(str(n_all) + " variants remaining for analysis")
    #
	#if args.groupfile_out:
	#	if n_all > 0:
	#		start_time = time.time()
	#		tbl = ht.flatten()
	#		tbl = tbl.select(*['locus','alleles','annotation.Gene'])
	#		tbl = tbl.annotate(groupfile_id = tbl.locus.contig + ":" + hl.str(tbl.locus.position) + "_" + tbl.alleles[0] + "/" + tbl.alleles[1])
	#		tbl = tbl.key_by('annotation.Gene')
	#		tbl = tbl.select(*['groupfile_id'])
	#		tbl = tbl.collect_by_key()
	#		tbl = tbl.annotate(values=tbl.values.map(lambda x: x.groupfile_id))
	#		df = tbl.to_pandas()
	#		df = df.dropna()
	#		elapsed_time = time.time() - start_time
	#		print(time.strftime("generate group file table - %H:%M:%S", time.gmtime(elapsed_time)))
	#		if df.shape[0] > 0:
	#			l = []
	#			for i, x in df.iterrows():                                    
	#				l = l + [x['annotation.Gene'] + "\t" + "\t".join(x['values'])]
	#			start_time = time.time()
	#			with hl.hadoop_open(args.groupfile_out, 'w') as f:
	#				f.write("\n".join(l))
	#			elapsed_time = time.time() - start_time
	#			print(time.strftime("generate null group file with " + str(len(l)) + " genes - %H:%M:%S", time.gmtime(elapsed_time)))
	#		else:
	#			print("no genes found for remaining variants... writing empty group file")
	#			Path(args.groupfile_out).touch()      
	#	else:
	#		print("no variants remaining... writing empty group file")
	#		Path(args.groupfile_out).touch()
    #
	#if args.vcf_out:
	#	start_time = time.time()
	#	hl.export_vcf(mt, args.vcf_out)
	#	elapsed_time = time.time() - start_time
	#	print(time.strftime("write VCF file to disk - %H:%M:%S", time.gmtime(elapsed_time)))
	
	if args.cloud:
		hl.copy_log(args.log)

	global_elapsed_time = time.time() - global_start_time
	print(time.strftime("total time elapsed - %H:%M:%S", time.gmtime(global_elapsed_time)))


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
	parser.add_argument('--variant-filters-out', help='a filename for variant filters')
	parser.add_argument('--cohort-stats-in', nargs='+', help='a list of cohort ids and hail tables with variant stats for each cohort, each separated by commas')
	#parser.add_argument('--groupfile-out', help='an output groupfile name')
	#parser.add_argument('--masked-groupfiles-out', nargs='+', help='a list of mask id and output groupfile name pairs, each separated by commas')
	#parser.add_argument('--vcf-out', help='mask id, column name, expression, groupfile; exclude variants satisfying this expression')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--full-stats-in', help='a hail table with variant stats on full sample set', required=True)
	requiredArgs.add_argument('--ht-checkpoint', help='a checkpoint filename', required=True)
	requiredArgs.add_argument('--variant-filters-ht-out', help='a filename for variant filters hail table', required=True)
	args = parser.parse_args()
	main(args)
