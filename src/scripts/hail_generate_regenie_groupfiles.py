import hail as hl
import argparse
import pandas as pd
import csv
from pathlib import Path
import time
import re

def write_empty_groupfiles(out):
	Path(out + ".regenie.annotations.tsv").touch()
	Path(out + ".regenie.setlist.tsv").touch()
	Path(out + ".regenie.masks.tsv").touch()

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

	print("read filter table")
	ht = hl.read_table(args.filter_table_in)

	n_all = ht.count()
	n_exclude = ht.aggregate(hl.agg.count_where(ht.ls_global_exclude == 1))
	print(str(n_exclude) + " variants flagged for removal via standard filters")
	print(str(n_all - n_exclude) + " variants remaining for analysis")

	def generate_setlist(ht: hl.Table):
		ht = ht.annotate(groupfile_id = ht.locus.contig + "><" + hl.str(ht.locus.position) + "><" + ht.alleles[0] + "><" + ht.alleles[1] + "><" + ht.rsid)
		ht = ht.key_by('annotation.Gene')
		ht = ht.select(*['groupfile_id'])
		ht = ht.collect_by_key()
		ht = ht.annotate(values=ht.values.map(lambda x: x.groupfile_id))
		df = ht.to_pandas()
		df = df.dropna()
		df = df.sort_values('annotation.Gene', ascending=True)
		l = []
		if df.shape[0] > 0:
			for i, x in df.iterrows():
				first_var = sorted([re.split("><",y) for y in x['values']], key = lambda z: (int(z[1]), z[2], z[3], z[4]))[0]
				l = l + [x['annotation.Gene'] + "\t" + str(first_var[0]) + "\t" + str(first_var[1]) + "\t" + ",".join(map(lambda a: a[4], sorted([re.split("><",y) for y in x['values']], key = lambda z: (int(z[1]), z[2], z[3], z[4]))))]
		return l

	def generate_annotations(ht: hl.Table):
		htm = ht.annotate(masks = "ALL")
		if args.masks:
			for m in args.masks.split(','):
				hh = ht.filter(ht['ls_mask_' + m + ".exclude"] == 0, keep = True)
				hh = hh.annotate(masks = m)
				htm = htm.union(hh)
		htm = htm.key_by(*['rsid','annotation.Gene'])
		htm = htm.select(*['masks'])
		htm = htm.collect_by_key()
		htm = htm.annotate(values=htm.values.map(lambda x: x.masks))
		df = htm.to_pandas()
		df = df.dropna()
		df = df.sort_values('annotation.Gene', ascending=True)
		l = []
		mask_list = []
		if df.shape[0] > 0:
			for i, x in df.iterrows():
				l = l + [x['rsid'] + "\t" + x['annotation.Gene'] + "\t" + ";".join(sorted(x['values']))]
				mask_list = mask_list + [";".join(sorted(x['values']))]
		return l, set(mask_list)

	ht = ht.flatten()
	if n_all - n_exclude > 0:
		ht = ht.filter((~ hl.is_missing(ht['annotation.Gene'])) & (ht.ls_global_exclude == 0), keep = True)
		if ht.count() > 0:
			htn = ht.select(*['locus','alleles','rsid','annotation.Gene'])
			print("aggregate setlist")
			sets = generate_setlist(ht = htn)
			print("write setlist with " + str(len(sets)) + " genes")
			with hl.hadoop_open(args.setlist_out, 'w') as f:
				f.writelines("%s\n" % set for set in sets)

			annots, mask_list = generate_annotations(ht = ht)
			with hl.hadoop_open(args.annotations_out, 'w') as f:
				f.writelines("%s\n" % annot for annot in annots)
			with hl.hadoop_open(args.masks_out, 'w') as f:
				if args.masks:
					for m in args.masks.split(','):
						mask_sets_used=[x for x in mask_list if m in x]
						if len(mask_sets_used) > 0:
							print("writing mask annotations used for mask " + m)
							f.write(m + "\t" + ",".join(mask_sets_used) + "\n")
						else:
							print("no annotations exist for mask " + m)
				else:
					f.write("ALL" + "\t" + "ALL" + "\n")
		else:
			print("no variants with non-missing gene annotations remaining ... writing empty null group file")
			write_empty_groupfiles(out)
	else:
		print("no variants remaining after standard filters ... writing empty null group file")
		write_empty_groupfiles(out)
		

	if args.cloud:
		hl.copy_log(args.log)

	global_elapsed_time = time.time() - global_start_time
	print(time.strftime("total time elapsed - %H:%M:%S", time.gmtime(global_elapsed_time)))


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--masks', help='a list of mask ids separated by a comma')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--filter-table-in', help='a hail table with variant filters and group (ie gene) assignments', required=True)
	requiredArgs.add_argument('--setlist-out', help='an output file basename for regenie setlist file', required=True)
	requiredArgs.add_argument('--masks-out', help='an output file basename for regenie masks file', required=True)
	requiredArgs.add_argument('--annotations-out', help='an output file basename for regenie annotations file', required=True)
	args = parser.parse_args()
	main(args)
