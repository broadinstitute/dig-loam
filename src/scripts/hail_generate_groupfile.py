import hail as hl
import argparse
import pandas as pd
import csv
from pathlib import Path
import time
import re

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
		hl.init(log = args.log, tmp_dir = args.tmp_dir, idempotent=True)
	else:
		hl.init(idempotent=True)

	print("read filter table")
	ht = hl.read_table(args.filter_table_in)

	n_all = ht.count()
	n_exclude = ht.aggregate(hl.agg.count_where(ht.ls_global_exclude == 1))
	print(str(n_exclude) + " variants flagged for removal via standard filters")
	print(str(n_all - n_exclude) + " variants remaining for analysis")

	masked_groupfiles = {}
	if args.masked_groupfiles_out:
		for x in args.masked_groupfiles_out:
			masked_groupfiles[x.split(",")[0]] = x.split(",")[1]

	def aggregate_genes(ht: hl.Table):
		ht = ht.annotate(groupfile_id = ht.locus.contig + ":" + hl.str(ht.locus.position) + "_" + ht.alleles[0] + "/" + ht.alleles[1])
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
				l = l + [x['annotation.Gene'] + "\t" + "\t".join(map(lambda a: a[0] + ":" + a[1] + "_" + a[2] + "/" + a[3], sorted([re.split(":|_|/",y) for y in x['values']], key = lambda z: (int(z[1]), z[2], z[3]))))]
		return l

	ht = ht.flatten()
	if n_all - n_exclude > 0:
		ht = ht.filter((~ hl.is_missing(ht['annotation.Gene'])) & (ht.ls_global_exclude == 0), keep = True)
		if ht.count() > 0:
			htn = ht.select(*['locus','alleles','annotation.Gene'])
			print("aggregate null groups")
			groups = aggregate_genes(ht = htn)
			print("write null group file with " + str(len(groups)) + " genes")
			with hl.hadoop_open(args.groupfile_out, 'w') as f:
				f.writelines("%s\n" % group for group in groups)
			for m, f in masked_groupfiles.items():
				htm = ht.filter(ht['ls_mask_' + m + ".exclude"] == 0, keep = True)
				if htm.count() > 0:
					print("aggregate groups for mask " + m)
					groups = aggregate_genes(ht = htm)
					print("write group file for mask " + m + " with " + str(len(groups)) + " genes")
					with hl.hadoop_open(f, 'w') as f:
						f.writelines("%s\n" % group for group in groups)
				else:
					print("no variants remaining with mask " + m + " applied ... writing empty mask group file")
					Path(f).touch()
		else:
			print("no variants with non-missing gene annotations remaining ... writing empty null group file")
			Path(out).touch()
	else:
		print("no variants remaining after standard filters ... writing empty null group file")
		Path(out).touch()
		

	if args.cloud:
		hl.copy_log(args.log)

	global_elapsed_time = time.time() - global_start_time
	print(time.strftime("total time elapsed - %H:%M:%S", time.gmtime(global_elapsed_time)))


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--masked-groupfiles-out', nargs='+', help='a list of mask id and groupfile names each separated by a comma')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--tmp-dir', help='a temporary path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--filter-table-in', help='a hail table with variant filters and group (ie gene) assignments', required=True)
	requiredArgs.add_argument('--groupfile-out', help='a groupfile name', required=True)
	args = parser.parse_args()
	main(args)
