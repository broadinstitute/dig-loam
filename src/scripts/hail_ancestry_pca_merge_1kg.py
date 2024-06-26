import hail as hl
import argparse
import os
import tempfile

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

	print("making temporary directory for storing checkpoints")
	if args.tmp_dir and not args.cloud:
		tmpdir = tempfile.TemporaryDirectory(dir = args.tmp_dir)
	else:
		tmpdir = tempfile.TemporaryDirectory(dir = "./")

	if not args.cloud:
		os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-memory ' + args.driver_memory + ' --executor-memory ' + args.executor_memory + ' pyspark-shell'
		os.environ["SPARK_LOCAL_DIRS"] = tmpdir.name
		hl.init(log = args.log, tmp_dir = tmpdir.name, local_tmpdir = tmpdir.name, idempotent=True)
	else:
		hl.init(idempotent=True)

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)

	print("filter to only non-vcf-filtered, well-called, non-monomorphic variants")
	mt = mt.filter_rows((hl.is_missing(hl.len(mt.filters)) | (hl.len(mt.filters) == 0)) & (mt.variant_qc_raw.AN > 1) & (mt.variant_qc_raw.AF > 0) & (mt.variant_qc_raw.AF < 1), keep=True)

	print("read kg vcf file")
	if args.reference_genome == 'GRCh38':
		recode = {f"{i}":f"chr{i}" for i in (list(range(1, 23)) + ['X', 'Y'])}
		recode['MT'] = 'chrM'
	else:
		recode = None
	kg = hl.import_vcf(args.kg_vcf_in, force_bgz=True, reference_genome=args.reference_genome, contig_recoding=recode)

	print("split multiallelic variants in kg data")
	kg = hl.split_multi_hts(kg)

	print("add POP and GROUP to mt")
	mt = mt.annotate_cols(POP = "NA", GROUP = "NA")

	print("add 1KG sample annotations")
	kg = kg.annotate_cols(famID = kg.s)
	tbl = hl.import_table(args.kg_sample, delimiter="\s+", impute=True)
	tbl = tbl.select(args.kg_sample_id, args.kg_sample_pop, args.kg_sample_group)
	tbl = tbl.key_by(args.kg_sample_id)
	kg = kg.annotate_cols(POP = tbl[kg.s][args.kg_sample_pop], GROUP = tbl[kg.s][args.kg_sample_group])

	print("rename sample IDs to avoid any possible overlap with test data")
	kg_rename = [x for x in kg.s.collect()]
	kg = kg.annotate_cols(col_ids = hl.literal(dict(zip(kg_rename, ["kg-" + x for x in kg_rename])))[kg.s])
	kg = kg.key_cols_by(kg.col_ids)
	kg = kg.drop('s')
	kg = kg.rename({'col_ids': 's'})
	kg = kg.select_cols('POP', 'GROUP')

	mt = mt.select_entries(mt.GT)
	kg = kg.select_entries(kg.GT)

	print("convert kg genotypes to unphased")
	kg = hail_utils.unphase_genotypes(kg)

	print("drop extraneous rows from both matrix tables")
	row_fields_keep = ['locus', 'alleles', 'rsid', 'qual', 'filters', 'a_index', 'was_split']
	mt_remove = [x for x in list(mt.row) if x not in row_fields_keep]
	kg_remove = [x for x in list(kg.row) if x not in row_fields_keep]
	for f in mt_remove:
		mt = mt.drop(f)
	for f in kg_remove:
		kg = kg.drop(f)

	print("drop extraneous columns from both matrix tables")
	col_fields_keep = ['s', 'POP', 'GROUP']
	mt_remove = [x for x in list(mt.col) if x not in col_fields_keep]
	kg_remove = [x for x in list(kg.col) if x not in col_fields_keep]
	for f in mt_remove:
		mt = mt.drop(f)
	for f in kg_remove:
		kg = kg.drop(f)

	print('extract AFR, AMR, EUR, EAS, and SAS samples from kg data')
	kg = kg.filter_cols(hl.set(['AFR','AMR','EUR','EAS','SAS']).contains(kg.GROUP), keep=True)

	print('study data: %d samples and %d variants' % (mt.count_cols(), mt.count_rows()))
	print('kg data: %d samples and %d variants' % (kg.count_cols(), kg.count_rows()))

	print("join matrix tables on columns with inner join on rows")
	mt = mt.union_cols(kg)
	hl.summarize_variants(mt)
	print('merged data: %d samples and %d variants' % (mt.count_cols(), mt.count_rows()))

	print("write Plink files to disk")
	hl.export_plink(mt, args.plink_out, ind_id = mt.s, fam_id = mt.s, pheno = -9)

	print("write ref sample table to file")
	kg.rename({'s': 'ID'}).cols().export(args.kg_samples_out, header=True, types_file=None)

	if args.cloud:
		hl.copy_log(args.log)

	tmpdir.cleanup()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--driver-memory', default="1g", help='spark driver memory')
	parser.add_argument('--executor-memory', default="1g", help='spark executor memory')
	parser.add_argument('--tmp-dir', help='a temporary path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--kg-vcf-in', help='a vcf file consisting of ~5k 1000 Genomes variants (selected by Purcell for ancestry)', required=True)
	requiredArgs.add_argument('--kg-sample', help='a 1KG sample file', required=True)
	requiredArgs.add_argument('--kg-sample-id', help='a 1KG sample file sample ID column name', required=True)
	requiredArgs.add_argument('--kg-sample-pop', help='a 1KG sample file sample POP column name', required=True)
	requiredArgs.add_argument('--kg-sample-group', help='a 1KG sample file sample GROUP column name', required=True)
	requiredArgs.add_argument('--plink-out', help='an output plink filename', required=True)
	requiredArgs.add_argument('--kg-samples-out', help='an output filename for kg samples that were merged', required=True)
	args = parser.parse_args()
	main(args)
