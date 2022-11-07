import hail as hl
import argparse
import pandas as pd

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log, tmp_dir = args.tmp_dir, idempotent=True)
	else:
		hl.init(idempotent=True)

	print("import annotations")
	ht = hl.import_table(args.annotations, impute=True, no_header=False, delimiter='\t', missing='-', min_partitions=args.min_partitions)
	ht = ht.rename({'#Uploaded_variation': 'Uploaded_variation'})
	ht.describe()

	print("starting with " + str(ht.count()) + " annotations")
	ht = ht.filter(ht.PICK == 1, keep = True)
	print("ending with " + str(ht.count()) + " PICK == 1 annotations")

	for field in ['DEOGEN2_score','FATHMM_score','LIST-S2_score','MPC_score','MVP_score','MetaRNN_score','MutationAssessor_score','PROVEAN_score','Polyphen2_HDIV_score','Polyphen2_HVAR_score','REVEL_score','SIFT4G_score','SIFT_score','VEST4_score']:
		if field in ht.row_value:
			ht = ht.annotate(**{
				field + '_max': hl.if_else(hl.is_missing(ht[field]), hl.missing(hl.tfloat64), hl.if_else(ht[field].split(",").filter(lambda x: x != '.').length() > 0, hl.max(ht[field].split(",").map(lambda x: hl.if_else(x != ".", hl.float(x), hl.missing(hl.tfloat64)))), hl.missing(hl.tfloat64))),
				field + '_min': hl.if_else(hl.is_missing(ht[field]), hl.missing(hl.tfloat64), hl.if_else(ht[field].split(",").filter(lambda x: x != '.').length() > 0, hl.min(ht[field].split(",").map(lambda x: hl.if_else(x != ".", hl.float(x), hl.missing(hl.tfloat64)))), hl.missing(hl.tfloat64))),
				field + '_mean': hl.if_else(hl.is_missing(ht[field]), hl.missing(hl.tfloat64), hl.if_else(ht[field].split(",").filter(lambda x: x != '.').length() > 0, hl.mean(ht[field].split(",").map(lambda x: hl.if_else(x != ".", hl.float(x), hl.missing(hl.tfloat64)))), hl.missing(hl.tfloat64)))
			})
    
	print("writing hail table to disk")
	ht.write(args.out, overwrite = True)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--min-partitions', type=int, default=100, help='number of min partitions')
	parser.add_argument('--tmp-dir', help='a temporary path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--annotations', help='an annotation file', required=True)
	requiredArgs.add_argument('--out', help='an output hail table name', required=True)
	args = parser.parse_args()
	main(args)
