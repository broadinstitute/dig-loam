import hail as hl
import argparse

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log)
	else:
		hl.init()

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)
	
	print("filter variants for QC")
	non_autosomal = [hl.parse_locus_interval(x) for x in hl.get_reference(args.reference_genome).mt_contigs + hl.get_reference(args.reference_genome).x_contigs + hl.get_reference(args.reference_genome).y_contigs]
	mt = hl.filter_intervals(mt, non_autosomal, keep=False)
	mt = mt.filter_rows(mt.variant_qc_raw.AN > 1, keep=True)
	mt = mt.filter_rows(hl.is_snp(mt.alleles[0], mt.alleles[1]), keep=True)
	mt = mt.filter_rows(~ hl.is_mnp(mt.alleles[0], mt.alleles[1]), keep=True)
	mt = mt.filter_rows(~ hl.is_indel(mt.alleles[0], mt.alleles[1]), keep=True)
	mt = mt.filter_rows(~ hl.is_complex(mt.alleles[0], mt.alleles[1]), keep=True)
	mt = mt.filter_rows(mt.variant_qc_raw.AF[1] >= args.filter_freq, keep=True)
	mt = mt.filter_rows(mt.variant_qc_raw.AF[1] <= 1 - args.filter_freq, keep=True)
	mt = mt.filter_rows(mt.variant_qc_raw.call_rate >= args.filter_callrate, keep=True)

	print("exclude regions with high LD")
	with hl.hadoop_open(args.regions_exclude, 'r') as f:
		hild = f.read().splitlines()
	mt = hl.filter_intervals(mt, [hl.parse_locus_interval(x) for x in hild], keep=False)

	n = mt.count()[0]
	if args.sample_n is not None:
		if n > args.sample_n:
			prop = args.sample_n / n
			print("downsampling variants by " + str(100*(1-prop)) + "%")
			mt = mt.sample_rows(p = prop, seed = args.sample_seed)
		else:
			print("skipping downsampling because " + str(n) + " <= " + str(args.sample_n))

	print("write variant table to file")
	mt.rows().flatten().export(args.variants_out, types_file=None)

	print("write Plink files to disk")
	hl.export_plink(mt, args.plink_out, ind_id = mt.s, fam_id = mt.s)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--sample-n', type=int, help='a probability for downsampling the variants included in qc data set (0.01 => 1% of variants are extracted)')
	parser.add_argument('--sample-seed', type=int, default=1, help='an integer used as a seed to allow for reproducibility in sampling variants')
	parser.add_argument('--filter-callrate', type=float, default=0.98, help='exclude variants with callrate below this number')
	parser.add_argument('--filter-freq', type=float, default=0.01, help='exclude variants with allele frequency lower than this number')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--regions-exclude', help='a list of Tabix formatted regions to exclude from QC', required=True)
	requiredArgs.add_argument('--variants-out', help='an output filename for pruned variant list', required=True)
	requiredArgs.add_argument('--plink-out', help='a pruned and filtered Plink dataset name', required=True)
	args = parser.parse_args()
	main(args)
