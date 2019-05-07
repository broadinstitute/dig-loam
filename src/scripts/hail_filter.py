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
	mt = mt.filter_rows(hl.is_snp(mt.alleles[0], mt.alleles[1]))
	mt = mt.filter_rows(~ hl.is_mnp(mt.alleles[0], mt.alleles[1]))
	mt = mt.filter_rows(~ hl.is_indel(mt.alleles[0], mt.alleles[1]))
	mt = mt.filter_rows(~ hl.is_complex(mt.alleles[0], mt.alleles[1]))
	mt = mt.filter_rows(mt.variant_qc.AF[1] >= 0.01)
	mt = mt.filter_rows(mt.variant_qc.AF[1] <= 0.99)
	mt = mt.filter_rows(mt.variant_qc.call_rate >= 0.98)

	print("exclude regions with high LD")
	with hl.hadoop_open(args.regions_exclude, 'r') as f:
		hild = f.read().splitlines()
	mt = hl.filter_intervals(mt, [hl.parse_locus_interval(x) for x in hild], keep=False)
	
	#print("write temporary filtered matrix table and read back in")
	#mt.write("temp.mt", overwrite=True)
	#mt = hl.read_matrix_table("temp.mt")

	#print("extract pruned set of variants")
	#pruned_tbl = hl.ld_prune(mt.GT, r2 = 0.2, bp_window_size = 1000000, memory_per_core = 256)
	#pruned_tbl.write("pruned_tbl.ht", overwrite=True)
	#pruned_tbl = hl.read_table('pruned_tbl.ht')
	#mt = mt.filter_rows(hl.is_defined(pruned_tbl[mt.row_key]))

	print("write variant qc metrics to file")
	mt.rows().flatten().export(args.variants_out, types_file=None)

	print("write Plink files to disk")
	hl.export_plink(mt, args.plink_out, ind_id = mt.s, fam_id = mt.s)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--regions-exclude', help='a list of Tabix formatted regions to exclude from QC', required=True)
	requiredArgs.add_argument('--variants-out', help='an output filename for pruned variant list', required=True)
	requiredArgs.add_argument('--plink-out', help='a pruned and filtered Plink dataset name', required=True)
	args = parser.parse_args()
	main(args)
