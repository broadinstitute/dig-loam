import hail as hl
import argparse
hl.init()

def main(args=None):

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)
	hl.summarize_variants(mt)
	
	if not 'variant_qc' in mt.rows().globals:
		print("calculate variant qc metrics")
		mt = hl.variant_qc(mt)
    
	print("write variant qc metrics to file")
	rows = mt.rows().flatten()
	rows.export(args.variant_qc_out)
    
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
	hl.summarize_variants(mt)

	print("exclude regions with high LD")
	with hl.hadoop_open(args.regions_exclude, 'r') as f:
		hild = f.read().splitlines()
	mt = hl.filter_intervals(mt, [hl.parse_locus_interval(x) for x in hild], keep=False)
	hl.summarize_variants(mt)

	print("extract pruned set of variants")
	pruned_tbl = hl.ld_prune(mt.GT, r2 = 0.2, bp_window_size = 1000000, memory_per_core = 1000)
	pruned_tbl.export(args.variants_prunedin_out)
	mt = mt.filter_rows(hl.is_defined(pruned_tbl[mt.row_key]))
	hl.summarize_variants(mt)

	print("write filtered matrix table")
	mt.write(args.filt_pruned_mt_out, overwrite=True)
	
	print("write Plink files to disk")
	hl.export_plink(mt, args.filt_pruned_plink_out, call = hl.call(1, 0, phased=False), ind_id = mt.s, fam_id = mt.s)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--regions-exclude', help='a list of Tabix formatted regions to exclude from QC', required=True)
	requiredArgs.add_argument('--variant-qc-out', help='an output filename for variant QC filters', required=True)
	requiredArgs.add_argument('--variants-prunedin-out', help='an output filename for pruned variant list', required=True)
	requiredArgs.add_argument('--filt-pruned-mt-out', help='a pruned and filtered hail mt dataset name', required=True)
	requiredArgs.add_argument('--filt-pruned-plink-out', help='a pruned and filtered Plink dataset name', required=True)
	args = parser.parse_args()
	main(args)
