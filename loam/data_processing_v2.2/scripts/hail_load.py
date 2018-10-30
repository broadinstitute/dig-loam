import hail as hl
import argparse
hl.init()

def main(args=None):

	print("read vcf file")
	mt = hl.import_vcf(args.vcf_in[1], force_bgz=True, reference_genome=args.reference_genome, min_partitions=args.partitions)
	hl.summarize_variants(mt)

	print("split multiallelic variants")
	mt = hl.split_multi(mt)

	print("remove variants with single called allele")
	mt = hl.variant_qc(mt)
	mt = mt.filter_rows(mt.variant_qc.AN > 1, keep=True)

	print("assign family ID to match sample ID and add POP and GROUP")
	mt = mt.annotate_cols(famID = mt.s, POP = args.vcf_in[0], GROUP = args.vcf_in[0])

	if args.info_in:
		print("add imputation info scores")
		tbl = hl.import_table(args.info_in, types={'SNP': hl.tstr, 'REF(0)': hl.tstr, 'ALT(1)': hl.tstr, 'Rsq': hl.tstr})
		tbl = tbl.rename({'REF(0)' : 'REF', 'ALT(1)' : 'ALT', 'Rsq': 'raw_info_score'})
		tbl = tbl.annotate(locus = hl.parse_locus(tbl.SNP), alleles =  [tbl.REF, tbl.ALT])
		tbl = tbl.select(tbl.locus, tbl.alleles, tbl.raw_info_score)
		tbl = tbl.key_by('locus', 'alleles')
		tbl = tbl.filter(tbl.raw_info_score != "-")
		mt = mt.annotate_rows(raw_info_score = hl.float64(tbl[mt.locus, mt.alleles].raw_info_score))

	print('Samples: %d  Variants: %d' % (mt.count_cols(), mt.count_rows()))

	print("write matrix table to disk")
	mt.write(args.mt_out, overwrite=True)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--info-in', help='a compressed info file from Minimac3 (MI imputation server)')
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--partitions', type=int, default=100, help='number of partitions')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vcf-in', nargs=2, help='a dataset label followed by a compressed vcf file (eg: CAMP CAMP.vcf.gz)', required=True)
	requiredArgs.add_argument('--mt-out', help='a hail mt directory name for output', required=True)
	args = parser.parse_args()
	main(args)
