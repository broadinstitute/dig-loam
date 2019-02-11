import hail as hl
hl.init()
import argparse

def main(args=None):

	if args.vcf_in:
		print("read vcf file")
		mt = hl.import_vcf(args.vcf_in[1], force_bgz=True, reference_genome=args.reference_genome, min_partitions=args.partitions)
		meta = hl.get_vcf_metadata(args.vcf_in[1])
	else:
		print("read plink file")
		mt = hl.import_plink(args.plink_in[1], args.plink_in[2], args.plink_in[3], reference_genome=args.reference_genome, min_partitions = args.partitions)
		meta = None

	print("split multiallelic variants")
	mt = hl.split_multi(mt)

	print("remove variants with single called allele")
	mt = hl.variant_qc(mt)
	mt = mt.filter_rows(mt.variant_qc.AN > 1, keep=True)

	print("assign family ID to match sample ID and add POP and GROUP")
	mt = mt.annotate_cols(famID = mt.s, POP = args.vcf_in[0], GROUP = args.vcf_in[0])

	print("write matrix table to disk")
	mt.write(args.mt, overwrite=True)

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt)

	print("write sites summary to disk")
	tbl = mt.rows()
	tbl.select('rsid','a_index','was_split','old_locus','old_alleles').export(args.sites_summary)

	print("write sites vcf to disk")
	mt = mt.filter_cols(False)
	hl.export_vcf(mt, args.sites_vcf, metadata = meta)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', help='a reference genome (default = "GRCh37")', default="GRCh37")
	parser.add_argument('--partitions', type=int, help='number of partitions (default = 200)', default=200)
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--mt', help='a hail mt directory name for output', required=True)
	requiredArgs.add_argument('--sites-summary', help='a variant type table name', required=True)
	requiredArgs.add_argument('--sites-vcf', help='a site vcf file name', required=True)
	group = parser.add_mutually_exclusive_group(required=True)
	group.add_argument('--vcf-in', nargs=2, help='a dataset label followed by a compressed vcf file (eg: CAMP CAMP.vcf.gz)')
	group.add_argument('--plink-in', nargs=4, help='a dataset label followed by a plink fileset (eg: CAMP CAMP.bed CAMP.bim CAMP.fam)')
	args = parser.parse_args()
	main(args)
