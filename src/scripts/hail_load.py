import hail as hl
import argparse

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log)
	else:
		hl.init()

	print("read vcf file")
	mt = hl.import_vcf(args.vcf_in[1], force_bgz=True, reference_genome=args.reference_genome, min_partitions=args.partitions, array_elements_required=False)

	print("replace any spaces in sample ids with an underscore")
	mt = mt.annotate_cols(s_new = mt.s.replace("\s+","_"))
	mt = mt.key_cols_by('s_new')
	mt = mt.drop('s')
	mt = mt.rename({'s_new': 's'})

	print("add pheno annotations, replacing any spaces in sample ids with an underscore")
	tbl = hl.import_table(args.pheno_in, delimiter="\t", no_header=False, types={args.id_col: hl.tstr})
	tbl = tbl.annotate(**{args.id_col + '_new': tbl[args.id_col].replace("\s+","_")})
	tbl = tbl.key_by(args.id_col + '_new')
	tbl = tbl.drop(args.id_col)
	tbl = tbl.rename({args.id_col + '_new': args.id_col})
	mt = mt.annotate_cols(pheno = tbl[mt.s])

	print("split multiallelic variants")
	mt_multi = mt.filter_rows(hl.len(mt.alleles) > 2)
	mt_multi = hl.split_multi_hts(mt_multi)
	print("split multiallelic variants")
	mt_multi.describe()
	mt_bi = mt.filter_rows(hl.len(mt.alleles) <= 2)
	mt_bi = mt_bi.annotate_rows(a_index = 1, was_split = False)
	mt_bi.describe()
	mt = mt_bi.union_rows(mt_multi)
	mt.describe()

	print("calculate variant qc metrics")
	mt = hl.variant_qc(mt)

	print("write variant qc metrics to file")
	mt.rows().flatten().export(args.variant_metrics_out, types_file=None)

	print("remove variants with single called allele")
	mt = mt.filter_rows(mt.variant_qc.AN > 1, keep=True)

	print("assign family ID to match sample ID and add POP and GROUP")
	mt = mt.annotate_cols(famID = mt.s, POP = args.vcf_in[0], GROUP = args.vcf_in[0])

	print("write matrix table to disk")
	mt.write(args.mt_out, overwrite=True)

	print("write site vcf file")
	mt_sites= mt.select_cols()
	mt_sites = mt_sites.filter_cols(False)
	mt_sites = mt_sites.select_rows('rsid','qual','filters','info')
	hl.export_vcf(mt_sites,args.sites_vcf_out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--partitions', type=int, default=100, help='number of partitions')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--vcf-in', nargs=2, help='a dataset label followed by a compressed vcf file (eg: CAMP CAMP.vcf.gz)', required=True)
	requiredArgs.add_argument('--pheno-in', help='a tab delimited phenotype file', required=True)
	requiredArgs.add_argument('--id-col', help='a column name for sample id in the phenotype file', required=True)
	requiredArgs.add_argument('--variant-metrics-out', help='an output filename for variant qc metrics', required=True)
	requiredArgs.add_argument('--sites-vcf-out', help='an output filename for a sites only VCF file (must end in .vcf)', required=True)
	requiredArgs.add_argument('--mt-out', help='a hail mt directory name for output', required=True)
	args = parser.parse_args()
	main(args)
