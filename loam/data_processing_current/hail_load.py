import hail as hl
hl.init()
import argparse

def main(args=None):

	if args.vcf_in:
		print("reading vcf file")
		mt = hl.import_vcf(args.vcf_in[1], force_bgz=True, reference_genome=args.build, min_partitions = args.partitions)
	else:
		print("reading plink file")
		mt = hl.import_plink(args.plink_in[1], args.plink_in[2], args.plink_in[3], reference_genome=args.build, min_partitions = args.partitions)

	hl.summarize_variants(mt)
	print("splitting multiallelic variants and removing duplicates")
	mt = hl.split_multi(mt)
	mt.row.describe()
	mt.col.describe()
	hl.summarize_variants(mt)

	Rows = mt.rows()
	# can change order if add field name for each, otherwise can reorder by calling select again and using only field names
	Rows.select('rsid', ref=Rows.alleles[0], 'qual', 'filters', 'info').export(args.sites_vcf)
	Rows.select('rsid', 'was_split', 'a_index', 'old_locus', 'old_alleles').export(args.sites_summary)

	#print "remove monomorphic variants"
	#vds = vds.filter_variants_expr('v.nAlleles > 1', keep=True)
	#vds.summarize().report()
	#
	#print "assigning family ID to match sample ID"
	#vds = vds.annotate_samples_expr("sa.famID = s")
	#
	#print "adding sample annotations"
	#vds = vds.annotate_samples_expr('sa.pheno.POP = "' + args.vcf_in[0] + '", sa.pheno.GROUP = "' + args.vcf_in[0] + '"')
	#
	#if args.info:
	#	print "adding imputation info"
	#	annot = hl.import_table(args.info, no_header=False, missing="NA", impute=True, types={"SNP": TString()}).annotate('ID = Variant(SNP + ":" + `REF(0)` + ":" + `ALT(1)`)').key_by("ID")
	#	vds = vds.annotate_variants_table(annot, root="va.imputedInfo")

	print("writing vds to disk")
	mt.write(args.mt, overwrite=True)

	print("writing site vcf to disk")
	

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	#parser.add_argument('--info', help='an imputation info file (see MI Imputation Server)')
	parser.add_argument('--build', help='a reference genome build (default = "GRCh37")', default="GRCh37")
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
