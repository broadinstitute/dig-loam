from hail import *
hc = HailContext()
import argparse

def main(args=None):

	print "reading vcf file"
	vds = hc.import_vcf(args.vcf_in[1], force_bgz=True)
	vds.summarize().report()

	print "repartitioning vds dataset"
	vds = vds.repartition(200)

	print "splitting multiallelic variants and removing duplicates"
	vds = vds.split_multi().deduplicate()

	print "remove monomorphic variants"
	vds = vds.filter_variants_expr('v.nAlleles > 1', keep=True)
	vds.summarize().report()

	print "assigning family ID to match sample ID"
	vds = vds.annotate_samples_expr("sa.famID = s")

	print "adding sample annotations"
	vds = vds.annotate_samples_expr('sa.pheno.POP = "' + args.vcf_in[0] + '", sa.pheno.GROUP = "' + args.vcf_in[0] + '"')

	if args.info:
		print "adding imputation info"
		annot = hc.import_table(args.info, no_header=False, missing="NA", impute=True, types={"SNP": TString()}).annotate('ID = Variant(SNP + ":" + `REF(0)` + ":" + `ALT(1)`)').key_by("ID")
		vds = vds.annotate_variants_table(annot, root="va.imputedInfo")

	print "writing vds to disk"
	vds.write(args.vds_out, overwrite=True)
	vds.summarize().report()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--info', help='an imputation info file (see MI Imputation Server)')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vcf-in', nargs=2, help='a dataset label followed by a compressed vcf file (eg: CAMP CAMP.vcf.gz)', required=True)
	requiredArgs.add_argument('--vds-out', help='a hail vds directory name for output', required=True)
	args = parser.parse_args()
	main(args)
