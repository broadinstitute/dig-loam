from hail import *
hc = HailContext()
import argparse

def main(args=None):

	print "reading vcf file"
	vds = hc.import_vcf(args.vcf_in[1], force_bgz=True, min_partitions=args.partitions)
	vds.summarize().report()

	print "splitting multiallelic variants and removing duplicates"
	vds = vds.split_multi().deduplicate()

	print "remove monomorphic variants"
	vds = vds.filter_variants_expr('v.nAlleles > 1', keep=True)
	vds.summarize().report()

	print "assigning family ID to match sample ID"
	vds = vds.annotate_samples_expr("sa.famID = s")

	print "adding sample annotations"
	vds = vds.annotate_samples_expr('sa.pheno.POP = "' + args.vcf_in[0] + '", sa.pheno.GROUP = "' + args.vcf_in[0] + '"')

	print "writing vds to disk"
	vds.write(args.vds_out, overwrite=True)
	vds.summarize().report()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--partitions', type=int, default=100, help='number of partitions')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vcf-in', nargs=2, help='a dataset label followed by a compressed vcf file (eg: CAMP CAMP.vcf.gz)', required=True)
	requiredArgs.add_argument('--vds-out', help='a hail vds directory name for output', required=True)
	args = parser.parse_args()
	main(args)
