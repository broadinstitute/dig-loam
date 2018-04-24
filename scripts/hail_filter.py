from hail import *
hc = HailContext()
import argparse
import multiprocessing

def main(args=None):

	print "reading vds dataset"
	vds = hc.read(args.vds_in)
	vds.summarize().report()

	print "calculating variant qc metrics"
	vds = vds.variant_qc(root='va.qc')

	print "write variant qc metrics to file"
	vds.export_variants(args.variant_qc_out, expr="Chrom = v.contig, Pos = v.start, ID = v, RSID = va.rsid, Ref = v.ref, Alt = v.alt, va.qc.*", types=False)

	print "filtering variants for QC"
	vds = vds.filter_variants_expr('v.altAllele.isSNP && ! v.altAllele.isComplex && v.isAutosomal && ["A","C","G","T"].toSet.contains(v.altAllele.ref) && ["A","C","G","T"].toSet.contains(v.altAllele.alt) && va.qc.AF >= 0.01 && va.qc.callRate >= 0.98', keep=True)
	vds.summarize().report()

	print "excluding regions with high LD"
	exclude_regions_list = hc.import_table(args.regions_exclude, no_header=True, key='f0', types={'f0': TInterval()})
	vds = vds.filter_variants_table(exclude_regions_list, keep=False)
	vds.summarize().report()

	print "extracting pruned set of variants"
	vds = vds.ld_prune(num_cores=multiprocessing.cpu_count())
	vds.export_variants(args.variants_prunedin_out, expr="v")
	vds.summarize().report()

	print "writing filtered vds dataset"
	vds.write(args.filt_pruned_vds_out, overwrite=True)
	
	print "writing Plink files to disk"
	vds.export_plink(args.filt_pruned_plink_out, fam_expr='famID = s, id = s')

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vds-in', help='a hail vds dataset', required=True)
	requiredArgs.add_argument('--regions-exclude', help='a list of Tabix formatted regions to exclude from QC', required=True)
	requiredArgs.add_argument('--variant-qc-out', help='an output filename for variant QC filters', required=True)
	requiredArgs.add_argument('--variants-prunedin-out', help='an output filename for pruned variant list', required=True)
	requiredArgs.add_argument('--filt-pruned-vds-out', help='a pruned and filtered hail vds dataset name', required=True)
	requiredArgs.add_argument('--filt-pruned-plink-out', help='a pruned and filtered Plink dataset name', required=True)
	args = parser.parse_args()
	main(args)
