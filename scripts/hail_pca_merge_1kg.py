from hail import *
hc = HailContext()
import argparse

def main(args=None):

	print "reading vds dataset"
	vds = hc.read(args.vds_in)
	vds.summarize().report()

	print "adding sample cluster annotations"
	annotations = (hc.import_table(args.clusters_in,delimiter="\t",no_header=True).annotate('IID = f0').key_by('IID'))
	vds = vds.annotate_samples_table(annotations, expr='sa.pheno.GROUP = table.f1')

	print "remove outlier samples"
	vds = vds.filter_samples_expr('sa.pheno.GROUP != "OUTLIERS"',keep=True)

	#print "calculate PCA"
	#vds = vds.pca('sa.pca.scores','va.pca.loadings','global.pca.evals',k=10,as_array=False)
    #
	#print "write sample scores to file"
	#vds.export_samples(args.sample_scores_out, expr="IID = s, POP = sa.pheno.POP, GROUP = sa.pheno.GROUP, PC1 = sa.pca.scores.PC1, PC2 = sa.pca.scores.PC2, PC3 = sa.pca.scores.PC3, PC4 = sa.pca.scores.PC4, PC5 = sa.pca.scores.PC5, PC6 = sa.pca.scores.PC6, PC7 = sa.pca.scores.PC7, PC8 = sa.pca.scores.PC8, PC9 = sa.pca.scores.PC9, PC10 = sa.pca.scores.PC10")
    #
	#print "write variant loadings to file"
	#vds.export_variants(args.variant_loadings_out, expr="ID = v, PC1 = va.pca.loadings.PC1, PC2 = va.pca.loadings.PC2, PC3 = va.pca.loadings.PC3, PC4 = va.pca.loadings.PC4, PC5 = va.pca.loadings.PC5, PC6 = va.pca.loadings.PC6, PC7 = va.pca.loadings.PC7, PC8 = va.pca.loadings.PC8, PC9 = va.pca.loadings.PC9, PC10 = va.pca.loadings.PC10")

	print "writing Plink files for PC-AiR"
	vds.export_plink(args.plink_out, fam_expr='famID = s, id = s')

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vds-in', help='a hail vds dataset', required=True)
	requiredArgs.add_argument('--clusters-in', help='a tab delimited file consisting of sample IDs and their cluster assignment (eg: Sample1    EUR)', required=True)
	#requiredArgs.add_argument('--sample-scores-out', help='an output filename for sample PC scores', required=True)
	#requiredArgs.add_argument('--variant-loadings-out', help='an output filename for variant loadings', required=True)
	requiredArgs.add_argument('--plink-out', help='an output Plink fileset name to be read by PC-AiR', required=True)
	args = parser.parse_args()
	main(args)
