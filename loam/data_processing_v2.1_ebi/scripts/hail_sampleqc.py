from hail import *
hc = HailContext()
import argparse

def main(args=None):

	print "reading vds dataset"
	vds = hc.read(args.vds_in)
	vds.summarize().report()

	print "adding sample cluster annotations"
	cl_annotations = (hc.import_table(args.clusters_in,delimiter="\t",no_header=True).annotate('IID = f0').key_by('IID'))
	vds = vds.annotate_samples_table(cl_annotations, expr='sa.pheno.GROUP = table.f1')

	print "remove outlier samples"
	vds = vds.filter_samples_expr('sa.pheno.GROUP != "OUTLIERS"',keep=True)

	print "calculating sample qc stats"
	vds = vds.sample_qc()

	print "calculating variant qc stats"
	vds = vds.variant_qc()

	print "annotating sample qc stats"
	vds = vds.annotate_samples_expr("sa.qc.nHetLow = gs.filter(v => va.qc.AF < 0.03).filter(g => g.isHet).count(), sa.qc.nHetHigh = gs.filter(v => va.qc.AF >= 0.03).filter(g => g.isHet).count(), sa.qc.nCalledLow = gs.filter(v => va.qc.AF < 0.03).filter(g => g.isCalled).count(), sa.qc.nCalledHigh = gs.filter(v => va.qc.AF >= 0.03).filter(g => g.isCalled).count()")

	print "write sample qc stats results to file"
	vds.export_samples(args.qc_out, expr="IID = s, nNonRef = sa.qc.nNonRef, nHet = sa.qc.nHet, nCalled = sa.qc.nCalled, callRate = sa.qc.callRate, nSingleton = sa.qc.nSingleton, rTiTv = sa.qc.rTiTv, het = sa.qc.nHet / sa.qc.nCalled, hetLow = sa.qc.nHetLow / sa.qc.nCalledLow, hetHigh = sa.qc.nHetHigh / sa.qc.nCalledHigh, nHomVar = sa.qc.nHomVar, rHetHomVar = sa.qc.rHetHomVar")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vds-in', help='a hail vds dataset', required=True)
	requiredArgs.add_argument('--clusters-in', help='a tab delimited file consisting of sample IDs and their cluster assignment (eg: Sample1    EUR)', required=True)
	requiredArgs.add_argument('--qc-out', help='an output filename for sample qc statistics', required=True)
	args = parser.parse_args()
	main(args)
