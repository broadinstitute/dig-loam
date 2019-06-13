import hail as hl
import argparse

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log)
	else:
		hl.init()

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)
	hl.summarize_variants(mt)

	print("add sample cluster annotations")
	tbl = hl.import_table(args.clusters_in, delimiter="\t", no_header=True)
	tbl = tbl.annotate(IID = tbl.f0)
	tbl = tbl.key_by('IID')
	mt = mt.annotate_cols(GROUP = tbl[mt.s].f1)

	print("remove outlier samples")
	mt = mt.filter_cols(mt.GROUP == "OUTLIERS", keep=False)

	print("filter to only autosomal variants for sample qc")
	mt = mt.filter_rows(mt.locus.in_autosome())

	print("calculate sample qc stats")
	mt = hl.sample_qc(mt, name='sample_qc')

	print("calculate variant qc stats")
	mt = hl.variant_qc(mt, name='variant_qc')

	print("annotate sample qc stats")
	mt = mt.annotate_cols(sample_qc = mt.sample_qc.annotate(
		n_het_low = hl.agg.count_where((mt.variant_qc.AF[1] < 0.03) & mt.GT.is_het()), 
		n_het_high = hl.agg.count_where((mt.variant_qc.AF[1] >= 0.03) & mt.GT.is_het()), 
		n_called_low = hl.agg.count_where((mt.variant_qc.AF[1] < 0.03) & ~hl.is_missing(mt.GT)), 
		n_called_high = hl.agg.count_where((mt.variant_qc.AF[1] >= 0.03) & ~hl.is_missing(mt.GT)),
		avg_ab = hl.cond('AD' in list(mt.entry), hl.agg.mean(mt.AB), hl.null(hl.tfloat64)),
		avg_ab_dist50 = hl.cond('AD' in list(mt.entry), hl.agg.mean(mt.AB_dist50), hl.null(hl.tfloat64))
	))

	print("write sample qc stats results to file")
	tbl = mt.cols()
	tbl = tbl.rename({'s': 'IID'})
	tbl = tbl.select(
		n_non_ref = tbl.sample_qc.n_non_ref, 
		n_het = tbl.sample_qc.n_het, 
		n_called = tbl.sample_qc.n_called, 
		call_rate = tbl.sample_qc.call_rate, 
		n_singleton = tbl.sample_qc.n_singleton, 
		r_ti_tv = tbl.sample_qc.r_ti_tv, 
		het = tbl.sample_qc.n_het / tbl.sample_qc.n_called, 
		het_low = tbl.sample_qc.n_het_low / tbl.sample_qc.n_called_low, 
		het_high = tbl.sample_qc.n_het_high / tbl.sample_qc.n_called_high, 
		n_hom_var = tbl.sample_qc.n_hom_var, 
		r_het_hom_var = tbl.sample_qc.r_het_hom_var,
		avg_ab = tbl.sample_qc.avg_ab,
		avg_ab_dist50 = tbl.sample_qc.avg_ab_dist50)
	if not 'AD' in list(mt.entry):
		tbl = tbl.drop('avg_ab','avg_ab_dist50')
	tbl.flatten().export(args.qc_out)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--clusters-in', help='a tab delimited file consisting of sample IDs and their cluster assignment (eg: Sample1    EUR)', required=True)
	requiredArgs.add_argument('--qc-out', help='an output filename for sample qc statistics', required=True)
	args = parser.parse_args()
	main(args)
