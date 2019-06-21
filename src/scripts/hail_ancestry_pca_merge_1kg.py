import hail as hl
import argparse
import hail_utils

def main(args=None):

	if not args.cloud:
		hl.init(log = args.log)
	else:
		hl.init()

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)

	print("filter to only non-vcf-filtered, well-called, non-monomorphic variants")
	mt = mt.filter_rows((hl.len(mt.filters) == 0) & (hl.len(mt.filters) == 0) & (mt.variant_qc_raw.AN > 1) & (mt.variant_qc_raw.AF[1] > 0) & (mt.variant_qc_raw.AF[1] < 1), keep=True)

	print("read kg vcf file")
	kg = hl.import_vcf(args.kg_vcf_in, force_bgz=True, reference_genome=args.reference_genome)

	print("split multiallelic variants in kg data")
	kg = hl.split_multi_hts(kg)

	print("add POP and GROUP to mt")
	mt = mt.annotate_cols(POP = "NA", GROUP = "NA")

	print("add 1KG sample annotations")
	kg = kg.annotate_cols(famID = kg.s)
	tbl = hl.import_table(args.kg_sample, delimiter=" ", impute=True)
	tbl = tbl.select(tbl.ID, tbl.POP, tbl.GROUP)
	tbl = tbl.key_by(tbl.ID)
	kg = kg.annotate_cols(POP = tbl[kg.s].POP, GROUP = tbl[kg.s].GROUP)

	print("rename sample IDs to avoid any possible overlap with test data")
	kg_rename = [x for x in kg.s.collect()]
	kg = kg.annotate_cols(col_ids = hl.literal(dict(zip(kg_rename, ["kg-" + x for x in kg_rename])))[kg.s])
	kg = kg.key_cols_by(kg.col_ids)
	kg = kg.drop('s')
	kg = kg.rename({'col_ids': 's'})
	kg = kg.select_cols('POP', 'GROUP')

	mt = mt.select_entries(mt.GT)
	kg = kg.select_entries(kg.GT)

	print("convert kg genotypes to unphased")
	kg = hail_utils.unphase_genotypes(kg)

	print("drop extraneous rows from both matrix tables")
	row_fields_keep = ['locus', 'alleles', 'rsid', 'qual', 'filters', 'a_index', 'was_split']
	mt_remove = [x for x in list(mt.rows().row) if x not in row_fields_keep]
	kg_remove = [x for x in list(kg.rows().row) if x not in row_fields_keep]
	for f in mt_remove:
		mt = mt.drop(f)
	for f in kg_remove:
		kg = kg.drop(f)

	print("drop pheno struct from mt")
	mt = mt.drop('pheno')

	print('study data: %d samples and %d variants' % (mt.count_cols(), mt.count_rows()))
	print('kg data: %d samples and %d variants' % (kg.count_cols(), kg.count_rows()))

	print("join matrix tables on columns with inner join on rows")
	mt = mt.union_cols(kg)
	hl.summarize_variants(mt)
	print('merged data: %d samples and %d variants' % (mt.count_cols(), mt.count_rows()))

	print("write Plink files to disk")
	hl.export_plink(mt, args.plink_out, ind_id = mt.s, fam_id = mt.s)

	print("write ref sample table to file")
	kg.rename({'s': 'ID'}).cols().export(args.kg_samples_out, header=True, types_file=None)

	if args.cloud:
		hl.copy_log(args.log)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--kg-vcf-in', help='a vcf file consisting of ~5k 1000 Genomes variants (selected by Purcell for ancestry)', required=True)
	requiredArgs.add_argument('--kg-sample', help='a 1KG sample file (header: ID POP GROUP SEX)', required=False)
	requiredArgs.add_argument('--plink-out', help='an output plink filename', required=True)
	requiredArgs.add_argument('--kg-samples-out', help='an output filename for kg samples that were merged', required=True)
	args = parser.parse_args()
	main(args)
