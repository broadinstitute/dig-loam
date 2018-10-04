import hail as hl
import argparse
hl.init()

def main(args=None):

	print("read matrix table")
	mt = hl.read_matrix_table(args.mt_in)
	hl.summarize_variants(mt)

	print("read kg vcf file")
	kg = hl.import_vcf(args.kg_vcf_in, force_bgz=True, reference_genome=args.reference_genome)
	hl.summarize_variants(kg)

	print("split multiallelic variants in kg data")
	kg = hl.split_multi(kg)

	print("add 1KG sample annotations")
	kg = kg.annotate_cols(famID = kg.s)
	tbl = hl.import_table(args.kg_sample, delimiter=" ", impute=True)
	tbl = tbl.select(tbl.ID, tbl.POP, tbl.GROUP)
	tbl = tbl.key_by(tbl.ID)
	kg = kg.annotate_cols(POP = tbl[kg.s].POP, GROUP = tbl[kg.s].GROUP)

	mt = mt.drop(mt.DS, mt.GP, mt.info, mt.variant_qc)
	kg = kg.drop(kg.info)
	print('study data: %d samples and %d variants' % (mt.count_cols(), mt.count_rows()))
	print('kg data: %d samples and %d variants' % (kg.count_cols(), kg.count_rows()))

	print("join matrix tables on columns with inner join on rows")
	mt = mt.union_cols(kg)
	hl.summarize_variants(mt)
	print('merged data: %d samples and %d variants' % (mt.count_cols(), mt.count_rows()))
    
	print("write merged plink files")
	hl.export_plink(mt, args.plink_out, call = hl.call(1, 0, phased=False), ind_id = mt.s, fam_id = mt.s)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--mt-in', help='a hail matrix table', required=True)
	requiredArgs.add_argument('--kg-vcf-in', help='a vcf file consisting of ~5k 1000 Genomes variants (selected by Purcell for ancestry)', required=True)
	requiredArgs.add_argument('--kg-sample', help='a 1KG sample file (header: ID POP GROUP SEX)', required=False)
	requiredArgs.add_argument('--plink-out', help='an output Plink fileset name to be read by PC-AiR', required=True)
	args = parser.parse_args()
	main(args)
