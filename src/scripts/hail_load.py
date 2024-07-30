import hail as hl
import tempfile
import argparse
import os
import tempfile

def main(args=None):

	if args.hail_utils:
		import importlib.util
		with hl.hadoop_open(args.hail_utils, 'r') as f:
			script = f.read()
		with open("hail_utils.py", 'w') as f:
			f.write(script)
		spec = importlib.util.spec_from_file_location('hail_utils', 'hail_utils.py')
		hail_utils = importlib.util.module_from_spec(spec)   
		spec.loader.exec_module(hail_utils)
	else:
		import hail_utils

	print("making temporary directory for storing checkpoints")
	if args.tmp_dir and not args.cloud:
		tmpdir = tempfile.TemporaryDirectory(dir = args.tmp_dir)
	else:
		tmpdir = tempfile.TemporaryDirectory(dir = "./")
	tmpdir_path = tmpdir.name + "/"

	if not args.cloud:
		os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-memory ' + args.driver_memory + ' --executor-memory ' + args.executor_memory + ' pyspark-shell'
		os.environ["SPARK_LOCAL_DIRS"] = tmpdir.name
		hl.init(log = args.log, tmp_dir = tmpdir.name, local_tmpdir = tmpdir.name, idempotent=True)
	else:
		hl.init(idempotent=True)

	if args.vcf_in:
		print("read vcf file")
		if args.reference_genome == 'GRCh38':
			recode = {f"{i}":f"chr{i}" for i in (list(range(1, 23)) + ['X', 'Y'])}
			recode['MT'] = 'chrM'
		else:
			recode = None
		mt = hl.import_vcf(args.vcf_in.split(","), force_bgz=True, reference_genome=args.reference_genome, min_partitions=args.min_partitions, array_elements_required=False, contig_recoding=recode)
	elif args.plink_in:
		print("read plink file")
		mt = hl.import_plink(bed = args.plink_in + ".bed", bim = args.plink_in + ".bim", fam = args.plink_in + ".fam", reference_genome=args.reference_genome, min_partitions=args.min_partitions, a2_reference=True, quant_pheno=True, missing='-9')
		mt = mt.filter_rows((mt.alleles[0] == ".") | (mt.alleles[1] == "."), keep=False)
		mt = mt.drop(mt.fam_id, mt.pat_id, mt.mat_id, mt.is_female, mt.quant_pheno)
		mt = mt.annotate_rows(qual = hl.missing(hl.tfloat64), filters = hl.missing(hl.tset(hl.tstr)), info = hl.struct(**{}))
	elif args.mt_in:
		print("read matrix table")
		mt = hl.read_matrix_table(args.mt_in)
	else:
		print("option --vcf-in, --plink-in, or --mt-in must be specified")
		return -1

	print("baseline matrix table description")
	print(mt.describe())

	print("replace any spaces in sample ids with an underscore")
	mt = mt.annotate_cols(s_new = mt.s.replace("\s+","_"))
	mt = mt.key_cols_by('s_new')
	mt = mt.drop('s')
	mt = mt.rename({'s_new': 's'})
	
	print("add pheno annotations, replacing any spaces in sample ids with an underscore")
	sample_in_types = {args.id_col: hl.tstr, args.sex_col: hl.tstr} if args.sex_col else {args.id_col: hl.tstr}
	tbl = hl.import_table(args.sample_in, delimiter="\t", no_header=False, types=sample_in_types)
	tbl = tbl.annotate(**{args.id_col + '_new': tbl[args.id_col].replace("\s+","_")})
	tbl = tbl.key_by(args.id_col + '_new')
	tbl = tbl.drop(args.id_col)
	tbl = tbl.rename({args.id_col + '_new': args.id_col})
	mt = mt.annotate_cols(pheno = tbl[mt.s])
	
	print("split multiallelic variants")
	mt_multi_snp = mt.filter_rows((hl.len(mt.alleles) > 2) & ~ hl.is_indel(mt.alleles[0], mt.alleles[1]))
	mt_multi_indel = mt.filter_rows((hl.len(mt.alleles) > 2) & hl.is_indel(mt.alleles[0], mt.alleles[1]))
	mt_multi_snp = hl.split_multi_hts(mt_multi_snp)
	mt_multi_indel = hl.split_multi_hts(mt_multi_indel)
	mt_multi = mt_multi_snp.union_rows(mt_multi_indel)
	
	print("prepare biallelic variants")
	mt_bi = mt.filter_rows(hl.len(mt.alleles) <= 2)
	mt_bi = mt_bi.annotate_rows(a_index = 1, was_split = False)
	mt = mt_bi.union_rows(mt_multi)

	print("add uid (chr:pos:ref:alt) id to matrix table")
	mt = mt.annotate_rows(uid = mt.locus.contig + ":" + hl.str(mt.locus.position) + ":" + mt.alleles[0] + ":" + mt.alleles[1])

	if args.dbsnp_ht:
		dbsnp_ht = hl.read_table(args.dbsnp_ht)
		mt = mt.annotate_rows(
			rsid = hl.if_else(
				hl.is_defined(dbsnp_ht[mt.row_key]),
				dbsnp_ht[mt.row_key].rsid,
				mt.rsid
			)
		)
		mt = mt.annotate_rows(
			rsid = hl.if_else(
				(mt.rsid == '.') | (~hl.is_defined(mt.rsid)),
				mt.uid,
					mt.rsid
			)
		)
	mt.rows().show()

	print("write checkpoint matrix table to disk")
	mt = mt.checkpoint(tmpdir_path + "hail_load.mt.checkpoint", overwrite=True)

	print("calculate raw variant qc metrics")
	mt = hl.variant_qc(mt, name="variant_qc_raw")

	print("convert males to diploid on non-PAR X/Y chromosomes and convert genotypes to unphased")
	mt = hail_utils.unphase_genotypes(mt = mt)

	print("impute sex on appropriate variants, compare to sample file (if supplied) and add annotations")
	if args.sex_col and args.male_code and args.female_code:
		mt = hail_utils.annotate_sex(
			mt = mt, 
			ref_genome = args.reference_genome,
			pheno_struct = 'pheno', 
			pheno_sex = args.sex_col, 
			male_code = args.male_code, 
			female_code = args.female_code
		)
	else:
		mt = hail_utils.annotate_sex(
			mt = mt, 
			ref_genome = args.reference_genome
		)

	print("write sexcheck results to file")
	tbl = mt.cols()
	tbl = tbl.rename({'s': 'IID'})
	if args.sex_col and args.male_code and args.female_code:
		tbl_out = tbl.select(pheno_sex = tbl.pheno[args.sex_col], sexcheck = tbl.sexcheck, is_female = tbl.is_female, f_stat = tbl.impute_sex.f_stat, n_called = tbl.impute_sex.n_called, expected_homs = tbl.impute_sex.expected_homs, observed_homs = tbl.impute_sex.observed_homs)
	else:
		tbl = tbl.annotate(pheno_sex = hl.missing(hl.tbool))
		tbl_out = tbl.select(pheno_sex = tbl.pheno_sex, sexcheck = tbl.sexcheck, is_female = tbl.is_female, f_stat = tbl.impute_sex.f_stat, n_called = tbl.impute_sex.n_called, expected_homs = tbl.impute_sex.expected_homs, observed_homs = tbl.impute_sex.observed_homs)
	tbl_out.export(args.sexcheck_out)

	print("write sexcheck problems to file")
	tbl_out = tbl_out.filter(tbl_out.sexcheck == "PROBLEM", keep=True)
	tbl_out.flatten().export(args.sexcheck_problems_out)

	print("set females to missing on Y and males to missing if heterozygous in nonpar regions")
	mt = hail_utils.adjust_sex_chromosomes(mt = mt, is_female = 'is_female')

	gt_codes = list(mt.entry)

	if 'AB' not in gt_codes:
		print("add AB")
		if 'AD' in gt_codes:
			mt = mt.annotate_entries(AB = hl.if_else(hl.is_defined(mt.AD), hl.if_else(hl.sum(mt.AD) > 0, mt.AD[1] / hl.sum(mt.AD), hl.missing(hl.tfloat64)) , hl.missing(hl.tfloat64)))
		else:
			mt = mt.annotate_entries(AB = hl.missing(hl.tfloat64))
		gt_codes = gt_codes + ['AB']

	if 'AB50' not in gt_codes:
		print("add AB50")
		if 'AD' in gt_codes:
			mt = mt.annotate_entries(AB50 = hl.if_else(hl.is_defined(mt.AD), hl.if_else(hl.sum(mt.AD) > 0, hl.abs((mt.AD[1] / hl.sum(mt.AD)) - 0.5), hl.missing(hl.tfloat64)) , hl.missing(hl.tfloat64)))
		else:
			mt = mt.annotate_entries(AB50 = hl.missing(hl.tfloat64))
		gt_codes = gt_codes + ['AB50']

	if 'GQ' not in gt_codes:
		print("add GQ")
		mt = mt.annotate_entries(GQ = hl.missing(hl.tint32))
		gt_codes = gt_codes + ['GQ']

	if args.gq_threshold is not None:
		if 'GTT' not in gt_codes:
			print("add GTT")
			mt = mt.annotate_entries(GTT = hl.if_else(hl.is_defined(mt.GQ) & hl.is_defined(mt.GT), hl.if_else(mt.GQ >= args.gq_threshold, mt.GT, hl.missing(hl.tcall)), hl.missing(hl.tcall)))
		if 'NALTT' not in gt_codes:
			print("add NALTT")
			mt = mt.annotate_entries(NALTT = hl.if_else(hl.is_defined(mt.GQ) & hl.is_defined(mt.GT), hl.if_else(mt.GQ >= args.gq_threshold, mt.GT.n_alt_alleles(), hl.missing(hl.tint32)), hl.missing(hl.tint32)))

	if 'DS' not in gt_codes:
		print("add DS")
		if 'PL' in gt_codes:
			print("adding DS from PL")
			mt = mt.annotate_entries(DS = hl.if_else((hl.is_defined(mt.PL)) & (hl.is_defined(mt.GT)), hl.pl_dosage(mt.PL), hl.missing(hl.tfloat64)))
		elif 'GP' in gt_codes:
			print("adding DS from GP")
			mt = mt.annotate_entries(DS = hl.if_else((hl.is_defined(mt.GP)) & (hl.is_defined(mt.GT)), hl.gp_dosage(mt.GP), hl.missing(hl.tfloat64)))
		else:
			print("unable to calculate DS due to missing PL and GP fields, using GT")
			mt = mt.annotate_entries(DS = hl.if_else(hl.is_defined(mt.GT), mt.GT.n_alt_alleles(), hl.missing(hl.tint32)))

	print("calculate call_rate, AC, AN, AF, het_freq_hwe, p_value_hwe, het, avg_ab, and avg_het_ab accounting appropriately for sex chromosomes")
	mt = hail_utils.update_variant_qc(mt = mt, is_female = 'is_female', variant_qc = 'variant_qc_raw')

	print("write variant table to file")
	mt.rows().flatten().export(args.variant_metrics_out, types_file=None)

	print("write variant list file")
	mt.rows().key_by().select('rsid','uid').export(args.variant_list_out, header=False, types_file=None)

	print("write sample list file")
	mt.cols().select().export(args.sample_list_out, header=False, types_file=None)

	print("write matrix table to disk")
	mt.write(args.mt_out, overwrite=True)
	
	print("write site vcf file")
	mt_sites= mt.select_cols()
	mt_sites = mt_sites.filter_cols(False)
	mt_sites_vcf = mt_sites.select_rows('uid','qual','filters','info')
	mt_sites_vcf = mt_sites_vcf.rename({'uid': 'rsid'})
	hl.export_vcf(mt_sites_vcf,args.sites_vcf_out)

	if args.cloud:
		hl.copy_log(args.log)

	tmpdir.cleanup()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--reference-genome', choices=['GRCh37','GRCh38'], default='GRCh37', help='a reference genome build code')
	parser.add_argument('--min-partitions', type=int, default=100, help='number of min partitions')
	parser.add_argument('--gq-threshold', type=int, help='add filtered entry fields set to missing where GQ is below threshold')
	parser.add_argument('--cloud', action='store_true', default=False, help='flag indicates that the log file will be a cloud uri rather than regular file path')
	parser.add_argument('--hail-utils', help='a path to a python file containing hail functions')
	parser.add_argument('--vcf-in', help='a compressed vcf file')
	parser.add_argument('--mt-in', help='a hail matrix table')
	parser.add_argument('--dbsnp-ht', help='a dbsnp build vcf (ex: https://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b151_GRCh37p13/VCF/00-All.vcf.gz)')
	parser.add_argument('--plink-in', help='a plink file set base name')
	parser.add_argument('--sex-col', help='a column name for sex in the sample file')
	parser.add_argument('--male-code', help='a code for male')
	parser.add_argument('--female-code', help='a code for female')
	parser.add_argument('--driver-memory', default="1g", help='spark driver memory')
	parser.add_argument('--executor-memory', default="1g", help='spark executor memory')
	parser.add_argument('--tmp-dir', help='a temporary path')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--log', help='a hail log filename', required=True)
	requiredArgs.add_argument('--sample-in', help='a tab delimited sample file', required=True)
	requiredArgs.add_argument('--id-col', help='a column name for sample id in the sample file', required=True)
	requiredArgs.add_argument('--variant-metrics-out', help='an output filename for variant qc metrics', required=True)
	requiredArgs.add_argument('--sexcheck-out', help='an output filename for sexcheck results', required=True)
	requiredArgs.add_argument('--sexcheck-problems-out', help='an output filename for sexcheck results that were problems', required=True)
	requiredArgs.add_argument('--sites-vcf-out', help='an output filename for a sites only VCF file (must end in .vcf)', required=True)
	requiredArgs.add_argument('--variant-list-out', help='an output filename for a variant ids', required=True)
	requiredArgs.add_argument('--sample-list-out', help='an output filename for sample ids', required=True)
	requiredArgs.add_argument('--mt-out', help='a hail mt directory name for output', required=True)
	args = parser.parse_args()
	main(args)
