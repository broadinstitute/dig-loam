from hail import *
hc = HailContext()
import argparse

def main(args=None):

	print "reading vds dataset"
	vds = hc.read(args.vds_in)
	vds.summarize().report()

	print "adding ancestry annotation"
	annotate_anc = hc.import_table(args.ancestry_in,delimiter="\t",no_header=True).annotate('IID = f0').key_by('IID')
	vds = vds.annotate_samples_table(annotate_anc, expr='sa.pheno.GROUP = table.f1')

	print "adding imputed sex annotation"
	annotate_sex = hc.import_table(args.sexcheck_in, no_header=False, types={'isFemale': TBoolean()}).key_by('IID')
	vds = vds.annotate_samples_table(annotate_sex, expr='sa.isFemale = table.isFemale')

	print "adding case/control status annotation"
	annotate_cc = hc.import_table(args.pheno_in, no_header=False, types={args.case_ctrl_col: TInt()}).key_by(args.iid_col)
	vds = vds.annotate_samples_table(annotate_cc, expr='sa.isCase = table.' + args.case_ctrl_col + ' == 1')

	print "calculating pre sampleqc genotype call rate"
	pre_sampleqc_callrate = vds.query_genotypes('gs.fraction(g => g.isCalled)')
	print 'pre sampleqc call rate is %.3f' % pre_sampleqc_callrate

	print "removing samples that failed QC"
	samples_remove = hc.import_table(args.samples_remove, no_header=True).key_by('f0')
	vds = vds.filter_samples_table(samples_remove, keep=False)
	vds.summarize().report()

	print "calculating post sampleqc genotype call rate"
	post_sampleqc_callrate = vds.query_genotypes('gs.fraction(g => g.isCalled)')
	print 'post sampleqc call rate is %.3f' % post_sampleqc_callrate

	#if sequence data, check genotype call using allele balance
	#filter_condition_ab = '''let ab = g.ad[1] / g.ad.sum() in
	#                         ((g.isHomRef && ab <= 0.1) ||
	#                          (g.isHet && ab >= 0.25 && ab <= 0.75) ||
	#                          (g.isHomVar && ab >= 0.9))'''
	#vds = vds.filter_genotypes(filter_condition_ab)

	#print "reducing to autosomal variants only"
	#vds = vds.filter_variants_expr('v.isAutosomal', keep=True)

	samples_df = vds.samples_table().to_pandas()
	samples_df['sa.isCase'] = samples_df['sa.isCase'].astype('bool')
	samples_df['sa.isFemale'] = samples_df['sa.isFemale'].astype('bool')
	group_counts = samples_df['sa.pheno.GROUP'][~samples_df['sa.isCase']].value_counts().to_dict()

	vds = vds.annotate_variants_expr('va.failed = 0')

	print "counting males and females"
	# not sure why this doesn't work, says symbol samples is not found
	# vds = vds.annotate_global_expr('global.nMales = samples.filter(s => ! sa.isFemale).count(), global.nFemales = samples.filter(s => sa.isFemale).count()')
	nMales = samples_df[~samples_df['sa.isFemale']].shape[0]
	nFemales = samples_df[samples_df['sa.isFemale']].shape[0]

	print "counting male/female hets, homvars and called"
	vds = vds.annotate_variants_expr('va.nMaleHet = gs.filter(g => ! sa.isFemale && g.isHet).count(), va.nMaleHomVar = gs.filter(g => ! sa.isFemale && g.isHomVar).count(), va.nMaleCalled = gs.filter(g => ! sa.isFemale && g.isCalled).count(), va.nFemaleHet = gs.filter(g => sa.isFemale && g.isHet).count(), va.nFemaleHomVar = gs.filter(g => sa.isFemale && g.isHomVar).count(), va.nFemaleCalled = gs.filter(g => sa.isFemale && g.isCalled).count()')

	print "calculating callRate, MAC, and MAF (accounting appropriately for sex chromosomes)"
	vds = vds.annotate_variants_expr('va.callRate = if (v.inYNonPar) ( va.nMaleCalled / ' + str(nMales) + ' ) else if (v.inXNonPar) ( (va.nMaleCalled + 2*va.nFemaleCalled) / (' + str(nMales) + ' + 2*' + str(nFemales) + ') ) else ( (va.nMaleCalled + va.nFemaleCalled) / (' + str(nMales) + ' + ' + str(nFemales) + ') ), va.MAC = if (v.inYNonPar) ( va.nMaleHomVar ) else if (v.inXNonPar) ( va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar ) else ( va.nMaleHet + 2*va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar ), va.MAF = if (v.inYNonPar) ( va.nMaleHomVar / va.nMaleCalled) else if (v.inXNonPar) ( (va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar) / (va.nMaleCalled + 2*va.nFemaleCalled) ) else ( (va.nMaleHet + 2*va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar) / (2*va.nMaleCalled + 2*va.nFemaleCalled) )')


	print "filtering variants for callrate"
	vds = vds.annotate_variants_expr('va.failed = if (va.callRate < 0.98) 1 else va.failed')

	groups_used = []
	for group in group_counts:
		if group_counts[group] > 100 and group_counts[group] != 'AMR':
			groups_used.extend([group])
			print "filtering autosomal variants with pHWE <= 1e-6 in " + group + " male and female controls"
			vds = vds.annotate_variants_expr('va.pHWECtrl' + group + ' = if (v.inXNonPar) (gs.filter(g => sa.isFemale && ! sa.isCase && sa.pheno.GROUP == "' + group + '").hardyWeinberg()) else if (v.inYPar || v.inYNonPar) (gs.filter(g => ! sa.isFemale && ! sa.isCase && sa.pheno.GROUP == "' + group + '").hardyWeinberg()) else gs.filter(g => ! sa.isCase && sa.pheno.GROUP == "' + group + '").hardyWeinberg()')
			vds = vds.annotate_variants_expr('va.failed = if (va.MAF >= 0.01 && va.pHWECtrl' + group + '.pHWE <= 1e-6) 1 else va.failed')

	print "writing variant qc results to file"
	if len(groups_used) > 0:
		export_expr = "Chrom = v.contig, Pos = v.start, ID = v, RSID = va.rsid, Ref = v.ref, Alt = v.alt, failed = va.failed, callRate = va.callRate, MAC = va.MAC, MAF = va.MAF, " + ', '.join(['pHWECtrl' + group + ' = va.pHWECtrl' + group + '.pHWE' for group in groups_used])
	else:
		export_expr = "Chrom = v.contig, Pos = v.start, ID = v, RSID = va.rsid, Ref = v.ref, Alt = v.alt, failed = va.failed, callRate = va.callRate, MAC = va.MAC, MAF = va.MAF"
	vds.export_variants(args.variantqc_out, expr=export_expr, types=False)

	print "writing variant exclusions to file"
	vds.filter_variants_expr('va.failed == 1', keep=True).export_variants(args.variants_exclude_out, expr="Chrom = v.contig, Pos = v.start, ID = v, RSID = va.rsid, Ref = v.ref, Alt = v.alt", types=False)

	print "filtering failed variants out of vds"
	vds = vds.filter_variants_expr('va.failed == 1', keep=False)

	print "writing vds to disk"
	vds.write(args.vds_out, overwrite=True)

	print "writing Plink files to disk"
	vds.export_plink(args.plink_out, fam_expr='famID = s, id = s, isFemale = sa.isFemale')

	print "writing VCF file to disk"
	vds.export_vcf(args.vcf_out)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vds-in', help='a hail vds dataset name', required=True)
	requiredArgs.add_argument('--ancestry-in', help='an inferred ancestry file', required=True)
	requiredArgs.add_argument('--sexcheck-in', help='an imputed sexcheck output file from Hail', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--case-ctrl-col', help='column name for case/control status in phenotype file', required=True)
	requiredArgs.add_argument('--samples-remove', help='a file containing sample IDs that failed QC', required=True)
	requiredArgs.add_argument('--variantqc-out', help='a base filename for variantqc', required=True)
	requiredArgs.add_argument('--variants-exclude-out', help='a base filename for failed variants', required=True)
	requiredArgs.add_argument('--plink-out', help='a plink fileset name for output', required=True)
	requiredArgs.add_argument('--vcf-out', help='a vcf file name for output', required=True)
	requiredArgs.add_argument('--vds-out', help='a vds directory name for output', required=True)
	args = parser.parse_args()
	main(args)
