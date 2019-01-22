from hail import *
hc = HailContext()
import argparse

def main(args=None):

	print "reading vds dataset"
	vds = hc.read(args.vds_in)
	vds.summarize().report()

	print "adding self reported sex annotations"
	sex_annotations = (hc.import_table(args.pheno_in,delimiter="\t",no_header=False,types={args.sex_col: TString()}).key_by(args.id_col))
	vds = vds.annotate_samples_table(sex_annotations, expr='sa.pheno.SEX = table.' + args.sex_col)

	print "calculating variant qc stats"
	vds = vds.variant_qc()

	print "filtering variants"
	vds = vds.filter_variants_expr('v.altAllele.isSNP && ! v.altAllele.isComplex && ["A","C","G","T"].toSet.contains(v.altAllele.ref) && ["A","C","G","T"].toSet.contains(v.altAllele.alt) && va.qc.AF >= 0.01 && va.qc.callRate >= 0.98', keep=True)
	vds.summarize().report()

	print "excluding regions with high LD"
	exclude_regions_list = hc.import_table(args.regions_exclude, no_header=True, key='f0', types={'f0': TInterval()})
	vds = vds.filter_variants_table(exclude_regions_list, keep=False)
	vds.summarize().report()

	print "imputing sex"
	vds = vds.impute_sex()

	print "annotating samples with sexcheck status"
	vds = vds.annotate_samples_expr('sa.sexcheck = if(((sa.pheno.SEX == "female" || sa.pheno.SEX == "Female" || sa.pheno.SEX == "f" || sa.pheno.SEX == "F" || sa.pheno.SEX == "' + args.female_code + '") && ! isMissing(sa.imputesex.isFemale) && sa.imputesex.isFemale) || ((sa.pheno.SEX == "male" || sa.pheno.SEX == "Male" || sa.pheno.SEX == "m" || sa.pheno.SEX == "M" || sa.pheno.SEX == "' + args.male_code + '") && ! isMissing(sa.imputesex.isFemale) && ! sa.imputesex.isFemale) || isMissing(sa.imputesex.isFemale)) "OK" else "PROBLEM"')

	print "replace isFemale annotation with self report if imputed sex failed"
	vds = vds.annotate_samples_expr('sa.imputesex.isFemale = if((sa.pheno.SEX == "female" || sa.pheno.SEX == "Female" || sa.pheno.SEX == "f" || sa.pheno.SEX == "F" || sa.pheno.SEX == "' + args.female_code + '") &&  isMissing(sa.imputesex.isFemale)) true else if((sa.pheno.SEX == "male" || sa.pheno.SEX == "Male" || sa.pheno.SEX == "m" || sa.pheno.SEX == "M" || sa.pheno.SEX == "' + args.male_code + '") &&  isMissing(sa.imputesex.isFemale)) false else sa.imputesex.isFemale')

	print "write sexcheck results to file"
	vds.export_samples(args.sexcheck_out, expr="IID = s, SEX = sa.pheno.SEX, sa.imputesex.*, sexCheck = sa.sexcheck")

	print "reducing to samples with sexcheck problems"
	vds = vds.filter_samples_expr('sa.sexcheck == "PROBLEM"',keep=True)

	print "write sexcheck problems to file"
	vds.export_samples(args.sexcheck_problems_out, expr="IID = s, SEX = sa.pheno.SEX, sa.imputesex.*, sexCheck = sa.sexcheck")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vds-in', help='a hail vds dataset', required=True)
	requiredArgs.add_argument('--regions-exclude', help='a list of regions to exclude from qc', required=True)
	requiredArgs.add_argument('--pheno-in', help='a tab delimited phenotype file', required=True)
	requiredArgs.add_argument('--id-col', help='a column name for sample id in the phenotype file', required=True)
	requiredArgs.add_argument('--sex-col', help='a column name for sex in the phenotype file', required=True)
	requiredArgs.add_argument('--male-code', help='a code for male', required=True)
	requiredArgs.add_argument('--female-code', help='a code for female', required=True)
	requiredArgs.add_argument('--sexcheck-out', help='an output filename for sexcheck results', required=True)
	requiredArgs.add_argument('--sexcheck-problems-out', help='an output filename for sexcheck results that were problems', required=True)
	args = parser.parse_args()
	main(args)
