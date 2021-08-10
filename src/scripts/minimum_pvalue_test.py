#Usage: python minimum_p_value_test.py
#
#This script is designed to condense a set of burden test p-values for a gene into a single p-value corrected for the effective number of tests (e.g. variant groups, or masks) performed. The effective number of tests is calculated based on the correlation matrix of variants across the tests
#The method implemented is that described in Li et al, GATES: A Rapid and Powerful Gene-Based Association Test Using Extended Simes Procedure. AJHG, 2011 (PMC3059433)
#
#Arguments:
# p-value-file[path]: the results of tests for each gene. Needs columns for test (group) id, p-value, and effect size
# gene-group-file[path]: a mapping between genes and tests (groups). Needs columns for gene-id and test (group) id [Required]
# group-variant-file[path]: the list of variants in each test (group). Needs columns for test (group) id, variant id, and minor allele frequency [Required]
# out-file[path]: where to write the output [Default: stdout]
# p-value-file-id-col[int]: the 1-based test (group) id column in the p-value-file [Default: 1]
# p-value-file-p-col[int]: the 1-based p-value column in the p-value-file [Default: 2]
# p-value-file-effect-col[int]: the 1-based effect size column in the p-value-file [Default: 3]
# p-value-file-add-cols[int]: additional 1-based columns to print for the consolidated p-value. These columns will be taken from the most significant test (group) id 
# add-col-headers[string]: headers to print for the specified p-value-file-add-cols
# gene-group-file-gene-col[int]: the 1-based gene id column in the gene-group-file [Default: 1]
# gene-group-file-id-col[int]: the 1-based test (group) id column in the gene-group-file [Default: 2]
# group-variant-file-id-col[int]: the 1-based test (group) id column in the group-variant-file [Default: 1]
# group-variant-file-variant-col[int]: the 1-based variant id column in the group-variant-file [Default: 2]
# group-variant-file-maf-col[int]: the 1-based MAF column in the group-variant-file [Default: 3]
# min-maf[bool]: exclude variants below a maf threshold [Default: 0]
# basic[bool]: apply a basic p-value correction, rather than the method proposed in Li et al [Default: false]
# no-transform-r2[bool]: do not apply the transformation proposed in Li et al for the correlations across masks [Default: false]
# no-headers[bool]: specify that no input files have headers [Default: false]
# no-effect[bool]: specify that the input tests do not have effect sizes (e.g. SKAT). If so, effect-col is not required [Default: false]
# na[string]: specify values to treat as implying "NA". Specify as many times as desired [Default: NA]
# parser.add_option("","--debug-level",type="int",default=1) #0=no log, 1=info, 2=debug, 3=trace
# warnings-file[path]: write warnings to a file rather than stderr

import optparse
import sys
import copy

def bail(message):
    sys.stderr.write("%s\n" % (message))
    sys.exit(1)

usage = "usage: mimimum_pvalue_test.py --p-value-file <add-file> --gene-group-file <file> --group-variant-file <file>  --out-file <file> [options]"
parser = optparse.OptionParser(usage)
parser.add_option("","--p-value-file",action='append',default=[])
parser.add_option("","--na",action='append',default=["NA"])
parser.add_option("","--gene-group-file",default=None)
parser.add_option("","--group-variant-file",default=None)
parser.add_option("","--p-value-file-id-col",type="int",default=1)
parser.add_option("","--p-value-file-p-col",type="int",default=2)
parser.add_option("","--p-value-file-effect-col",type="int",default=3)
parser.add_option("","--add-col-headers",action='append',default=[])
parser.add_option("","--p-value-file-add-cols",action='append',default=[])
parser.add_option("","--no-effect",action="store_true")
parser.add_option("","--gene-group-file-gene-col",type="int",default=1)
parser.add_option("","--gene-group-file-id-col",type="int",default=2)
parser.add_option("","--group-variant-file-id-col",type="int",default=1)
parser.add_option("","--group-variant-file-variant-col",type="int",default=2)
parser.add_option("","--group-variant-file-maf-col",type="int",default=3)
parser.add_option("","--warnings-file",default=None)
parser.add_option("","--no-transform-r2",action="store_true")
parser.add_option("","--no-headers",action="store_true")
parser.add_option("","--basic",action="store_true")
parser.add_option("","--min-maf",type="float",default=0)
parser.add_option("","--debug-level",type="int",default=1) #0=no log, 1=info, 2=debug, 3=trace
parser.add_option("","--out-file",default=None)

(options, args) = parser.parse_args()

if len(options.p_value_file) == 0 or options.gene_group_file is None is None:
    bail(usage)

#set up warnings
warnings_fh = None
if options.warnings_file is not None:
    warnings_fh = open(options.warnings_file, 'w')
else:
    warnings_fh = sys.stderr

def warn(message):
    if warnings_fh is not None:
        warnings_fh.write("%s\n" % message)
        warnings_fh.flush()

NONE=0
INFO=1
DEBUG=2
TRACE=3
def log(message, level=INFO):
    if level <= options.debug_level:
        sys.stderr.write("%s\n" % message)
        sys.stderr.flush()

log("Reading in files...")

gene_group_fh = open(options.gene_group_file, 'r')
if not options.no_headers:
    gene_group_header = gene_group_fh.readline()
gene_to_groups = dict()
group_to_gene = dict()
for line in gene_group_fh:
    line = line.strip()
    cols = line.split()
    if options.gene_group_file_id_col < 1 or options.gene_group_file_id_col > len(cols):
        bail("Error: --gene-group-file-id-col %d out of range in line with %d cols" % (options.gene_group_file_id_col, len(cols)))
    if options.gene_group_file_gene_col < 1 or options.gene_group_file_gene_col > len(cols):
        bail("Error: --gene-group-file-gene-col %d out of range in line with %d cols" % (options.gene_group_file_gene_col, len(cols)))
    group = cols[options.gene_group_file_id_col-1]
    gene = cols[options.gene_group_file_gene_col-1]
    group_to_gene[group] = gene
    if gene not in gene_to_groups:
        gene_to_groups[gene] = set()
    gene_to_groups[gene].add(group)
gene_group_fh.close()

if options.p_value_file_add_cols and not len(options.p_value_file_add_cols) == len(options.add_col_headers):
    bail("Error: must have one --add-col-headers for every --p-value-file-add-cols")

group_to_pvalue = dict()
group_to_effect = dict()
group_to_add = dict()
for p_value_file in options.p_value_file:
    p_value_fh = open(p_value_file, 'r')
    if not options.no_headers:
        p_value_header = p_value_fh.readline()
    for line in p_value_fh:
        line = line.strip()
        cols = line.split()
        if options.p_value_file_id_col < 1 or options.p_value_file_id_col > len(cols):
            bail("Error: --p-value-file-id-col %d out of range in line with %d cols" % (options.p_value_file_id_col, len(cols)))
        if options.p_value_file_p_col < 1 or options.p_value_file_p_col > len(cols):
            bail("Error: --p-value-file-p-col %d out of range in line with %d cols" % (options.p_value_file_p_col, len(cols)))
        group = cols[options.p_value_file_id_col-1]
        if group not in group_to_gene:
            warn("Group %s was not defined to map to a gene" % group)
            continue
        p_value = cols[options.p_value_file_p_col-1]
        if p_value not in options.na:
            p_value = float(p_value)
        else:
            continue
        group_to_pvalue[group] = p_value

        if not options.no_effect:
            if options.p_value_file_p_col < 1 or options.p_value_file_effect_col > len(cols):
                bail("Error: --p-value-file-effect-col %d out of range in line with %d cols" % (options.p_value_file_effect_col, len(cols)))
            effect = cols[options.p_value_file_effect_col-1]
        else:
            effect = options.na[0]
            
        if effect not in options.na:
            effect = float(effect)
        group_to_effect[group] = effect

        for p_value_file_add_col in [int(x) for x in options.p_value_file_add_cols]:
            if p_value_file_add_col < 1 or p_value_file_add_col > len(cols):
                bail("Error: --p-value-file-add-col %d out of range in line with %d cols" % (p_value_file_add_col, len(cols)))
            add_col = cols[p_value_file_add_col-1]
            if group not in group_to_add:
                group_to_add[group] = []
            group_to_add[group].append(add_col)

    p_value_fh.close()

if options.group_variant_file is not None:
    group_variant_fh = open(options.group_variant_file, 'r')
    if not options.no_headers:
        group_variant_header = group_variant_fh.readline()
    group_to_variants = dict()
    group_to_combined_maf = dict()
    variant_to_maf = dict()
    for line in group_variant_fh:
        line = line.strip()
        cols = line.split()
        if options.group_variant_file_id_col < 1 or options.group_variant_file_id_col > len(cols):
            bail("Error: --group-variant-file-id-col %d out of range in line with %d cols" % (options.group_variant_file_id_col, len(cols)))
        if options.group_variant_file_variant_col < 1 or options.group_variant_file_variant_col > len(cols):
            bail("Error: --group-variant-file-variant-col %d out of range in line with %d cols" % (options.group_variant_file_variant_col, len(cols)))
        if options.group_variant_file_maf_col < 1 or options.group_variant_file_maf_col > len(cols):
            bail("Error: --group-variant-file-maf-col %d out of range in line with %d cols" % (options.group_variant_file_maf_col, len(cols)))
        group = cols[options.group_variant_file_id_col-1]
        variant = cols[options.group_variant_file_variant_col-1]
        maf = cols[options.group_variant_file_maf_col-1]
        if maf not in options.na:
            maf = float(maf)
        else:
            continue
        if maf > 0.5:
            warn("Variant %s had MAF > 0.5 (%.2g); converting to %.2g" % (variant, maf, 1 - maf))
            maf = 1 - maf
        if variant in variant_to_maf and not variant_to_maf[variant] == maf:
            warn("Variant %s has inconsistent maf values (%.2g and %.2g)" % (variant, variant_to_maf[variant], maf))
            continue
        variant_to_maf[variant] = maf
        if group not in group_to_variants:
            group_to_variants[group] = set()
            group_to_combined_maf[group] = 0
        group_to_variants[group].add(variant)
        group_to_combined_maf[group] += maf
    group_variant_fh.close()

log("Performing tests...")

if options.out_file is not None:
    out_fh = open(options.out_file, 'w')
else:
    out_fh = sys.stdout

import numpy as np
#Use PMC3059433
#now do the actual gene testing
if options.add_col_headers:
    add_text = '\t' + '\t'.join(options.add_col_headers)
else:
    add_text = ''
out_fh.write("%s\t%s\t%s\t%s\t%s\t%s%s\n" % ("Gene", "P-value", "Effect", "Num_tests", "Chosen_P-value", "Num_tests_eff", add_text))

for gene in gene_to_groups:
    log("Processing %s" % gene, DEBUG)
    gene_groups = [x for x in gene_to_groups[gene] if x in group_to_pvalue and (options.group_variant_file is None or x in group_to_variants) and group_to_pvalue[x] >= 0 and group_to_pvalue[x] <= 1 and (options.group_variant_file is None or group_to_combined_maf[x] > options.min_maf)]
    if (len(gene_groups) == 0):
        continue
    r2_mat = np.zeros((len(gene_groups),len(gene_groups)))
    problems = set()
    p_values_effects = list()
    for i in range(len(gene_groups)):
        cur_groupA = gene_groups[i]
        cur_effect = "NA"
        cur_add = ["NA" for x in options.add_col_headers]
        if cur_groupA in group_to_effect:
            cur_effect = group_to_effect[cur_groupA]
        if cur_groupA in group_to_add:
            cur_add = group_to_add[cur_groupA]

        p_values_effects.append((group_to_pvalue[cur_groupA], cur_effect, cur_add))

        variantsA = None
        if options.group_variant_file is not None:
            variantsA = group_to_variants[cur_groupA]
        for j in range(i,len(gene_groups)):
            if i == j:
                r2 = 1
            else:
                if options.group_variant_file is None:
                    r2 = 0
                else:
                    cur_groupB = gene_groups[j]
                    variantsB = set(group_to_variants[cur_groupB])
                    all_variants = set(list(variantsA) + list(variantsB))
                    pA = 0
                    pB = 0
                    pAB = 0
                    for variant in all_variants:
                        maf = 0
                        if variant in variant_to_maf:
                            maf = variant_to_maf[variant]
                        if variant in variantsA:
                            pA += maf
                        if variant in variantsB:
                            pB += maf
                        if variant in variantsA and variant in variantsB:
                            pAB += maf
                    if pA == 0 or pB == 0:
                        r2 = 0
                    elif pA > 1 or pB > 1:
                        if pA > 1:
                            problems.add("Sum of variant frequencies in %s exceeded 1 (%.6g)" % (gene_groups[i], pA))
                        if pB > 1:
                            problems.add("Sum of variant frequencies in %s exceeded 1 (%.6g)" % (gene_groups[j], pB))
                        break
                    else:
                        r2 = (pAB - pA * pB)**2 / (pA * (1 - pA) * pB * (1 - pB))
                    #now map to p-value r2
                    if options.no_transform_r2:
                        pass
                    else:
                        r2 = 0.2982 * r2 ** 6 - 0.0127 * r2 ** 5 + 0.0588 * r2 ** 4 + 0.0099 * r2 ** 3 + 0.6281 * r2 ** 2 - 0.0009 * r2
                        r2 /= 0.9814

            r2_mat[i,j] = r2
            r2_mat[j,i] = r2
            
    log("r2_mat: %s" % r2_mat, TRACE)

    def get_n_tests_eff(num_p_values):
        eigen_values = list(reversed(sorted(list(np.linalg.eig(r2_mat[0:num_p_values,0:num_p_values])[0]))))        
        log("Submatrix %d eigen_values: %s" % (num_p_values, eigen_values), TRACE)
        n_tests_eff = num_p_values
        for eigen_val in eigen_values:
            if eigen_val > 1:
                n_tests_eff -= (eigen_val - 1)
        return n_tests_eff

    p_values_effects.sort(key=lambda x: x[0])

    if len(problems) == 0:
        n_tests_eff = get_n_tests_eff(len(p_values_effects))
    if len(problems) > 0 or n_tests_eff < 0:
        if len(problems) > 0:
            for problem in problems:
                warn(problem)
        warn("Problem calculating r2 matrix for gene %s; skipping" % gene)
        n_tests_eff = len(p_values_effects)
        
    cur_min_p = None
    p_value_rank = 0

    if options.basic:
        if p_values_effects[0][0] > 1e-10:
            calculated_min_p = 1 - (1 - p_values_effects[0][0])**n_tests_eff
        else:
            x = p_values_effects[0][0]
            y = n_tests_eff
            #use taylor expansion (third order is probably way overdoing it)
            calculated_min_p = x*y - (1.0/2)*x**2 *(y-1) + (1.0/6)*x**3*((y-1)*(y-2)*y)
        cur_min_p = (calculated_min_p, p_values_effects[0][1], p_values_effects[0][2])
        p_value_rank = 1
    else:
        for i in range(len(gene_groups)):
            cur_n_tests_eff = get_n_tests_eff(i+1)
            cur_p = p_values_effects[i][0] * n_tests_eff / cur_n_tests_eff
            if cur_min_p is None or cur_p < cur_min_p[0]:
                p_value_rank = i + 1
                cur_min_p = (cur_p, p_values_effects[i][1])
            
    effect = cur_min_p[1]
    add = cur_min_p[2]

    try:
        effect = "%.3g" % (effect)
    except ValueError:
        pass
    except TypeError:
        pass

    if options.add_col_headers:
        add_text = '\t' + '\t'.join(add)
    else:
        add_text = ''
    out_fh.write("%s\t%.3g\t%s\t%d\t%d\t%.2g%s\n" % (gene, cur_min_p[0], effect, len(p_values_effects), p_value_rank, n_tests_eff, add_text))        


if options.out_file:
    out_fh.close()
