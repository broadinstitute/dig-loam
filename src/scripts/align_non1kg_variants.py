import pandas as pd
import numpy as np
import argparse
import os

def complement(a):
	if a != "NA":
		letters = list(a)
		comp = []
		for l in letters:
			if l == 'T': 
				c = 'A'
			elif l == 'A':
				c = 'T'
			elif l == 'G': 
				c = 'C'
			elif l == 'C':
				c = 'G'
			elif l == '0':
				c = '0'
			elif l == ',':
				c = ','
			elif l == 'NA':
				c = 'NA'
			elif l == '-':
				c = '-'
			elif l == 'I':
				c = 'D'
			elif l == 'D':
				c = 'I'
			elif l in ['1','2','3','4','5','6','7','8','9','0']:
				c = l
			else:
				c = 'X'
			comp.append(c)
	else:
		comp = ['NA']
	return ''.join(comp)

def main(args=None):

	if os.path.isfile(args.bim) and os.path.getsize(args.bim) > 0:

		bim = pd.read_table(args.bim, header=None, sep=None, engine='python')
		bim.columns = ["chr","rsid","cm","pos","a1","a2"]
		bim['kg'] = 0
		print "loaded bim file with " + str(bim.shape[0]) + " variants"

		if args.kg_ids:
			with open(args.kg_ids) as f:
				x=f.read().splitlines()
			bim.loc[bim['rsid'].isin(x),'kg'] = 1
	
		bim['ref'] = bim['a1']
		bim['alt'] = bim['a2']
		bim['status'] = "ignore"
	
		with open(args.ref,"r") as ref_file:
			ref_seq = ref_file.read().upper()
	
		snps = []
		n = bim[bim['kg'] == 0].shape[0]
		print "iterating over " + str(n) + " non 1kg rows for comparison with ref_seq"
	
		for idx, row in bim[bim['kg'] == 0].iterrows():
			if row[3] <= len(ref_seq):
				ref = ref_seq[row[3]-1]
				bim.loc[idx,'ref'] = ref
				bima1 = row[4]
				bima2 = row[5]
				bima1comp = complement(bima1)
				bima2comp = complement(bima2)
				if bima1 + bima2 not in ["AT","TA","GC","CG"]:
					if bima1 == "0" or bima2 == "0":
						print str(idx) + " / " + str(n) + " " + bima1 + "/" + bima2 + " " + ref + " : remove"
						bim.loc[idx,'status'] = "remove_mono"
					else:
						if bima1 == ref:
							print str(idx) + " / " + str(n) + " " + bima1 + "/" + bima2 + " " + ref + " : match"
							bim.loc[idx,'alt'] = bima2
							bim.loc[idx,'status'] = "match"
						elif bima2 == ref:
							print str(idx) + " / " + str(n) + " " + bima1 + "/" + bima2 + " " + ref + " : reverse"
							bim.loc[idx,'alt'] = bima1
							bim.loc[idx,'status'] = "reverse"
						elif bima1comp == ref:
							print str(idx) + " / " + str(n) + " " + bima1 + "/" + bima2 + " " + ref + " : flip"
							bim.loc[idx,'alt'] = bima2comp
							bim.loc[idx,'status'] = "flip"
						elif bima2comp == ref:
							print str(idx) + " / " + str(n) + " " + bima1 + "/" + bima2 + " " + ref + " : flip_reverse"
							bim.loc[idx,'alt'] = bima1comp
							bim.loc[idx,'status'] = "flip_reverse"
						else:
							print str(idx) + " / " + str(n) + " " + bima1 + "/" + bima2 + " " + ref + " : remove"
							bim.loc[idx,'status'] = "remove_nomatch"
			else:
				bim.loc[idx,'status'] = "remove_nomatch"
	
		print "writing results to files"
	
		bim['rsid'][bim['status'].isin(["remove_mono","remove_nomatch"])].to_csv(args.out_remove, header=False, index=False)
		bim['rsid'][bim['status'] == "remove_mono"].to_csv(args.out_mono, header=False, index=False)
		bim['rsid'][bim['status'] == "remove_nomatch"].to_csv(args.out_nomatch, header=False, index=False)
		bim['rsid'][bim['status'] == "ignore"].to_csv(args.out_ignore, header=False, index=False)
		bim['rsid'][bim['status'].isin(["flip","flip_reverse"])].to_csv(args.out_flip, header=False, index=False)
		bim = bim[~bim['status'].isin(["remove_mono","remove_nomatch"])].reset_index(drop=True)
		bim[['rsid','ref']].to_csv(args.out_force_a1, header=False, index=False, sep=" ")

	else:

		with open(args.out_remove, mode='a'): pass
		with open(args.out_mono, mode='a'): pass
		with open(args.out_nomatch, mode='a'): pass
		with open(args.out_ignore, mode='a'): pass
		with open(args.out_flip, mode='a'): pass
		with open(args.out_force_a1, mode='a'): pass

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--kg-ids', help='a 1kg variant ids file')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--bim', help='a bim file already aligned with 1kg using genotype harmonizer', required=True)
	requiredArgs.add_argument('--ref', help='a file containing entire human reference build 37', required=True)
	requiredArgs.add_argument('--out-remove', help='output file name for list of variants to remove', required=True)
	requiredArgs.add_argument('--out-ignore', help='output file name for list of variants that were ignored (AT/GC variants)', required=True)
	requiredArgs.add_argument('--out-mono', help='output file name for list of monomorphic variants', required=True)
	requiredArgs.add_argument('--out-nomatch', help='output file name for list of variants whose alleles do not match the reference', required=True)
	requiredArgs.add_argument('--out-flip', help='output file name for list of variants to flip', required=True)
	requiredArgs.add_argument('--out-force-a1', help='output file name for list of variants and alleles to be force into a1 position in Plink file', required=True)
	args = parser.parse_args()
	main(args)
