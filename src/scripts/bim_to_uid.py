import sys
import argparse

def main(args=None):

	def complement(allele):
		if allele != "NA":
			letters = list(allele)
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
	
	def get_universal_variant_id(chr,pos,a1,a2,delim):
		analogs = [
			chr + delim + pos + delim + a1 + delim + a2,
			chr + delim + pos + delim + complement(a1) + delim + complement(a2)
		]
		if a2 != 'NA':	
			analogs = analogs + [
				chr + delim + pos + delim + complement(a2) + delim + complement(a1),
				chr + delim + pos + delim + a2 + delim + a1, 
				chr + delim + pos + delim + a1 + delim + 'NA', 
				chr + delim + pos + delim + complement(a1) + delim + 'NA', 
				chr + delim + pos + delim + complement(a2) + delim + 'NA', 
				chr + delim + pos + delim + a2 + delim + 'NA'
			]
		return "><".join(sorted(list(set(analogs))))
	
	#for line in sys.stdin:
	#	chr, rsid, cm, pos, a1, a2 = line.rstrip().split()
	#	print "\t".join([rsid, get_universal_variant_id(chr,pos,a1[0:10],a2[0:10],".")])
	with open(args.bim, 'r') as f:
		with open(args.out, 'w') as o:
			for line in f.readlines():
				chr, rsid, cm, pos, a1, a2 = line.rstrip().split()
				o.write("\t".join([rsid, get_universal_variant_id(chr,pos,a1[0:10],a2[0:10],".")]) + "\n")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--bim', help='a bim file', required=True)
	requiredArgs.add_argument('--out', help='an output filename', required=True)
	args = parser.parse_args()
	main(args)
