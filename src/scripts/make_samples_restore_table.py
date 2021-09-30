import pandas as pd
import argparse

def main(args=None):

	df = pd.DataFrame({'IID': [], 'RestoreFrom': []})

	if args.ancestry_outliers_keep:
		with open(args.ancestry_outliers_keep) as f:
			lines = f.read().splitlines()
		tempdf=pd.DataFrame({'IID': lines, 'RestoreFrom': 'ancestryOutliersKeep'})
		df = df.append(tempdf, ignore_index=True)

	if args.duplicates_keep:
		with open(args.duplicates_keep) as f:
			lines = f.read().splitlines()
		tempdf=pd.DataFrame({'IID': lines, 'RestoreFrom': 'duplicatesKeep'})
		df = df.append(tempdf, ignore_index=True)

	if args.famsize_keep:
		with open(args.famsize_keep) as f:
			lines = f.read().splitlines()
		tempdf=pd.DataFrame({'IID': lines, 'RestoreFrom': 'famsizeKeep'})
		df = df.append(tempdf, ignore_index=True)

	if args.sampleqc_keep:
		with open(args.sampleqc_keep) as f:
			lines = f.read().splitlines()
		tempdf=pd.DataFrame({'IID': lines, 'RestoreFrom': 'sampleqcKeep'})
		df = df.append(tempdf, ignore_index=True)

	if args.sexcheck_keep:
		with open(args.sexcheck_keep) as f:
			lines = f.read().splitlines()
		tempdf=pd.DataFrame({'IID': lines, 'RestoreFrom': 'sexcheckKeep'})
		df = df.append(tempdf, ignore_index=True)

	df.to_csv(args.out, header=True, index=False, sep="\t")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--ancestry-outliers-keep', help='a list of sample IDs')
	parser.add_argument('--duplicates-keep', help='a list of sample IDs')
	parser.add_argument('--famsize-keep', help='a list of sample IDs')
	parser.add_argument('--sampleqc-keep', help='a list of sample IDs')
	parser.add_argument('--sexcheck-keep', help='a list of sample IDs')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output filename', required=True)
	args = parser.parse_args()
	main(args)
