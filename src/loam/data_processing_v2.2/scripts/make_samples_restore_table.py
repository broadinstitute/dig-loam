import pandas as pd
import argparse

def main(args=None):

	df = pd.DataFrame({'IID': [], 'RestoreFrom': []})

	if args.ancestry_outliers_keep != "":
		tempdf=pd.DataFrame({'IID': args.ancestry_outliers_keep.split(","), 'RestoreFrom': 'ancestryOutliersKeep'})
		df = df.append(tempdf, ignore_index=True)

	if args.duplicates_keep != "":
		tempdf=pd.DataFrame({'IID': args.duplicates_keep.split(","), 'RestoreFrom': 'duplicatesKeep'})
		df = df.append(tempdf, ignore_index=True)

	if args.famsize_keep != "":
		tempdf=pd.DataFrame({'IID': args.famsize_keep.split(","), 'RestoreFrom': 'famsizeKeep'})
		df = df.append(tempdf, ignore_index=True)

	if args.sampleqc_keep != "":
		tempdf=pd.DataFrame({'IID': args.sampleqc_keep.split(","), 'RestoreFrom': 'sampleqcKeep'})
		df = df.append(tempdf, ignore_index=True)

	if args.sexcheck_keep != "":
		tempdf=pd.DataFrame({'IID': args.sexcheck_keep.split(","), 'RestoreFrom': 'sexcheckKeep'})
		df = df.append(tempdf, ignore_index=True)

	df.to_csv(args.out, header=True, index=False, sep="\t")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--ancestry-outliers-keep', help='a comma separated list of sample IDs', required=True)
	requiredArgs.add_argument('--duplicates-keep', help='a comma separated list of sample IDs', required=True)
	requiredArgs.add_argument('--famsize-keep', help='a comma separated list of sample IDs', required=True)
	requiredArgs.add_argument('--sampleqc-keep', help='a comma separated list of sample IDs', required=True)
	requiredArgs.add_argument('--sexcheck-keep', help='a comma separated list of sample IDs', required=True)
	requiredArgs.add_argument('--out', help='an output filename', required=True)
	args = parser.parse_args()
	main(args)
