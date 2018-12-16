import pandas as pd
import argparse
from pandas.io.common import EmptyDataError

def main(args=None):

	print "reading ancestry inferred file"
	x=pd.read_table(args.ancestry_inferred,header=None)
	x[0] = x[0].astype(str)
	exc=x[0][x[1] == "OUTLIERS"].tolist()
	if not args.ancestry_keep is None:
		print "reinstating samples in ancestry keep list"
		exc=[a for a in exc if a not in args.ancestry_keep.split(",")]
	final=exc

	print "reading kinship related file"
	try:
		x=pd.read_table(args.kinship_related)
	except EmptyDataError:
		pass
	else:
		x['ID1'] = x['ID1'].astype(str)
		x['ID2'] = x['ID2'].astype(str)
		x=x[x['Kinship'] >= 0.4]
		if x.shape[0] > 0:
			exc=x['ID1'].tolist()
			exc.extend(x['ID2'].tolist())
			if not args.duplicates_keep is None:
				print "reinstating samples in duplicates keep list"
				exc=[a for a in exc if a not in args.duplicates_keep.split(",")]
			final.extend(exc)

	print "reading kinship famsizes file"
	try:
		x=pd.read_table(args.kinship_famsizes,header=None)
	except EmptyDataError:
		pass
	else:
		x[0] = x[0].astype(str)
		exc=x[0][x[1] >= 10].tolist()
		if len(exc) > 0:
			if not args.famsize_keep is None:
				print "reinstating samples in famsize keep list"
				exc=[a for a in exc if a not in args.famsize_keep.split(",")]
			final.extend(exc)

	print "reading sampleqc outliers file"
	try:
		x=pd.read_table(args.sampleqc_outliers)
	except EmptyDataError:
		pass
	else:
		x['IID'] = x['IID'].astype(str)
		exc=x['IID'].tolist()
		if len(exc) > 0:
			if not args.sampleqc_keep is None:
				print "reinstating samples in sampleqc keep list"
				exc=[a for a in exc if a not in args.sampleqc_keep.split(",")]
			final.extend(exc)

	print "reading sexcheck problems file"
	try:
		x=pd.read_table(args.sexcheck_problems)
	except EmptyDataError:
		pass
	else:
		x['IID'] = x['IID'].astype(str)
		exc=x['IID'].tolist()
		if len(exc) > 0:
			if not args.sexcheck_keep is None:
				print "reinstating samples in sexcheck keep list"
				exc=[a for a in exc if a not in args.sexcheck_keep.split(",")]
			final.extend(exc)

	print "writing final sample exclusions to file"
	final=list(set(final))
	with open(args.out, 'w') as out:
		out.write("\n".join(final))

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--ancestry-inferred', help='file ending in .ancestry.inferred.tsv', required=True)
	requiredArgs.add_argument('--kinship-related', help='file ending in .kinship.kin0.related', required=True)
	requiredArgs.add_argument('--kinship-famsizes', help='file ending in .kinship.famsizes.tsv', required=True)
	requiredArgs.add_argument('--sampleqc-outliers', help='file ending in .sampleqc.outliers.tsv', required=True)
	requiredArgs.add_argument('--sexcheck-problems', help='file ending in .sexcheck.problems.tsv', required=True)
	requiredArgs.add_argument('--ancestry-keep', help='comma separated list of ancestry flagged sample IIDs to keep', required=True, nargs='?')
	requiredArgs.add_argument('--duplicates-keep', help='comma separated list of duplicate sample IIDs to keep', required=True, nargs='?')
	requiredArgs.add_argument('--famsize-keep', help='comma separated list of famsize flagged sample IIDs to keep', required=True, nargs='?')
	requiredArgs.add_argument('--sampleqc-keep', help='comma separated list of sampleqc flagged sample IIDs to keep', required=True, nargs='?')
	requiredArgs.add_argument('--sexcheck-keep', help='comma separated list of sexcheck flagged sample IIDs to keep', required=True, nargs='?')
	requiredArgs.add_argument('--out', help='filename for final sample exclusions list', required=True)
	args = parser.parse_args()
	main(args)
