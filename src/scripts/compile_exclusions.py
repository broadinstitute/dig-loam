import pandas as pd
import argparse
from pandas.io.common import EmptyDataError

def main(args=None):

	print "reading samples restore table"
	restore=pd.read_table(args.restore)

	print "reading ancestry inferred file"
	x=pd.read_table(args.ancestry_inferred,header=None)
	n_samples = x.shape[0]
	x[0] = x[0].astype(str)
	exc=x[0][x[1] == "OUTLIERS"].tolist()
	restore_temp = restore[restore['RestoreFrom'] == "ancestryOutliersKeep"]
	if restore_temp.shape[0] > 0:
		print "restoring " + str(restore_temp.shape[0]) + " samples from ancestryOutliersKeep list"
		exc=[a for a in exc if a not in restore_temp['IID'].tolist()]
	final=exc

	print "reading sample qc stats file"
	stats_df=pd.read_table(args.sampleqc_stats)

	print "reading kinship related file"
	try:
		x=pd.read_table(args.kinship_related)
	except EmptyDataError:
		pass
	else:
		if 'Kinship' in x: x = x.rename(columns={'Kinship': 'KINSHIP'})
		if 'kinship' in x: x = x.rename(columns={'kinship': 'KINSHIP'})
		exc = []
		x['ID1'] = x['ID1'].astype(str)
		x['ID2'] = x['ID2'].astype(str)
		x=x[x['KINSHIP'] >= 0.4]
		x=x[(~x['ID1'].isin(final)) & (~x['ID2'].isin(final))]
		if x.shape[0] > 0:
			print str(x.shape[0]) + " duplicate pairs found!"
			x_remaining = x.copy()
			for i, r in x.iterrows():
				if x_remaining.shape[0] > 0:
					id1_cr = stats_df.loc[stats_df['IID'] == r['ID1'],'call_rate'].values[0]
					id2_cr = stats_df.loc[stats_df['IID'] == r['ID2'],'call_rate'].values[0]
					if id1_cr < id2_cr:
						x_remove = r['ID1']
						print "... KINSHIP(" + r['ID1'] + ", " + r['ID2'] + ") = " + str(r['KINSHIP']) + ", call_rate(" + r['ID1'] + ") = " + str(id1_cr) + " < call_rate(" + r['ID2'] + ") = " + str(id2_cr) + ", removing " + r['ID1']
					else:
						x_remove = r['ID2']
						print "... KINSHIP(" + r['ID1'] + ", " + r['ID2'] + ") = " + str(r['KINSHIP']) + ", call_rate(" + r['ID2'] + ") = " + str(id2_cr) + " <= call_rate(" + r['ID1'] + ") = " + str(id1_cr) + ", removing " + r['ID2']
					x_remaining = x_remaining[x_remaining['ID1'] != x_remove]
					exc.extend([x_remove])
			restore_temp = restore[restore['RestoreFrom'] == "duplicatesKeep"]
			if restore_temp.shape[0] > 0:
				print "restoring " + str(restore_temp.shape[0]) + " samples from duplicatesKeep list"
				exc=[a for a in exc if a not in restore_temp['IID'].tolist()]
			final.extend(exc)

	print "reading kinship famsizes file"
	try:
		x=pd.read_table(args.kinship_famsizes,header=None)
	except EmptyDataError:
		pass
	else:
		x[0] = x[0].astype(str)
		exc=x[0][x[1] >= 0.5 * n_samples].tolist()
		if len(exc) > 0:
			restore_temp = restore[restore['RestoreFrom'] == "famsizeKeep"]
			if restore_temp.shape[0] > 0:
				print "restoring " + str(restore_temp.shape[0]) + " samples from famsizeKeep list"
				exc=[a for a in exc if a not in restore_temp['IID'].tolist()]
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
			restore_temp = restore[restore['RestoreFrom'] == "sampleqcKeep"]
			if restore_temp.shape[0] > 0:
				print "restoring " + str(restore_temp.shape[0]) + " samples from sampleqcKeep list"
				exc=[a for a in exc if a not in restore_temp['IID'].tolist()]
			final.extend(exc)

	print "reading sampleqc incomplete observations file"
	try:
		x=pd.read_table(args.sampleqc_incomplete_obs)
	except EmptyDataError:
		pass
	else:
		x['IID'] = x['IID'].astype(str)
		exc=x['IID'].tolist()
		if len(exc) > 0:
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
			restore_temp = restore[restore['RestoreFrom'] == "sexcheckKeep"]
			if restore_temp.shape[0] > 0:
				print "restoring " + str(restore_temp.shape[0]) + " samples from sexcheckKeep list"
				exc=[a for a in exc if a not in restore_temp['IID'].tolist()]
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
	requiredArgs.add_argument('--sampleqc-stats', help='file ending in .sampleqc.stats.tsv', required=True)
	requiredArgs.add_argument('--kinship-famsizes', help='file ending in .kinship.famsizes.tsv', required=True)
	requiredArgs.add_argument('--sampleqc-outliers', help='file ending in .sampleqc.outliers.tsv', required=True)
	requiredArgs.add_argument('--sampleqc-incomplete-obs', help='file ending in .sampleqc.incomplete_obs.tsv', required=True)
	requiredArgs.add_argument('--sexcheck-problems', help='file ending in .sexcheck.problems.tsv', required=True)
	requiredArgs.add_argument('--restore', help='a sample restore table', required=True)
	requiredArgs.add_argument('--out', help='filename for final sample exclusions list', required=True)
	args = parser.parse_args()
	main(args)
