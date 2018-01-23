import pandas as pd
import argparse

def main(args=None):

	print "reading remove files"
	remove_list = []
	for f in args.remove_in.split(","):
		with open(f) as fin:
			remove_list = remove_list + fin.read().splitlines()

	print "reading remove mono files"
	remove_mono_list = []
	for f in args.remove_mono_in.split(","):
		with open(f) as fin:
			remove_mono_list = remove_mono_list + fin.read().splitlines()

	print "reading remove nomatch files"
	remove_nomatch_list = []
	for f in args.remove_nomatch_in.split(","):
		with open(f) as fin:
			remove_nomatch_list = remove_nomatch_list + fin.read().splitlines()

	print "reading ignore files"
	ignore_list = []
	for f in args.ignore_in.split(","):
		with open(f) as fin:
			ignore_list = ignore_list + fin.read().splitlines()

	print "reading flip files"
	flip_list = []
	for f in args.flip_in.split(","):
		with open(f) as fin:
			flip_list = flip_list + fin.read().splitlines()

	print "reading force a1 files"
	force_a1_list = []
	for f in args.force_a1_in.split(","):
		with open(f) as fin:
			force_a1_list = force_a1_list + fin.read().splitlines()

	print "reading snp log files"
	snp_log_df = pd.DataFrame({'chr': [], 'pos': [], 'id': [], 'alleles': [], 'action': [], 'message': []})
	for f in args.snp_log_in.split(","):
		df = pd.read_table(f, sep="\t", dtype=str)
		if df.shape[0] > 0:
			snp_log_df = snp_log_df.append(df, ignore_index=True)

	print "writing merged files"
	with open(args.remove_out, 'w') as out:
		out.write("\n".join(remove_list))

	with open(args.remove_mono_out, 'w') as out:
		out.write("\n".join(remove_mono_list))

	with open(args.remove_nomatch_out, 'w') as out:
		out.write("\n".join(remove_nomatch_list))

	with open(args.ignore_out, 'w') as out:
		out.write("\n".join(ignore_list))

	with open(args.flip_out, 'w') as out:
		out.write("\n".join(flip_list))

	with open(args.force_a1_out, 'w') as out:
		out.write("\n".join(force_a1_list))

	snp_log_df[['chr', 'pos', 'id', 'alleles', 'action', 'message']].to_csv(args.snp_log_out, index=False, header=True, sep="\t")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--remove-in', help='a comma separated list of files', required=True)
	requiredArgs.add_argument('--remove-mono-in', help='a comma separated list of files', required=True)
	requiredArgs.add_argument('--remove-nomatch-in', help='a comma separated list of files', required=True)
	requiredArgs.add_argument('--ignore-in', help='a comma separated list of files', required=True)
	requiredArgs.add_argument('--flip-in', help='a comma separated list of files', required=True)
	requiredArgs.add_argument('--force-a1-in', help='a comma separated list of files', required=True)
	requiredArgs.add_argument('--snp-log-in', help='a comma separated list of files', required=True)
	requiredArgs.add_argument('--remove-out', help='an output filename', required=True)
	requiredArgs.add_argument('--remove-mono-out', help='an output filename', required=True)
	requiredArgs.add_argument('--remove-nomatch-out', help='an output filename', required=True)
	requiredArgs.add_argument('--ignore-out', help='an output filename', required=True)
	requiredArgs.add_argument('--flip-out', help='an output filename', required=True)
	requiredArgs.add_argument('--force-a1-out', help='an output filename', required=True)
	requiredArgs.add_argument('--snp-log-out', help='an output filename', required=True)
	args = parser.parse_args()
	main(args)
