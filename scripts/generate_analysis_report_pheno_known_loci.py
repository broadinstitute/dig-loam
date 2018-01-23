import argparse
import numpy as np
import pandas as pd
import collections
from scipy.stats import binom_test

def main(args=None):

	top = collections.OrderedDict()
	for s in args.top_known_loci.split(","):
		top[s.split("___")[0]] = s.split("___")[1]

	tag = collections.OrderedDict()
	for s in args.tag.split(","):
		tag[s.split("___")[0]] = s.split("___")[1]

	desc = collections.OrderedDict()
	for s in args.desc.split(",,,"):
		desc[s.split("___")[0]] = s.split("___")[1]

	result_cols = ['chr','pos','id','alt','ref','n','case','ctrl','af','beta','se','or','pval','cohort','gene','r2','id_known','n_known','case_known','ctrl_known','beta_known','se_known','or_known','pval_known']
	report_cols = ['CHR','POS','ID','EA','OA','N','CASE','CTRL','FREQ','EFFECT','STDERR','OR','P','COHORT',r"GENE\textsubscript{CLOSEST}",r"R\textsuperscript{2}",r"ID\textsubscript{KNOWN}",r"N\textsubscript{KNOWN}",r"CASE\textsubscript{KNOWN}",r"CTRL\textsubscript{KNOWN}",r"EFFECT\textsubscript{KNOWN}",r"STDERR\textsubscript{KNOWN}",r"OR\textsubscript{KNOWN}",r"P\textsubscript{KNOWN}"]
	cols = dict(zip(result_cols,report_cols))
	types = {'chr': 'string type', 'pos': 'string type', 'id': 'string type', 'alt': 'verb string type', 'ref': 'verb string type', 'gene': 'string type', 'cohort': 'verb string type', 'dir': 'verb string type', 'id_known': 'string type'}

	with open(args.out_input,'w') as fin:

		## open latex file for writing
		with open(args.out_tex,'w') as f:
	
			print "writing top associations section"
			f.write("\n"); f.write(r"\subsection{Previously identified risk loci}"); f.write("\n")
	
			for model in top:
	
				# read in top results
				df = pd.read_table(top[model],sep="\t")
				df = df[[c for c in result_cols if c in df.columns]]
	
				text = []
				text.extend([r"\begin{table}[H]",
					r"   \begin{center}",
					r"   \caption{Top known loci in " + model.replace(r'_',r'\_') + r" (\textbf{bold} variants indicate matching direction of effect)}",
					r"   \resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%",
					r"      \pgfplotstabletypeset[",
					r"         font=\footnotesize,",
					r"         col sep=tab,",
					r"         columns={" + ",".join(df.columns.tolist()) + r"},",
					r"         column type={>{\fontseries{bx}\selectfont}c},"])
				for c in df.columns.tolist():
					if c in types:
						text.extend([r"         columns/" + c + r"/.style={column name=" + cols[c] + r", " + types[c] + r"},"])
					else:
						text.extend([r"         columns/" + c + r"/.style={column name=" + cols[c] + r"},"])
				text.extend([r"         postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
					r"         every head row/.style={before row={\toprule}, after row={\midrule}},",
					r"         every last row/.style={after row=\bottomrule}",
					r"         ]{" + top[model] + r"}}",
					r"   \label{table:topLoci" + args.pheno_name + r"}",
					r"   \end{center}",
					r"\end{table}"])
				f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

				text = r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_long_name.replace(" ","-") + r"-top-known-loci-in-" + model.replace("_","-") + r"}"
				f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

				nSig = df[df['pval'] <= 0.05].shape[0]
				nMatch = df[df['r2'] == 1].shape[0]
				nSame = df[(df['id'].str.match("\\\large")) & (df['r2'] == 1)].shape[0]
				binom = binom_test(nSame,nMatch,alternative="greater")

				text = ["",r"%<*"  + args.pheno_long_name.replace(" ","-") + r"-top-known-loci-in-" + model.replace("_","-") + r">"]
				text_insert = r"Table \ref{table:topLoci" + args.pheno_name + r"} shows statistics from the " + model.replace("_","\_") + r" model for " + str(df.shape[0]) + r" loci that were shown to be significantly associated with " + args.pheno_long_name.replace("_","\_") + r" in the " + desc[model].replace("_","\_") + r" \cite{" + tag[model] + r"}. Where a previously reported variant was not genotyped in the study (indicated by $\bar{R\textsuperscript{2}} < 1$), if available, a tagging variant in LD with the reported variant ($\bar{R\textsuperscript{2}} >= 0.7$ and within 250kb) was provided. Tags were identified using 1000 Genomes data."
				if nSig == 0:
					text_insert = text_insert + r" None of the variants shows even nominal significance ($p < 0.05$) in this study."
				else:
					text_insert = text_insert + r" There are " + str(nSig) + r" variants that show at least nominal significance ($p < 0.05$) in this study."
				text_insert = text_insert + r" Out of the " + str(nMatch) + r" variants in both studies, " + str(nSame) + r" exhibit the same direction of effect with the known result (binomial test $p = " + '{:.3g}'.format(binom) + r"$)."
				text.extend([text_insert])
				text.extend(["%</"  + args.pheno_long_name.replace(" ","-") + r"-top-known-loci-in-" + model.replace("_","-") + r">"])
				fin.write("\n".join(text).encode('utf-8')); fin.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--top-known-loci', help='a comma separated list of models and top known loci results files (aligned to risk allele and gene annotated), each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pheno-name', help='a column name for phenotype', required=True)
	requiredArgs.add_argument('--pheno-long-name', help='a full phenotype name', required=True)
	requiredArgs.add_argument('--desc', help='a comma separated list of models and descriptions of papers (each separated by 3 underscores)', required=True)
	requiredArgs.add_argument('--tag', help='a comma separated list of models and citation tags of papers (each separated by 3 underscores)', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
