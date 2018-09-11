import argparse
import numpy as np
import pandas as pd
import collections
from scipy.stats import binom_test

def main(args=None):

	top = collections.OrderedDict()
	for s in args.top_known_loci:
		ss = s.split(",")
		if len(ss) > 4:
			ss[1] = " ".join([ss[0],ss[1]])
			del ss[0]
		if ss[2] == "":
			ss[2] = "Unadjusted"
		else:
			ss[2] = "Adjusted " + ss[2]
		if ss[1] != "":
			id = ss[1] + " " + ss[2]
		else:
			id = ss[2]
		if not ss[0] in top:
			top[ss[0]] = collections.OrderedDict()
		top[ss[0]][id] = ss[3]

	tag = collections.OrderedDict()
	for s in args.tag:
		ss = s.split(",")
		if len(ss) > 4:
			ss[1] = " ".join([ss[0],ss[1]])
			del ss[0]
		if ss[2] == "":
			ss[2] = "Unadjusted"
		else:
			ss[2] = "Adjusted " + ss[2]
		if ss[1] != "":
			id = ss[1] + " " + ss[2]
		else:
			id = ss[2]
		if not ss[0] in tag:
			tag[ss[0]] = collections.OrderedDict()
		tag[ss[0]][id] = ss[3]

	desc = collections.OrderedDict()
	for s in args.desc.split(",,,"):
		ss = s.split("___")
		if len(ss) > 4:
			ss[1] = " ".join([ss[0],ss[1]])
			del ss[0]
		if ss[2] == "":
			ss[2] = "Unadjusted"
		else:
			ss[2] = "Adjusted " + ss[2]
		if ss[1] != "":
			id = ss[1] + " " + ss[2]
		else:
			id = ss[2]
		if not ss[0] in desc:
			desc[ss[0]] = collections.OrderedDict()
		desc[ss[0]][id] = ss[3]

	result_cols = ['chr','pos','id','alt','ref','n','case','ctrl','af','afavg','afmin','afmax','beta','se','or','pval','dir','cohort','gene','r2','id_known','n_known','case_known','ctrl_known','beta_known','se_known','or_known','pval_known']
	report_cols = ['CHR','POS','ID','EA','OA','N','CASE','CTRL','FREQ','FREQ\textsubscript{AVG}','FREQ\textsubscript{MIN}','FREQ\textsubscript{MAX}','EFFECT','STDERR','OR','P','DIR','COHORT',r"GENE\textsubscript{CLOSEST}",r"R\textsuperscript{2}",r"ID\textsubscript{KNOWN}",r"N\textsubscript{KNOWN}",r"CASE\textsubscript{KNOWN}",r"CTRL\textsubscript{KNOWN}",r"EFFECT\textsubscript{KNOWN}",r"STDERR\textsubscript{KNOWN}",r"OR\textsubscript{KNOWN}",r"P\textsubscript{KNOWN}"]
	cols = dict(zip(result_cols,report_cols))
	types = {
		'chr': 'string type', 
		'pos': 'string type', 
		'id': 'string type', 
		'alt': 'verb string type', 
		'ref': 'verb string type', 
		'gene': 'string type', 
		'cohort': 'verb string type', 
		'dir': 'verb string type',
		'id_known': 'string type',
		'af': 'precision = 3, sci precision = 2',
		'afavg': 'precision = 3, sci precision = 2',
		'afmin': 'precision = 3, sci precision = 2',
		'afmax': 'precision = 3, sci precision = 2',
		'beta': 'precision = 3, sci precision = 2',
		'se': 'precision = 3, sci precision = 2',
		'or': 'precision = 3, sci precision = 2',
		'pval': 'precision = 3, sci precision = 2',
		'r2': 'precision = 3, sci precision = 2',
		'id_known': 'string type',
		'beta_known': 'precision = 3, sci precision = 2',
		'se_known': 'precision = 3, sci precision = 2',
		'or_known': 'precision = 3, sci precision = 2',
		'pval_known': 'precision = 3, sci precision = 2'
	}

	## open files for writing
	with open(args.out_input,'w') as fin:
		with open(args.out_tex,'w') as f:
		
			print "writing top associations section"
			f.write("\n"); f.write(r"\subsection{Previously identified risk loci}"); f.write("\n")
	
			f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Known-Loci}".encode('utf-8')); f.write("\n")

			fin.write("\n"); fin.write("\n".join([r"%<*" + args.pheno_name.replace("_","-") + r"-Known-Loci>", "%</" + args.pheno_name.replace("_","-") + r"-Known-Loci>"]).encode('utf-8')); fin.write("\n")
		
			for cohort in top:
	
				f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Known-Loci-" + cohort.replace("_","-") + r"}".encode('utf-8')); f.write("\n")

				fin.write("\n"); fin.write("\n".join([r"%<*" + args.pheno_name.replace("_","-") + r"-Known-Loci-" + cohort.replace("_","-") + r">", "%</" + args.pheno_name.replace("_","-") + r"-Known-Loci-" + cohort.replace("_","-") + r">"]).encode('utf-8')); fin.write("\n")
	
				for model in top[cohort]:
	
					f.write("\n"); f.write(r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_name.replace("_","-") + r"-Known-Loci-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r"}".encode('utf-8')); f.write("\n")
		
					# read in top results
					df = pd.read_table(top[cohort][model],sep="\t")
					df = df[[c for c in result_cols if c in df.columns]]
		
					text = []
					text.extend([
								r"\begin{table}[H]",
								r"	\begin{center}",
								r"	\caption{Top known loci in " + cohort.replace("_","\_") + " model " + model.replace("_","\_") + r" (\textbf{bold} variants indicate matching direction of effect)}",
								r"	\label{table:" + args.pheno_name.replace("_","-") + r"-Known-Loci-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r"}",
								r"	\resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%",
								r"	\pgfplotstabletypeset[",
								r"		font=\footnotesize,",
								r"		col sep=tab,",
								r"		columns={" + ",".join(df.columns.tolist()) + r"},",
								r"		column type={>{\fontseries{bx}\selectfont}c},"])
					for c in df.columns.tolist():
						if c in types:
							text.extend([
								r"		columns/" + c + r"/.style={column name=" + cols[c] + r", " + types[c] + r"},"])
						else:
							text.extend([
								r"		columns/" + c + r"/.style={column name=" + cols[c] + r"},"])
					text.extend([
								r"		postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
								r"		every head row/.append style={",
								r"			before row=",
								r"				\toprule,",
								r"			after row=",
								r"				\midrule",
								r"		},",
								r"		every last row/.style={",
								r"			after row=",
								r"				\bottomrule",
								r"		},",
								r"		empty cells with={}",
								r"		]{" + top[cohort][model] + r"}}",
								r"	\end{center}",
								r"\end{table}"])
					f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

					nSig = df[df['pval'] <= 0.05].shape[0]
					nMatch = df[df['r2'] == 1].shape[0]
					nSame = df[(df['id'].str.match("\\\large")) & (df['r2'] == 1)].shape[0]
					binom = binom_test(nSame,nMatch,alternative="greater")

					text = [r"%<*" + args.pheno_name.replace("_","-") + r"-Known-Loci-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r">"]
					text_insert = r"Table \ref{table:" + args.pheno_name.replace("_","-") + r"-Known-Loci-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r"} shows statistics from the " + cohort.replace("_","\_") + r" cohort for " + str(df.shape[0]) + r" loci that were shown to be significantly associated with " + args.pheno_long_name.replace("_","\_") + r" in the " + desc[cohort][model].replace("_","\_") + r" \cite{" + tag[cohort][model].replace("_","\_") + r"}. Where a previously reported variant was not genotyped in the study (indicated by $\bar{R\textsuperscript{2}} < 1$), if available, a tagging variant in LD with the reported variant ($\bar{R\textsuperscript{2}} >= 0.7$ and within 250kb) was provided. Tags were identified using 1000 Genomes data."
					if nSig == 0:
						text_insert = text_insert + r" None of the variants shows even nominal significance ($p < 0.05$) in this study."
					else:
						text_insert = text_insert + r" There are " + str(nSig) + r" variants that show at least nominal significance ($p < 0.05$) in this study."
					text_insert = text_insert + r" Out of the " + str(nMatch) + r" variants in both studies, " + str(nSame) + r" exhibit the same direction of effect with the known result (binomial test $p = " + '{:.3g}'.format(binom) + r"$)."
					text.extend([text_insert])
					text.extend(["%</"  + args.pheno_name.replace("_","-") + r"-Known-Loci-" + cohort.replace("_","-") + "-" + model.replace("_","-").replace("+","-").replace(" ","-") + r">"])
					fin.write("\n"); fin.write("\n".join(text).encode('utf-8')); fin.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--top-known-loci', nargs='+', help='a comma separated list of models and top known loci results files (aligned to risk allele and gene annotated), each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pheno-name', help='a column name for phenotype', required=True)
	requiredArgs.add_argument('--pheno-long-name', help='a full phenotype name', required=True)
	requiredArgs.add_argument('--desc', help='a three comma separated list of models and descriptions of papers (each separated by 3 underscores)', required=True)
	requiredArgs.add_argument('--tag', nargs='+', help='a comma separated list of models and citation tags of papers (each separated by 3 underscores)', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
