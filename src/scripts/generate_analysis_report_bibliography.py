import argparse

def main(args=None):

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		if args.acknowledgements != "":

			print "writing acknowledgements section"
			f.write("\n"); f.write(r"\clearpage"); f.write("\n")
			f.write("\n"); f.write(r"\section{Acknowledgements}"); f.write("\n")
			
			text = [r"We would like to acknowledge the following people for their significant contributions to this work.", "", r"\bigskip", ""]
			i = 0
			for name in args.acknowledgements.split(","):
				i = i + 1
				if i == 1:
					text.extend([r"\noindent " + name.replace("_","\_") + r" \\"])
				else:
					text.extend([name.replace("_","\_") + r" \\"])
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		print "writing bibliography section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\begin{thebibliography}{}"); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{plink} Plink1.9, \url{https://www.cog-genomics.org/plink2}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{liftover} Kent WJ, Sugnet CW, Furey TS, Roskin KM, Pringle TH, Zahler AM, Haussler D. The human genome browser at UCSC. Genome Res. 2002 Jun;12(6):996-1006. LiftOver \url{http://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64/liftOver}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{pcair} Conomos MP. GENetic EStimation and Inference in Structured samples (GENESIS): Statistical methods for analyzing genetic data from samples with population structure and/or relatedness, \url{https://www.rdocumentation.org/packages/GENESIS/versions/2.2.2}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{king} \url{http://people.virginia.edu/~wc9c/KING/}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{1KG} 1000 Genomes Phase 3 v5, \url{https://mathgen.stats.ox.ac.uk/impute/1000GP_Phase3.html}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{hail} Seed C, Bloemendal A, Bloom JM, Goldstein JI, King D, Poterba T, Neale BM. Hail: An Open-Source Framework for Scalable Genetic Data Analysis. In preparation. \url{https://github.com/hail-is/hail}."); f.write("\n")

		citations = list(set(args.known_loci_citations.split(",,,")))
		citations = [c for c in citations if c != '']
		for c in citations:
			tag = c.split("___")[0]
			text = c.split("___")[1]
			f.write("\n"); f.write(r"\bibitem{" + tag.replace("_","\_") + r"} " + text.replace("_","\_")); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{Loamstream} Gilbert C, Ruebenacker O, Koesterer R, Massung J, Flannick J. Loamstream. " + args.loamstream_version.replace("_","\_") + r". \url{https://github.com/broadinstitute/dig-loam-stream}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{Pipeline} Koesterer R, Gilbert C, Ruebenacker O, Massung J, Flannick J. AMP-DCC Data Analysis Pipeline. " + args.pipeline_version.replace("_","\_") + r". \url{https://github.com/broadinstitute/dig-loam}."); f.write("\n")

		text = r"\ExecuteMetaData[\currfilebase.input]{bibliography}"
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\end{thebibliography}"); f.write("\n")
		f.write("\n"); f.write(r"\end{document}"); f.write("\n")

	with open(args.out_input,'w') as f:

		f.write("\n".join(["",r"%<*bibliography>","%</bibliography>"]).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--known-loci-citations', help='a 3 comma separated list of tags and citations, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--acknowledgements', help='a comma separated list of names for acknowledgement', required=True)
	requiredArgs.add_argument('--loamstream-version', help='a loamstream version', required=True)
	requiredArgs.add_argument('--pipeline-version', help='a pipeline version', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
