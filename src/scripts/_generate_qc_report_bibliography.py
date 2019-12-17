import argparse

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		if args.acknowledgements:
	
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

		print "writing biliography section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\begin{thebibliography}{}"); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{plink} Plink1.9, \url{https://www.cog-genomics.org/plink2}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{genotypeHarmonizer} Deelan P, Bonder MJ, Joeri van der Velde K, Westra HJ, Winder E, Hendriksen D, Franke L, Swertz MA. Genotype harmonizer: automatic strand alignment and format conversion for genotype data integration. BMC Research Notes; 2014. 7:901. doi:10.1186/1756-0500-7-901. \url{https://github.com/molgenis/systemsgenetics/wiki/Genotype-Harmonizer}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{pcair} Conomos MP. GENetic EStimation and Inference in Structured samples (GENESIS): Statistical methods for analyzing genetic data from samples with population structure and/or relatedness, \url{https://www.rdocumentation.org/packages/GENESIS/versions/2.2.2}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{1KG} 1000 Genomes Phase 3 v5, \url{https://mathgen.stats.ox.ac.uk/impute/1000GP_Phase3.html}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{klustakwik} Klustakwik, \url{http://klustakwik.sourceforge.net/}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{umichHiLd} \url{http://genome.sph.umich.edu/wiki/Regions\_of\_high\_linkage\_disequilibrium\_(LD)}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{humref} \url{https://www.ncbi.nlm.nih.gov/grc/human/data?asm=GRCh37}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{king} \url{http://people.virginia.edu/~wc9c/KING/}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{hail} Seed C, Bloemendal A, Bloom JM, Goldstein JI, King D, Poterba T, Neale BM. Hail: An Open-Source Framework for Scalable Genetic Data Analysis. In preparation. \url{https://github.com/hail-is/hail}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{Loamstream} Gilbert C, Ruebenacker O, Koesterer R, Massung J, Flannick J. Loamstream. " + args.loamstream_version.replace("_","\_") + r". \url{https://github.com/broadinstitute/dig-loam-stream}."); f.write("\n")

		f.write("\n"); f.write(r"\bibitem{Pipeline} Koesterer R, Gilbert C, Ruebenacker O, Massung J, Flannick J. AMP-DCC Data Analysis Pipeline. " + args.pipeline_version.replace("_","\_") + r". \url{https://github.com/broadinstitute/dig-loam}."); f.write("\n")

		f.write("\n"); f.write(r"\end{thebibliography}"); f.write("\n")
		f.write("\n"); f.write(r"\end{document}"); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--acknowledgements', help='a comma separated list of names for acknowledgement')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--loamstream-version', help='a loamstream version', required=True)
	requiredArgs.add_argument('--pipeline-version', help='a pipeline version', required=True)
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	args = parser.parse_args()
	main(args)
