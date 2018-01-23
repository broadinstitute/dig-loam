import argparse
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Data}"); f.write("\n")

		text=r"""\begin{figure}[H]
					\centering
					\includegraphics[width=0.75\linewidth]{""" + args.samples_upset_diagram + r"""}
					\caption{Samples remaining for analysis after quality control}
					\label{fig:samplesUpsetDiagram}
				\end{figure}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{figure}[H]
					\centering
					\includegraphics[width=0.75\linewidth]{""" + args.variants_upset_diagram + r"""}
					\caption{Variants remaining for analysis after quality control}
					\label{fig:variantsUpsetDiagram}
				\end{figure}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text = r"""\ExecuteMetaData[\currfilebase.input]{data}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		text = ["",r"%<*data>","%</data>"]
		f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	requiredArgs.add_argument('--samples-upset-diagram', help='an upset diagram for samples', required=True)
	requiredArgs.add_argument('--variants-upset-diagram', help='an upset diagram for harmonized variants', required=True)
	args = parser.parse_args()
	main(args)
