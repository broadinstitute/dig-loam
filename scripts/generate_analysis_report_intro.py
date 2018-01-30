import argparse
import pysam
import numpy as np
import pandas as pd

header=r"""\documentclass[11pt]{article}
\usepackage[top=1in,bottom=1in,left=0.75in,right=0.75in]{geometry}
\renewcommand{\familydefault}{\sfdefault}
\usepackage{lmodern}
\usepackage{bm}
\usepackage[T1]{fontenc}
\usepackage[toc,page]{appendix}
\usepackage{graphicx}
\usepackage{grffile}
\usepackage{caption}
\usepackage{subcaption}
\usepackage{float}
\usepackage{microtype}
\DisableLigatures{encoding = *, family = *}
\usepackage{booktabs}
\usepackage{pgfplotstable}
\usepackage{fixltx2e}
\usepackage[colorlinks=true,urlcolor=blue,linkcolor=black]{hyperref}
\usepackage{fancyhdr}
\usepackage{mathtools}
\usepackage[nottoc,numbib]{tocbibind}
\usepackage{color}
\usepackage{colortbl}
\usepackage{enumitem}
\usepackage{currfile}
\usepackage{catchfilebetweentags}
\usepackage[export]{adjustbox}
%\setcounter{section}{-1}
\pagestyle{fancy}
%\fancyhf{}
\renewcommand{\sectionmark}[1]{\markboth{#1}{\thesection.\ #1}}
\renewcommand{\subsectionmark}[1]{\markright{\thesubsection.\ #1}}
\lhead{\fancyplain{}{\nouppercase{\leftmark}}} % 1. sectionname
\rhead{\fancyplain{}{\nouppercase{\rightmark}}} % 1. sectionname
\cfoot{\fancyplain{}{\thepage}}
\def \hfillx {\hspace*{-\textwidth} \hfill}
\definecolor{Gray}{gray}{0.9}
\makeatletter
   \setlength\@fptop{0\p@}
\makeatother
\usepackage{placeins}
\let\Oldsection\section
\renewcommand{\section}{\FloatBarrier\Oldsection}
\let\Oldsubsection\subsection
\renewcommand{\subsection}{\FloatBarrier\Oldsubsection}
\let\Oldsubsubsection\subsubsection
\renewcommand{\subsubsection}{\FloatBarrier\Oldsubsubsection}
\captionsetup[table]{singlelinecheck=off}
\linespread{1.3}
"""


def main(args=None):

	## open latex file for writing
	print ""
	with open(args.out_tex,'w') as f:

		## begin document
		f.write(header); f.write("\n")
		f.write("\n"); f.write(r"\begin{document}"); f.write("\n")

		## title page
		f.write("\n"); f.write(r"\title{AMP-DCC Data Analysis Report \\")
		f.write("\n"); f.write(args.id.replace("_","\_") + r" \\")
		f.write("\n"); f.write("Phase " + args.phase + "}"); f.write("\n")
		f.write("\n"); f.write(r"\maketitle"); f.write("\n")

		if len(args.authors.split(",")) == 1:
			authors = args.authors
		else:
			a = args.authors.split(",")
			authors = a[0]
			for author in a[1:]:
				if author == a[-1]:
					authors = authors + " and " + author
				else:
					authors = authors + ", " + author

		f.write("\n"); f.write("Prepared by " + authors + " on behalf of the AMP-DCC Analysis Team"); f.write("\n")
		f.write("\n"); f.write(r"\bigskip"); f.write("\n")
		f.write("\n"); f.write(r"Contact: AMP-DCC Analysis Team (\href{mailto:amp-dcc-dat@broadinstitute.org}{amp-dcc-dat@broadinstitute.org})"); f.write("\n")

		## table of contents
		f.write("\n"); f.write(r"\tableofcontents"); f.write("\n")

		print "writing introduction"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Introduction}"); f.write("\n")

		text = r"""\ExecuteMetaData[\currfilebase.input]{introduction}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		text = [r"%<*introduction>","%</introduction>"]
		f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--id', help='a project ID', required=True)
	requiredArgs.add_argument('--phase', help='an analysis phase', required=True)
	requiredArgs.add_argument('--authors', help='a comma separated list of authors', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
