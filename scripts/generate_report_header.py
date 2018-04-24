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
\usepackage{longtable}
\usepackage{threeparttable}
\usepackage{threeparttablex}
\usepackage{needspace}
\newcommand{\needlines}[1]{\Needspace*{#1\baselineskip}}
%\usepackage{underscore}
\usepackage[english]{babel}
\pgfplotstableset{
	highlight/.append style={
		postproc cell content/.append code={
			\pgfkeysalso{@cell content=\textbf{##1}}
		}
	},
	highlightrowskipcol1/.append style={
		postproc cell content/.append code={
			\count0=\pgfplotstablerow
			\advance\count0 by1
			\ifnum\count0=#1
				\count1=\pgfplotstablecol
				\advance\count1 by1
				\ifnum\count1>1
					\pgfkeysalso{@cell content=\textbf{##1}}
				\fi
			\fi
		}
	},
	grayrow/.append style={
		postproc cell content/.append code={
			\count0=\pgfplotstablerow
			\advance\count0 by1
			\ifnum\count0=#1
				\pgfkeysalso{@cell content/.add={\cellcolor{Gray}}{}}
			\fi
		}
	}
}
"""

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		## begin document
		f.write(header); f.write("\n")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	args = parser.parse_args()
	main(args)
