import argparse

def writeHeader(fileHandle):
	fileHandle.write(r"""\documentclass[11pt]{article}
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
\pagestyle{fancy}
\renewcommand{\sectionmark}[1]{\markboth{#1}{}}
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
\usepackage[english]{babel}
\usepackage{datetime}
\usepackage{fancyvrb}
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
\makeatletter
\renewcommand\tableofcontents{
	\section*{\contentsname
		\@mkboth{
			\MakeUppercase\contentsname}{}}
	\@starttoc{toc}
	}
\makeatother

\begin{document}
""".encode('utf-8'))

def writeVerbatim(fileHandle, txt):
	fileHandle.write("\n")
	fileHandle.write(r"\VerbatimInput{" + txt + "}".encode('utf-8'))

def writeDoubleQQ(fileHandle, qqAll, qqCommon):
	fileHandle.write("\n")
	fileHandle.write(r"""\begin{figure}[H]
\centering
\begin{subfigure}{.5\textwidth}
	\centering
	\includegraphics[width=\linewidth]{""".encode('utf-8') + qqAll + r"""}
	\caption{All Variants)}
\end{subfigure}%
\begin{subfigure}{.5\textwidth}
	\centering
	\includegraphics[width=\linewidth]{""".encode('utf-8') + qqCommon + r"""}
	\caption{Common Variants}
\end{subfigure}
\caption{QQ Plots}
\label{fig:QQ}
\end{figure}
""".encode('utf-8'))

def writeQQ(fileHandle, qq):
	fileHandle.write("\n")
	fileHandle.write(r"""\begin{figure}[H]
\centering
\begin{subfigure}{\textwidth}
	\centering
	\includegraphics[width=\linewidth]{""".encode('utf-8') + qq + r"""}
\end{subfigure}\\
\caption{QQ Plot}
\label{fig:QQ}
\end{figure}
""".encode('utf-8'))

def writeMht(fileHandle, mht):
	fileHandle.write("\n")
	fileHandle.write(r"""\begin{figure}[H]
\centering
\begin{subfigure}{\textwidth}
	\centering
	\includegraphics[width=\linewidth]{""".encode('utf-8') + mht + r"""}
\end{subfigure}\\
\caption{Manhattan Plot}
\label{fig:MHT}
\end{figure}
""".encode('utf-8'))

def writeTopResults(fileHandle, tsv):
	fileHandle.write("\t")
	fileHandle.write(r"""\begin{table}[H]
	\begin{center}
	\caption{Top Loci}
	\resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%
	\pgfplotstabletypeset[
		font=\footnotesize,
		col sep=tab,
		column type={>{\fontseries{bx}\selectfont}c},
		columns/marker/.style={column name=marker, verb string type},
		columns/pvalue/.style={column name=pvalue, precision = 3, sci precision = 2},
		columns/zscore/.style={column name=zscore, precision = 3, sci precision = 2},
		columns/stderr/.style={column name=stderr, precision = 3, sci precision = 2},
		columns/beta/.style={column name=beta, precision = 3, sci precision = 2},
		columns/odds_ratio/.style={column name=odds\_ratio, precision = 3, sci precision = 2},
		columns/eaf/.style={column name=eaf, precision = 3, sci precision = 2},
		columns/maf/.style={column name=maf, precision = 3, sci precision = 2},
		columns/n/.style={column name=n, verb string type},
		columns/GENE/.style={column name=GENE, verb string type},
		postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},
		every head row/.append style={
			before row=
				\toprule,
			after row=
				\midrule
		},
		every last row/.style={
			after row=
				\bottomrule
		},
		empty cells with={}
		]{""".encode('utf-8') + tsv + r"""}}
	\end{center}
\end{table}
""".encode('utf-8'))

def writeTail(fileHandle):
	fileHandle.write("\n")
	fileHandle.write(r"\end{document}".encode('utf-8'))
	fileHandle.write("\n")

def main(args=None):

	with open(args.out,'w') as f:

		print "writing header"
		writeHeader(f)

		print "writing config"
		writeVerbatim(f, args.cfg)

		print "writing qq plots"
		if args.qq:
			if args.qq_common:
				writeDoubleQQ(f, args.qq, args.qq_common)
			else:
				writeQQ(f, args.qq)

		print "writing mht plot"
		writeMht(f, args.mht)

		print "writing top results table"
		writeTopResults(f, args.top_results)

		print "writing tail"
		writeTail(f)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--qq-common', help='qq plot for common variants')
	parser.add_argument('--qq', help='qq plot')
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--cfg', help='a config file name', required=True)
	requiredArgs.add_argument('--mht', help='a manhattan plot file name', required=True)
	requiredArgs.add_argument('--top-results', help='a top results table file name', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
