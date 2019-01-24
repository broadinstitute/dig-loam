#!/bin/bash

pdflatex=$1
tex=$2
dir=$3

$pdflatex --output-directory=${dir} $tex
sleep 5
$pdflatex --output-directory=${dir} $tex
