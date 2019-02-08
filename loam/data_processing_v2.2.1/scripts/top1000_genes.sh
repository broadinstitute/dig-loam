#!/bin/bash

top=$1
python=$2
pyAddGeneAnnot=$3
genePositions=$4
out=$5

(sed '1d' $top | \
awk '{print $1"\t"$2}' | \
$python $pyAddGeneAnnot --outside-name NA --chr-col 1 --pos-col 2 --gene-file $genePositions --out-delim \\t) | \
sort -u \
> $out
