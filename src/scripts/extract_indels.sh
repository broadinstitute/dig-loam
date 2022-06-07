#!/bin/bash

plink=$1
inBase=$2
exclude=$3
chr=$4
outBase=$5
mem=$6

n=`awk -v c=$chr '{if($1 == c && (length($5) > 1 || length($6) > 1)) print $0}' ${inBase}.bim | wc -l`

if [ "$n" -gt "0" ]
then
	echo "removing ${n_exclude} variants from plink dataset with ${n_bim} variants"
	$plink --bfile $inBase --allow-no-sex --exclude $exclude --chr $chr --keep-allele-order --make-bed --out $outBase --output-chr MT --memory $mem --seed 1
else
	echo "writing empty plink fileset: remove ${n_exclude} from ${n_bim} variants"
	touch ${outBase}.bed
	touch ${outBase}.bim
	touch ${outBase}.fam
	echo "no indels found" > ${outBase}.log
fi
