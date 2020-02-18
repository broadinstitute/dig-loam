#! /bin/bash

binPlink=$1
uids=$2
possDupVars=$3
rawPlinkBase=$4
possDupPlinkBase=$5
possDupFreq=$6
possDupMissing=$7
mem=$8


awk 'BEGIN { FS="\t" } { if(length($2)+length($3) == 2) { c[$4]++; l[$4,c[$4]]=$0 } } END { for (i in c) { if (c[i] > 1) for (j = 1; j <= c[i]; j++) print l[i,j] } }' $uids | awk '{print $1}' > $possDupVars

if [ -s $possDupVars ]
then
	$binPlink --bfile $rawPlinkBase --allow-no-sex --extract $possDupVars --make-bed --out $possDupPlinkBase --memory $mem --seed 1
	$binPlink --bfile $possDupPlinkBase --allow-no-sex --freq --out $possDupFreq --memory $mem --seed 1
	$binPlink --bfile $possDupPlinkBase --allow-no-sex --missing --out $possDupMissing --memory $mem --seed 1
else
	touch ${possDupPlinkBase}.bed
	touch ${possDupPlinkBase}.bim
	touch ${possDupPlinkBase}.fam
	touch ${possDupFreq}.frq
	touch ${possDupMissing}.lmiss
fi
