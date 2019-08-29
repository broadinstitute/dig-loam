#!/bin/bash

plink=$1
base=$2
forceA2=$3
refBase=$4
mem=$5
vcf=$6

$plink --bfile $base --allow-no-sex --output-chr MT --recode vcf-iid bgz --real-ref-alleles --a2-allele $forceA2 --out ${refBase}.temp --memory $mem
$plink --vcf ${refBase}.temp.vcf.gz --allow-no-sex --output-chr MT --vcf-half-call haploid --recode vcf-iid bgz --real-ref-alleles --a2-allele $forceA2 --out $refBase --memory $mem
rm ${refBase}.temp.*
