#!/bin/bash

binEpacts=$1
type=$2
vcf=$3
groupf=$4
ped=$5
modelVars=$6
test=$7
field=$8
out=$9
run=${10}

echo "binEpacts: $binEpacts"
echo "type: $type"
echo "vcf: $vcf"
echo "groupf: $groupf"
echo "ped: $ped"
echo "modelVars: $modelVars"
echo "test: $test"
echo "field: $field"
echo "out: $out"
echo "run: $run"

i=0
phenoCovars=""
while read line; do \
	i=$((i+1))
	if [ $i -eq 1 ]; then \
		phenoCovars=`echo "--pheno $line"`
	else
		phenoCovars=`echo "$phenoCovars --cov $line"`
	fi
done < $modelVars

echo $phenoCovars

# run epacts
$binEpacts $type \
--vcf $vcf \
--groupf $groupf \
--ped $ped \
$phenoCovars \
--test $test \
--field $field \
--out $out \
--run $run \
--no-plot

if [ -f "${out}.epacts.OK" ]; then
	mv ${out}.epacts $out
	exit 0
else
	exit 1
fi
