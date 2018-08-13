#!/bin/bash

king=$1
bed=$2
prefix=$3
log=$4
kin0=$5
kin0Related=$6
cpus=$7

$king -b $bed --kinship --prefix $prefix --cpus $cpus > $log
if [ -f $kin0 ]; then
	(head -1 $kin0; sed '1d' $kin0 | awk '{if($8 >= 0.0884) print $0}' | sort -rn -k8,8) > $kin0Related
else
	head -1 ${prefix}.kin > $kin0
	cp $kin0 $kin0Related
fi
