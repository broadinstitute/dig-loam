#!/bin/bash

king=$1
bed=$2
prefix=$3
cpus=$4
log=$5
kin0=$6

$king -b $bed --related --degree 2 --prefix $prefix --cpus $cpus > $log

if [ $? -eq 0 ]; then
	if [ ! -f $kin0 ]; then
		echo -e "FID1\tID1\tFID2\tID2\tN_SNP\tHetHet\tIBS0\tHetConc\tHomIBS0\tKinship\tIBD1Seg\tIBD2Seg\tPropIBD\tInfType" > $kin0
	fi
else
	exit $?
fi
