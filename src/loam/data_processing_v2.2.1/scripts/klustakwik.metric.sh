#!/bin/bash

klustakwik=$1
statsAdj=$2
metric=$3
base=$4
fet=$5
log=$6

metricIdx=`head -1 $statsAdj | tr '\t' '\n' | awk '{print NR, $0}' | grep -w $metric | awk '{print $1}'`

echo 1 > $fet
exitCode=$?

sed '1d' $statsAdj | awk -v col=${metricIdx} '{print $col}' >> $fet
if [ "$exitCode" == "0" ]; then
	exitCode=$?
fi

$klustakwik $base 1 -UseFeatures 1 -UseDistributional 0 > $log
if [ "$exitCode" == "0" ]; then
	exitCode=$?
fi

exit $exitCode
