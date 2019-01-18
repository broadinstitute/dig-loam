#!/bin/bash

klustakwik=$1
scores=$2
base=$3
fet=$4
log=$5

N=$(head -1 $scores | wc | awk '{print $2-1}')
echo $N > $fet
sed '1d' $scores | cut -f2- | sed 's/\t/ /g' >> $fet

FEATURES=1
for i in $(seq 2 $N); do 
	FEATURES=${FEATURES}1
done

$klustakwik $base 1 -UseFeatures $FEATURES -UseDistributional 0 > $log
