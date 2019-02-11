#!/bin/bash

klustakwik=$1
pcaScores=$2
fet=$3
base=$4
log=$5

N=`head -1 $pcaScores | wc | awk '{print $2-1}'`

echo $N > $fet
exitCode=$?

sed '1d' $pcaScores | \
cut -f2- | \
sed 's/\t/ /g' \
>> $fet
if [ "$exitCode" == "0" ]; then
	exitCode=$?
fi

FEATURES=1
for i in $(seq 2 $N); do \
FEATURES=${FEATURES}1; \
done
if [ "$exitCode" == "0" ]; then
	exitCode=$?
fi

$klustakwik $base 1 -UseFeatures $FEATURES -UseDistributional 0 > $log
if [ "$exitCode" == "0" ]; then
	exitCode=$?
fi

exit $exitCode
