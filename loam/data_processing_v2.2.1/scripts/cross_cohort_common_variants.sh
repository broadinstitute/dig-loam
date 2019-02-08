#!/bin/bash

BIMS=$1
OUT=$2

BIMLIST=`echo $BIMS | tr ',' ' '`
N=`echo $BIMLIST | wc -w`

(for f in $BIMLIST; do \
awk '{print $1":"$2":"$4":"$5":"$6}' $f; \
done) | \
sort | \
uniq -c | \
awk -v n=$N '{if($1 == n) print $2}' | \
awk -F':' '{print $2}' \
> $OUT
