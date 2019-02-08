#!/bin/bash

fam=$1
ancestryInferred=$2
ancestries=$3
out=$4

join -1 2 -2 1 <(sort -k 2 $fam) <(sort -k 1 $ancestryInferred) | \
awk -v var=${ancestries} '{ split(var,arr,","); for ( a in arr ) { if (arr[a] == $7 ) print $2"\t"$1 } }' \
> $out
