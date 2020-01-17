#!/bin/bash

fileList=$1
mergedFile=$2

header=`head -1 $fileList`
cat $header > $mergedFile
while read line; do \
	sed '1d' $line >> $mergedFile; \
done < <(sed '1d' $fileList)
