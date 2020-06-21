#!/bin/bash

files=`ls -alt src/loam/* | awk '{print $9}' | cut -d '/' -f3- | grep -v ^_ | sed 's/\.loam//g' | tr '\n' ' '`

for file in $files; do
	echo "object ${file} extends loamstream.LoamFile {" > src/scala/${file}.scala
	echo "" >> src/scala/${file}.scala
	IFS=''
	while read line; do
		echo "  ${line}" >> src/scala/${file}.scala
	done < src/loam/${file}.loam
	echo "" >> src/scala/${file}.scala
	echo "}" >> src/scala/${file}.scala
done
