#!/bin/bash

FILE=$1
CFG=$2
OUT=$3

echo "...extracting chromosome list for ${FILE}"
chrs=`sed '1d' $FILE | awk '{print $1}' | awk -F'_' '{print $1}' | sort -u | tr '\n' ' '`
echo "...extracting header for ${FILE}"
header=`head -1 $FILE`

i=0
while read line || [ -n "$line" ]
do
	i=$((i+1))
	echo "...${i} processing config line: ${line}"
	if [[ "${line}" =~ ^load.* ]]
	then
		echo "......${i} processing chromosomes for config load line: ${line}"
		for chr in $chrs
		do
			outfile=`echo $FILE | sed "s/\.tsv/\.chr${chr}\.tsv/g"`
			echo ".........${chr} adding header to ${outfile}"
			echo $header | awk 'BEGIN { OFS="\t" } { $1=$1 }1' > $outfile
			echo ".........${chr} adding chromosome ${chr} to ${outfile}"
			sed '1d' $FILE | grep "^${chr}_" >> $outfile
			echo ".........${chr} adding load statement to split config ${OUT}"
			echo "load ${outfile}" >> $OUT
			echo ".........${chr} adding process statement to split config"
			echo "process" >> $OUT
		done
	else
		if [[ "${line}" =~ ^process ]]
		then
			echo "......${i} removing extra process line"
		else
			if [ "$i" == "1" ]
			then
				echo "......${i} overwriting split config with line: ${line}"
				echo $line > $OUT
			else
				echo "......${i} adding line to split config: ${line}"
				echo $line >> $OUT
			fi
		fi
	fi
done < $CFG

exit 0
