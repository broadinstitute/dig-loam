#!/bin/bash

tabix=$1
locuszoom=$2
ghostscript=$3
sigregions=$4
results=$5
pop=$6
build=$7
source=$8
out=$9

nSig=`wc -l $sigregions | awk '{print $1}'`
exitCode=0

echo "$nSig significant variants found" > ${out}.log

sourcePath=""
if [[ "$source" =~ 2014$ ]]; then
	sourcePath="/usr/local/src/locuszoom/data/1000G/genotypes/2014-10-14"
elif [[ "$source" =~ 2017$ ]]; then
	sourcePath="/usr/local/src/locuszoom/data/1000G/genotypes/2017-04-10"
else
	echo "source path not available for ${source}"
	exit 1
fi

if [ "$nSig" -ge "1" ]; then
	prefixArray=()
	dataArray=()
	dirArray=()
	pdfArray=()
	logArray=()
	while read line; do
		regionChr=`echo $line | awk '{print $1}'`
		regionStart=`echo $line | awk '{print $2}'`
		regionEnd=`echo $line | awk '{print $3}'`
		regionVar=`echo $line | awk '{print $4}'`
		pcol=`tabix -H $results | tr "\t" "\n" | grep -n pval | awk -F':' '{print $1}'`
		prefix=${out}.${regionVar}
		prefixArray+=("$prefix")
		data=${prefix}.results.tsv
		dataArray+=("$data")
		log=${prefix}.log
		logArray+=("$log")
		(echo -e "id\tpval"; $tabix $results ${regionChr}:${regionStart}-${regionEnd} | awk -v pcol=$pcol '{if($pcol != "NA" && $pcol != "NaN") { if(substr($3 ,0, 2) != "rs") { print "chr"$1":"$2"\t"$pcol } else print $3"\t"$pcol} }') > $data
		ldRefVars=`sed '1d' $data | sort -g -k2,2 | awk '{print $1}' | head -10 | tr '\n' ' '`
		refVar=""
		refVarId=""
		for id in $ldRefVars; do
			chr=`echo $id | awk -F':' '{print $1}' | sed 's/chr//g'`
			pos=`echo $id | awk -F':' '{print $2}'`
			n=`awk -v c=$chr -v p=$pos '{if($1 == c && $4 == p) print $0}' ${sourcePath}/${pop}/chr${chr}.bim | wc -l`
			if [ $n == 1 ]; then
				refVar=$id
				m=`tabix $results ${chr}:${pos}-${pos} | wc -l`
				if [ $m == 1 ]; then
					v=`tabix $results ${chr}:${pos}-${pos} | awk '{print $3}'`
					if [[ "$v" =~ ^rs ]]; then
						refVarId=$v
					else
						refVarId=$refVar
					fi
				else
					l=`tabix $results ${chr}:${pos}-${pos} | grep -w "${regionVar}" | wc -l`
					if [ $l == 1 ]; then
						refVarId=$regionVar
					else
						v=`tabix $results ${chr}:${pos}-${pos} | head -1 | awk '{print $3}'`
						if [[ "$v" =~ ^rs ]]; then
							refVarId=$v
						else
							refVarId=$refVar
						fi
					fi
				fi
				break
			fi
		done
		echo -e "sig region: ${line}"
		echo -e "chr: ${regionChr}"
		echo -e "start: ${regionStart}"
		echo -e "end: ${regionEnd}"
		echo -e "variant: ${regionVar}"
		echo -e "formatted input data: ${data}"
		echo -e "sorted possible ld ref variants: ${ldRefVars}"
		echo -e "ld ref var: ${refVarId}"
		
		options="--metal $data --chr $regionChr --start $regionStart --end $regionEnd --markercol id --pvalcol pval --no-date --prefix $prefix --cache None --build $build --pop $pop --source $source --no-cleanup"
		if [[ "$regionChr" -ge "23" || "${refVarId}" == "" ]]; then
			options="$options --no-ld"
		else
			options="$options --refsnp ${refVarId}"
		fi

		echo "Locuszoom arguments: $options" >> ${out}.log
		$locuszoom $options > $log

		if [[ "$refVarId" =~ ^rs ]]; then
			dir=`echo "${prefix}_${refVarId}" | sed 's/:/_/g'`
		else
			dir=`echo "${prefix}_chr${regionChr}_${regionStart}-${regionEnd}" | sed 's/:/_/g'`
		fi
		dirArray+=("$dir")
		pdf=${dir}/chr${regionChr}_${regionStart}-${regionEnd}.pdf
		pdfArray+=("$pdf")

		exitCode=$?
	done < $sigregions
	$ghostscript -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -dFirstPage=1 -dLastPage=1 -sOutputFile=${out}.pdf $(IFS=" "; echo "${pdfArray[*]}")
	if [ "$exitCode" == "0" ]; then
		exitCode=$?
	fi
	cat $(IFS=" "; echo "${logArray[*]}") >> ${out}.log
	rm $(IFS=" "; echo "${logArray[*]}")
	rm $(IFS=" "; echo "${dataArray[*]}")
	rm -r $(IFS=" "; echo "${dirArray[*]}")
else
	touch ${out}.pdf
fi

exit $exitCode
