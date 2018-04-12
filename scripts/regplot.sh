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

if [ "$nSig" -gt "1" ]; then
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
		pcol=`$tabix -H $results | tr "\t" "\n" | grep -n pval | awk -F':' '{print $1}'`
		prefix=${out}.${regionVar}
		prefixArray+=("$prefix")
		data=${prefix}.results.tsv
		dataArray+=("$data")
		dir=${prefix}_chr${regionChr}_${regionStart}-${regionEnd}
		dirArray+=("$dir")
		pdf=${dir}/chr${regionChr}_${regionStart}-${regionEnd}.pdf
		pdfArray+=("$pdf")
		log=${prefix}.log
		logArray+=("$log")
		(echo -e "id\tpval"; $tabix $results ${regionChr}:${regionStart}-${regionEnd} | awk -v pcol=$pcol '{if($pcol != "NA") { if(substr($4 ,0, 2) != "rs") { print "chr"$1":"$2"\t"$pcol } else print $4"\t"$pcol} }') > $data
		$locuszoom \
		--metal $data \
		--chr $regionChr \
		--start $regionStart \
		--end $regionEnd \
		--markercol id \
		--pvalcol pval \
		--pop $pop \
		--build $build \
		--source $source \
		--no-date \
		--prefix $prefix \
		--cache None \
		> $log
	done < $sigregions
	$ghostscript -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -dFirstPage=1 -dLastPage=1 -sOutputFile=${out}.pdf $(IFS=" "; echo "${pdfArray[*]}")
	cat $(IFS=" "; echo "${logArray[*]}") > ${out}.log
	rm $(IFS=" "; echo "${logArray[*]}")
	rm $(IFS=" "; echo "${dataArray[*]}")
	rm -r $(IFS=" "; echo "${dirArray[*]}")
else
	touch ${out}.pdf
	echo "no significant variants found" > ${out}.log
fi

exit 0
