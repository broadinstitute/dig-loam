#!/bin/bash

binPlink=$1
base=$2
rawBase=$3
rawDupBase=$4
mem=$5

$binPlink \
--bfile $base \
--allow-no-sex \
--list-duplicate-vars 'ids-only' \
--out ${rawBase}.listdupvars \
--memory $mem \
--seed 1

if [ -s "${rawBase}.listdupvars.dupvar" ]; then
	$binPlink \
	--bfile $base \
	--allow-no-sex \
	--extract ${rawBase}.listdupvars.dupvar \
	--make-bed \
	--out $rawDupBase \
	--memory $mem \
	--seed 1

    $binPlink \
	--bfile $rawDupBase \
	--allow-no-sex \
	--freq \
	--out ${rawDupBase}.freq \
	--memory $mem \
	--seed 1

	$binPlink \
	--bfile $rawDupBase \
	--allow-no-sex \
	--missing \
	--out ${rawDupBase}.missing \
	--memory $mem \
	--seed 1
else
	touch ${rawDupBase}.bed
	touch ${rawDupBase}.bim
	touch ${rawDupBase}.fam
	touch ${rawDupBase}.freq.frq
	touch ${rawDupBase}.missing.imiss
	touch ${rawDupBase}.missing.lmiss
fi

exit 0
