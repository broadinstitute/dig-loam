#!/bin/bash

plink=$1
rawBase=$2
preparedBase=$3
imissRemove=$4
dupRemove=$5
mem=$6

awk '{print $1, $2, $2, $2}' ${rawBase}.fam > ${preparedBase}.nofid.updateids
$plink --bfile $rawBase --allow-no-sex --update-ids ${preparedBase}.nofid.updateids --keep-allele-order --make-bed --out ${preparedBase}.nofid
rm ${preparedBase}.nofid.updateids
awk '{print $1, $2, "0", "0"}' ${preparedBase}.nofid.fam > ${preparedBase}.nofid.updateparents 
$plink --bfile ${preparedBase}.nofid --allow-no-sex --update-parents ${preparedBase}.nofid.updateparents --keep-allele-order --make-bed --out ${preparedBase}.nofid.noparents
awk '{print $1, $2, "0"}' ${preparedBase}.nofid.noparents.fam > ${preparedBase}.nofid.noparents.updatesex
$plink --bfile ${preparedBase}.nofid.noparents --allow-no-sex --update-sex ${preparedBase}.nofid.noparents.updatesex --keep-allele-order --make-bed --out ${preparedBase}.nofid.noparents.nosex
rm ${preparedBase}.nofid.noparents.updatesex
awk '{print $1, $2, "-9"}' ${preparedBase}.nofid.noparents.nosex.fam > ${preparedBase}.nofid.noparents.nosex.updatepheno
$plink --bfile ${preparedBase}.nofid.noparents.nosex --allow-no-sex --pheno ${preparedBase}.nofid.noparents.nosex.updatepheno --keep-allele-order --make-bed --out ${preparedBase}.nofid.noparents.nosex.nopheno
$plink --bfile ${preparedBase}.nofid.noparents.nosex.nopheno --remove $imissRemove --exclude $dupRemove --merge-x no-fail --allow-no-sex --output-chr MT --make-bed --out $preparedBase --memory $mem
rm ${preparedBase}.nofid.*
