#!/bin/bash

binFlashpca=$1
binRscript=$2
binPlink=$3
rScript=$4
cpus=$5
plinkData=$6
samplesAvailable=$7
outPrefix=$8
outpc=$9
outvec=${10}
outload=${11}
outval=${12}
outpve=${13}
outmeansd=${14}
maxiter=${15}
preliminaryPheno=${16}
phenotype=${17}
iidCol=${18}
trans=${19}
covars=${20}
minPcs=${21}
maxPcs=${22}
nStddevs=${23}
phenoFile=${24}
pcsIncludeFile=${25}
outlierFile=${26}

echo "binFlashpca: $binFlashpca"
echo "binRscript: $binRscript"
echo "binPlink: $binPlink"
echo "rScript: $rScript"
echo "cpus: $cpus"
echo "plinkData: $plinkData"
echo "samplesAvailable: $samplesAvailable"
echo "outPrefix: $outPrefix"
echo "outpc: $outpc"
echo "outvec: $outvec"
echo "outload: $outload"
echo "outval: $outval"
echo "outpve: $outpve"
echo "outmeansd: $outmeansd"
echo "maxiter: $maxiter"
echo "preliminaryPheno: $preliminaryPheno"
echo "phenotype: $phenotype"
echo "iidCol: $iidCol"
echo "trans: $trans"
echo "covars: $covars"
echo "minPcs: $minPcs"
echo "maxPcs: $maxPcs"
echo "nStddevs: $nStddevs"
echo "phenoFile: $phenoFile"
echo "pcsIncludeFile: $pcsIncludeFile"
echo "outlierFile: $outlierFile"

# copy samples available to plink keep file
awk '{print $1"\t"$1}' $samplesAvailable > ${outPrefix}.tmp.samples.keep

# use Plink to extract available samples for this cohort
$binPlink --bfile $plinkData --keep ${outPrefix}.tmp.samples.keep --make-bed --out ${outPrefix}.tmp

# run flashpca first time
$binFlashpca \
--verbose \
--seed 1 \
--numthreads $cpus \
--ndim 20 \
--bfile ${outPrefix}.tmp \
--outpc ${outPrefix}.tmp.outpc \
--outvec ${outPrefix}.tmp.outvec \
--outload ${outPrefix}.tmp.outload \
--outval ${outPrefix}.tmp.outval \
--outpve ${outPrefix}.tmp.outpve \
--outmeansd ${outPrefix}.tmp.outmeansd

## use r script to check for outliers and generate phenotype file
$binRscript --vanilla --verbose $rScript \
--pheno-in $preliminaryPheno \
--pheno-col $phenotype \
--pcs-in ${outPrefix}.tmp.outpc \
--iid-col $iidCol \
--trans "$trans" \
--covars "$covars" \
--min-pcs $minPcs \
--max-pcs $maxPcs \
--n-stddevs $nStddevs \
--out-pheno ${outPrefix}.tmp.pheno \
--out-pcs ${outPrefix}.tmp.pcs.include \
--out-outliers ${outPrefix}.tmp.outliers

# copy initial outliers to file
cat ${outPrefix}.tmp.outliers > $outlierFile

if [[ -s ${outPrefix}.tmp.outliers && $maxiter -gt 1 ]]; then
	for (( i=2; i<=$maxiter; i++ )); do

		# use Plink to remove outliers found in previous iterations
		$binPlink --bfile ${outPrefix}.tmp --remove ${outPrefix}.outliers --make-bed --out ${outPrefix}.tmp.outliers.removed
	
		# run flashpca
		$binFlashpca \
		--verbose \
		--seed 1 \
		--numthreads $cpus \
		--ndim 20 \
		--bfile ${outPrefix}.tmp \
		--outpc ${outPrefix}.tmp.outpc \
		--outvec ${outPrefix}.tmp.outvec \
		--outload ${outPrefix}.tmp.outload \
		--outval ${outPrefix}.tmp.outval \
		--outpve ${outPrefix}.tmp.outpve \
		--outmeansd ${outPrefix}.tmp.outmeansd
	
		# use r script to check for outliers and generate phenotype file
		$binRscript --vanilla --verbose $rScript \
		--pheno-in $preliminaryPheno \
		--pheno-col $phenotype \
		--pcs-in ${outPrefix}.tmp.outpc \
		--iid-col $iidCol \
		--trans "$trans" \
		--covars "$covars" \
		--min-pcs $minPcs \
		--max-pcs $maxPcs \
		--n-stddevs $nStddevs \
		--out-pheno ${outPrefix}.tmp.pheno \
		--out-pcs ${outPrefix}.tmp.pcs.include \
		--out-outliers ${outPrefix}.tmp.outliers

		# break or add new temp outliers to permanent file and continue
		if [ ! -s ${outPrefix}.tmp.outliers ]; then
			break
		else
			cat ${outPrefix}.tmp.outliers >> $outlierFile
		fi
	done
fi

# mv tmp files to permanent
mv ${outPrefix}.tmp.outpc $outpc
mv ${outPrefix}.tmp.outvec $outvec
mv ${outPrefix}.tmp.outload $outload
mv ${outPrefix}.tmp.outval $outval
mv ${outPrefix}.tmp.outpve $outpve
mv ${outPrefix}.tmp.outmeansd $outmeansd
mv ${outPrefix}.tmp.pheno $phenoFile
mv ${outPrefix}.tmp.pcs.include $pcsIncludeFile

# remove remaining temporary files
rm ${outPrefix}.tmp*
