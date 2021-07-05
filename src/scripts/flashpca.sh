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
modelType=${20}
covars=${21}
minPcs=${22}
maxPcs=${23}
nStddevs=${24}
phenoFile=${25}
pcsIncludeFile=${26}
outlierFile=${27}
mem=${28}

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
echo "modelType: $modelType"
echo "covars: $covars"
echo "minPcs: $minPcs"
echo "maxPcs: $maxPcs"
echo "nStddevs: $nStddevs"
echo "phenoFile: $phenoFile"
echo "pcsIncludeFile: $pcsIncludeFile"
echo "outlierFile: $outlierFile"
echo "mem: $mem"

# copy samples available to plink keep file
awk '{print $1"\t"$1}' $samplesAvailable > ${outPrefix}.tmp.1.samples.keep

# use Plink to extract available samples for this cohort
$binPlink --bfile $plinkData --keep ${outPrefix}.tmp.1.samples.keep --make-bed --out ${outPrefix}.tmp.1 --memory $mem --seed 1

# run flashpca first time
$binFlashpca \
--seed 1 \
--numthreads $cpus \
--ndim 20 \
--bfile ${outPrefix}.tmp.1 \
--outpc ${outPrefix}.tmp.1.outpc \
--outvec ${outPrefix}.tmp.1.outvec \
--outload ${outPrefix}.tmp.1.outload \
--outval ${outPrefix}.tmp.1.outval \
--outpve ${outPrefix}.tmp.1.outpve \
--outmeansd ${outPrefix}.tmp.1.outmeansd

if [ "$modelType" == "binary" ]
then
	modelTypeFlag="--binary"
else
	modelTypeFlag=""
fi

## use r script to check for outliers and generate phenotype file
$binRscript --vanilla --verbose $rScript \
--pheno-in $preliminaryPheno \
--pheno-col $phenotype \
--pcs-in ${outPrefix}.tmp.1.outpc \
--iid-col $iidCol \
--trans "$trans" \
$modelTypeFlag \
--covars "$covars" \
--min-pcs $minPcs \
--max-pcs $maxPcs \
--n-stddevs $nStddevs \
--out-pheno ${outPrefix}.tmp.1.pheno \
--out-pcs ${outPrefix}.tmp.1.pcs.include \
--out-outliers ${outPrefix}.tmp.1.outliers

# copy initial outliers to file
cat ${outPrefix}.tmp.1.outliers > $outlierFile
awk '{print $1"\t"$1}' $outlierFile > ${outPrefix}.tmp.1.outliers.plink

initoutliers=`wc -l $outlierFile | awk '{print $1}'`

n=0

if [[ -s ${outPrefix}.tmp.1.outliers && $maxiter -gt 1 ]]; then

	echo "iter 1: ${initoutliers} outliers found in initial PCA iteration"

	for (( i=2; i<=$maxiter; i++ )); do

		n=$((i-1))

		# use Plink to remove outliers found in previous iterations
		nsamples=`wc -l $outlierFile | awk '{print $1}'`
		echo "iter ${i}: removing ${nsamples} outliers from ${outPrefix}.tmp using Plink"
		$binPlink --bfile ${outPrefix}.tmp.${n} --remove ${outPrefix}.tmp.${n}.outliers.plink --make-bed --out ${outPrefix}.tmp.${i}
		nsamplesremaining=`wc -l ${outPrefix}.tmp.${i}.fam | awk '{print $1}'`
		echo "iter ${i}: ${nsamplesremaining} samples remaining for PCA analysis"
	
		# run flashpca
		$binFlashpca \
		--seed 1 \
		--numthreads $cpus \
		--ndim 20 \
		--bfile ${outPrefix}.tmp.${i} \
		--outpc ${outPrefix}.tmp.${i}.outpc \
		--outvec ${outPrefix}.tmp.${i}.outvec \
		--outload ${outPrefix}.tmp.${i}.outload \
		--outval ${outPrefix}.tmp.${i}.outval \
		--outpve ${outPrefix}.tmp.${i}.outpve \
		--outmeansd ${outPrefix}.tmp.${i}.outmeansd
	
		# use r script to check for outliers and generate phenotype file
		$binRscript --vanilla --verbose $rScript \
		--pheno-in $preliminaryPheno \
		--pheno-col $phenotype \
		--pcs-in ${outPrefix}.tmp.${i}.outpc \
		--iid-col $iidCol \
		--trans "$trans" \
        $modelTypeFlag \
		--covars "$covars" \
		--min-pcs $minPcs \
		--max-pcs $maxPcs \
		--n-stddevs $nStddevs \
		--out-pheno ${outPrefix}.tmp.${i}.pheno \
		--out-pcs ${outPrefix}.tmp.${i}.pcs.include \
		--out-outliers ${outPrefix}.tmp.${i}.outliers

		# break or add new temp outliers to permanent file and continue
		if [ ! -s ${outPrefix}.tmp.${i}.outliers ]; then
			echo "iter ${i}: no outliers found in iteration ${i}"
			break
		else
			nsamplestoremove=`wc -l ${outPrefix}.tmp.${i}.outliers | awk '{print $1}'`
			echo "iter ${i}: ${nsamplestoremove} outliers will be added to ${outlierFile}"
			cat ${outPrefix}.tmp.${i}.outliers >> $outlierFile
			awk '{print $1"\t"$1}' ${outPrefix}.tmp.${i}.outliers >> ${outPrefix}.tmp.${i}.outliers.plink
		fi

	done
fi

N=$((n+1))
# mv tmp files to permanent
echo "mv ${outPrefix}.tmp.${N}.outpc $outpc"
mv ${outPrefix}.tmp.${N}.outpc $outpc
echo "mv ${outPrefix}.tmp.${N}.outvec $outvec"
mv ${outPrefix}.tmp.${N}.outvec $outvec
echo "mv ${outPrefix}.tmp.${N}.outload $outload"
mv ${outPrefix}.tmp.${N}.outload $outload
echo "mv ${outPrefix}.tmp.${N}.outval $outval"
mv ${outPrefix}.tmp.${N}.outval $outval
echo "mv ${outPrefix}.tmp.${N}.outpve $outpve"
mv ${outPrefix}.tmp.${N}.outpve $outpve
echo "mv ${outPrefix}.tmp.${N}.outmeansd $outmeansd"
mv ${outPrefix}.tmp.${N}.outmeansd $outmeansd
echo "mv ${outPrefix}.tmp.${N}.pheno $phenoFile"
mv ${outPrefix}.tmp.${N}.pheno $phenoFile
echo "mv ${outPrefix}.tmp.${N}.pcs.include $pcsIncludeFile"
mv ${outPrefix}.tmp.${N}.pcs.include $pcsIncludeFile

# remove remaining temporary files
rm ${outPrefix}.tmp*
