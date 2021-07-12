#!/bin/bash

while :; do
	case $1 in
		--regenie)
			if [ "$2" ]; then
				regenie=$2
				shift
			else
				echo "ERROR: --regenie requires a non-empty argument."
				exit 1
			fi
			;;
		--bgzip)
			if [ "$2" ]; then
				bgzip=$2
				shift
			else
				echo "ERROR: --bgzip requires a non-empty argument."
				exit 1
			fi
			;;
		--bgen)
			if [ "$2" ]; then
				bgen=$2
				shift
			else
				echo "ERROR: --bgen requires a non-empty argument."
				exit 1
			fi
			;;
		--sample)
			if [ "$2" ]; then
				sample=$2
				shift
			else
				echo "ERROR: --sample requires a non-empty argument."
				exit 1
			fi
			;;
		--covar-file)
			if [ "$2" ]; then
				covarFile=$2
				shift
			else
				echo "ERROR: --covar-file requires a non-empty argument."
				exit 1
			fi
			;;
		--pheno-file)
			if [ "$2" ]; then
				phenoFile=$2
				shift
			else
				echo "ERROR: --pheno-file requires a non-empty argument."
				exit 1
			fi
			;;
		--pheno-name)
			if [ "$2" ]; then
				phenoName=$2
				shift
			else
				echo "ERROR: --pheno-name requires a non-empty argument."
				exit 1
			fi
			;;
		--block-size)
			if [ "$2" ]; then
				blockSize=$2
				shift
			else
				echo "ERROR: --block-size requires a non-empty argument."
				exit 1
			fi
			;;
		--bt)
			bt=true
			;;
        --chr)
			if [ "$2" ]; then
				chr=$2
				shift
			else
				echo "ERROR: --chr requires a non-empty argument."
				exit 1
			fi
			;;
        --pred)
			if [ "$2" ]; then
				pred=$2
				shift
			else
				echo "ERROR: --pred requires a non-empty argument."
				exit 1
			fi
			;;
		--anno-file)
			if [ "$2" ]; then
				annoFile=$2
				shift
			else
				echo "ERROR: --anno-file requires a non-empty argument."
				exit 1
			fi
			;;
		--set-list)
			if [ "$2" ]; then
				setList=$2
				shift
			else
				echo "ERROR: --set-list requires a non-empty argument."
				exit 1
			fi
			;;
		--mask-def)
			if [ "$2" ]; then
				maskDef=$2
				shift
			else
				echo "ERROR: --mask-def requires a non-empty argument."
				exit 1
			fi
			;;
		--out)
			if [ "$2" ]; then
				out=$2
				shift
			else
				echo "ERROR: --out requires a non-empty argument."
				exit 1
			fi
			;;
		--)
			shift
			break
			;;
		-?*)
			echo "WARN: Unknown option (ignored): $1"
			;;
		*)
			break
	esac
	shift
done

echo "regenie: $regenie"
echo "bgzip: $bgzip"
echo "bgen: $bgen"
echo "sample: $sample"
echo "covarFile: $covarFile"
echo "phenoFile: $phenoFile"
echo "phenoName: $phenoName"
echo "blockSize: $blockSize"
echo "bt: $bt"
echo "chr: $chr"
echo "pred: $pred"
echo "annoFile: $annoFile"
echo "setList: $setList"
echo "maskDef: $maskDef"
echo "out: $out"

if [ $bt ]
then
	btString="--bt --firth --firth-se --approx --pThresh 0.05"
else
	btString=""
fi

$regenie \
--step 2 \
--bgen $bgen \
--sample $sample \
--covarFile $covarFile \
--phenoFile $phenoFile \
--bsize $blockSize \
$btString \
--chr $chr \
--pred $pred \
--anno-file $annoFile \
--set-list $setList \
--mask-def $maskDef \
--out $out

if [ ! -f "${out}_${phenoName}.regenie" ]
then
	EXITCODE=1
else
	(head -2 ${out}_${phenoName}.regenie | tail -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print "#"$0,"MAF","MAC","P"}'; sed '1,2d' ${out}_${phenoName}.regenie | awk 'BEGIN { OFS="\t" } {$1=$1; if($6 > 0.5) { maf=1-$6 } else { maf=$6 }; print $0,maf,sprintf("%.0f",2*$7*maf),10^(-$NF)}') | $bgzip -c > ${out}.results.tsv.bgz
	rm ${out}_${phenoName}.regenie
	EXITCODE=0
fi

exit $EXITCODE
