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
		--pheno-names)
			if [ "$2" ]; then
				phenoNames=$2
				shift
			else
				echo "ERROR: --pheno-names requires a non-empty argument."
				exit 1
			fi
			;;
        --cli-options)
			if [ "$2" ]; then
				cliOptions=$2
				shift
			else
				echo "ERROR: --cli-options requires a non-empty argument."
				exit 1
			fi
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
echo "phenoNames: $phenoNames"
echo "chr: $chr"
echo "pred: $pred"
echo "out: $out"
echo "cliOptions: $cliOptions"

$regenie \
--step 2 \
--bgen $bgen \
--sample $sample \
--covarFile $covarFile \
--phenoFile $phenoFile \
$cliOptions \
--chr $chr \
--pred $pred \
--out $out \
--gz \
--verbose

# header
#CHROM GENPOS ID ALLELE0 ALLELE1 A1FREQ INFO N TEST BETA SE CHISQ LOG10P EXTRA

for p in `echo $phenoNames | sed 's/,/ /g'`
do
	if [ ! -f "${out}_${p}.regenie.gz" ]
	then
		EXITCODE=1
	else
		mv ${out}_${p}.regenie.gz ${out}_${p}.regenie.tmp.gz
		n=`zcat ${out}_${p}.regenie.tmp.gz | head -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep LOG10P | awk '{print $1}'`
		(zcat ${out}_${p}.regenie.tmp.gz | head -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print "#"$0,"P"}'; zcat ${out}_${p}.regenie.tmp.gz | sed '1d' | awk -v c=$n 'BEGIN { OFS="\t" } {$1=$1; print $0,10^(-$c)}') | $bgzip -c > ${out}.${p}.results.tsv.bgz
		rm ${out}_${p}.regenie.tmp.gz
		EXITCODE=0
	fi
done

exit $EXITCODE
