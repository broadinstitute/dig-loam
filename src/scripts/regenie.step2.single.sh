#!/bin/bash

chr="NA"
splitPhenoOut=0

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
		--pheno-table)
			if [ "$2" ]; then
				phenoTable=$2
				shift
			else
				echo "ERROR: --pheno-table requires a non-empty argument."
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
		--batch)
			if [ "$2" ]; then
				batch=$2
				shift
			else
				echo "ERROR: --batch requires a non-empty argument."
				exit 1
			fi
			;;
        --split-pheno-out)
			splitPhenoOut=1
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
echo "phenoTable: $phenoTable"
echo "chr: $chr"
echo "batch: $batch"
echo "splitPhenoOut: $splitPhenoOut"
echo "pred: $pred"
echo "out: $out"
echo "cliOptions: $cliOptions"

EXITCODE=0

if [ "$chr" == "NA" ]
then
	chrString=""
else
	chrString="--chr $chr"
fi

if [ $splitPhenoOut -eq 1 ]
then
	splitPhenoOutString=""
else
	splitPhenoOutString="--no-split"
fi


$regenie \
--step 2 \
--bgen $bgen \
--ref-first \
--sample $sample \
--covarFile $covarFile \
--phenoFile $phenoFile \
$cliOptions \
$chrString \
$splitPhenoOutString \
--pred $pred \
--out $out \
--gz \
--verbose

if [ $? != 0 ]
then
	echo "regenie step 2 failed"
	EXITCODE=1
fi

# header
#CHROM GENPOS ID ALLELE0 ALLELE1 A1FREQ INFO N TEST BETA SE CHISQ LOG10P EXTRA

if [ $splitPhenoOut -eq 1 ]
then
	while read line
	do
		p=`echo "${line}" | awk -F'\t' '{print $1}'`
		pAnalyzed=`echo "${line}" | awk -F'\t' '{print $5}'`
		if [ ! -f "${out}_${pAnalyzed}.regenie.gz" ]
		then
			echo "no successful tests for phenotype ${p}!"
			EXITCODE=1
		else
			mv ${out}_${pAnalyzed}.regenie.gz ${out}_${pAnalyzed}.regenie.tmp.gz
			n=`zcat ${out}_${pAnalyzed}.regenie.tmp.gz | head -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep LOG10P | awk '{print $1}'`
			(zcat ${out}_${pAnalyzed}.regenie.tmp.gz | head -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print "#"$0,"P"}'; zcat ${out}_${pAnalyzed}.regenie.tmp.gz | sed '1d' | awk -v c=$n 'BEGIN { OFS="\t" } {$1=$1; print $0,10^(-$c)}') | $bgzip -c > ${out}.${p}.results.tsv.bgz
			rm ${out}_${pAnalyzed}.regenie.tmp.gz
		fi
	done < <(sed '1d' $phenoTable | awk -v batch=$batch -F'\t' '{if($7 == batch) print $0}')
else
	mv ${out}.regenie.gz ${out}.regenie.tmp.gz
	#n=`zcat ${out}.regenie.tmp.gz | head -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep LOG10P | awk '{print $1}'`
	#(zcat ${out}.regenie.tmp.gz | head -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print "#"$0,"P"}'; zcat ${out}.regenie.tmp.gz | sed '1d' | awk -v c=$n 'BEGIN { OFS="\t" } {$1=$1; print $0,10^(-$c)}') | $bgzip -c > ${out}.results.tsv.bgz
	(zcat ${out}.regenie.tmp.gz | head -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print "#"$0}'; zcat ${out}.regenie.tmp.gz | sed '1d' | awk 'BEGIN { OFS="\t" } {$1=$1; print $0}') | $bgzip -c > ${out}.results.tsv.bgz
	rm ${out}.regenie.tmp.gz
fi

exit $EXITCODE
