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
echo "chr: $chr"
echo "pred: $pred"
echo "annoFile: $annoFile"
echo "setList: $setList"
echo "maskDef: $maskDef"
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
--anno-file $annoFile \
--set-list $setList \
--mask-def $maskDef \
--out $out \
--gz \
--htp ALL \
--verbose

## quantitative trait header
###MASKS=<0of5_1pct="0of5_1pct,0of5_1pct;LoF_HC";LoF_HC="0of5_1pct;LoF_HC">
#CHROM GENPOS ID ALLELE0 ALLELE1 A1FREQ N TEST BETA SE CHISQ LOG10P EXTRA
#1 974537 ENSG00000187583.0of5_1pct.1 ref 0of5_1pct.1 0.0138889 108 ADD -0.249906 0.594486 0.176713 0.171203 DF=1
#
###MASKS=<0of5_1pct="0of5_1pct,0of5_1pct;LoF_HC";LoF_HC="0of5_1pct;LoF_HC">
#Name	Chr	Pos	Ref	Alt	Trait	Cohort	Model	Effect	LCI_Effect	UCI_Effect	Pval	AAF	Num_Cases	Cases_Ref	Cases_Het	Cases_Alt	Num_Controls	Controls_Ref	Controls_Het	Controls_Alt	Info
#ENSG00000187583.0of5_1pct.1	1	974537	ref	0of5_1pct.1	CaffeineConsumption	COHORT_NAME	ADD-WGR-LR	-0.249906	-1.41508	0.915266	0.674213	0.0138889	108	106	1	1	NA	NA	NA	NA	REGENIE_SE=0.594486;MAC=3.000000;DF=1

## binary trait header
###MASKS=<0of5_1pct="0of5_1pct,0of5_1pct;LoF_HC";LoF_HC="0of5_1pct;LoF_HC">
#CHROM GENPOS ID ALLELE0 ALLELE1 A1FREQ A1FREQ_CASES A1FREQ_CONTROLS N TEST BETA SE CHISQ LOG10P EXTRA
#1 974537 ENSG00000187583.0of5_1pct.singleton ref 0of5_1pct.singleton 0.0045045 0.01 0 111 ADD 1.11045 5.19977 0.0456069 0.0804555 DF=1
#
###MASKS=<0of5_1pct="0of5_1pct,0of5_1pct;LoF_HC";LoF_HC="0of5_1pct;LoF_HC">
#Name	Chr	Pos	Ref	Alt	Trait	Cohort	Model	Effect	LCI_Effect	UCI_Effect	Pval	AAF	Num_Cases	Cases_Ref	Cases_Het	Cases_Alt	Num_Controls	Controls_Ref	Controls_Het	Controls_Alt	Info
#ENSG00000187583.0of5_1pct.singleton	1	974537	ref	0of5_1pct.singleton	PurpleHair	COHORT_NAME	ADD-WGR-FIRTH	3.03573	0.000113817	80969	0.830892	0.0045045	50	49	1	0	61	61	0	0	REGENIE_BETA=1.110452;REGENIE_SE=5.199774;MAC=1.000000;DF=1

#if [ ! -f "${out}_${phenoName}.regenie.gz" ]
#then
#	EXITCODE=1
#else
#	(zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print "#"$0,"MAF","MAC","P"}'; zcat ${out}_${phenoName}.regenie.gz | sed '1,2d' | awk 'BEGIN { OFS="\t" } {$1=$1; if($6 > 0.5) { maf=1-$6 } else { maf=$6 }; print $0,maf,sprintf("%.0f",2*$7*maf),10^(-$NF)}') | $bgzip -c > ${out}.results.tsv.bgz
#	rm ${out}_${phenoName}.regenie.gz
#	EXITCODE=0
#fi
#
#exit $EXITCODE
