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
		--group-stats)
			groupStats=true
			#shift
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
echo "groupStats: $groupStats"
echo "out: $out"
echo "cliOptions: $cliOptions"

EXITCODE=0

n=`awk 'NR==FNR{a[$0];next}{if($1 in a) print $2}' <(awk '{print $2}' $annoFile | sort -T . -u) $setList | wc -l`

if [ $n -gt 0 ]
then
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
	--verbose
	
	#CHROM GENPOS ID ALLELE0 ALLELE1 A1FREQ N TEST BETA SE CHISQ LOG10P EXTRA
	
	if [ ! -f "${out}_${phenoName}.regenie.gz" ]
	then
		EXITCODE=1
	else
		n=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep LOG10P | awk '{print $1}'`
		test_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep TEST | awk '{print $1}'`
		id_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep ID | awk '{print $1}'`
		(zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print "UID",$0,"P"}'; zcat ${out}_${phenoName}.regenie.gz | sed '1,2d' | awk -v c=$n -v id=$id_col -v test=$test_col 'BEGIN { OFS="\t" } {$1=$1; print $id":"$test,$0,10^(-$c)}' | sort -T . -k1,1) | $bgzip -c > ${out}.results.tsv.bgz
		#CHROM GENPOS ID ALLELE0 ALLELE1 A1FREQ N TEST BETA SE CHISQ LOG10P EXTRA P
		rm ${out}_${phenoName}.regenie.gz
	fi
	
	if [ $groupStats ]
	
	then
	
		mv  ${out}.results.tsv.bgz  ${out}.results.standard.tsv.bgz
	
		$regenie \
		--step 2 \
		--bgen $bgen \
		--sample $sample \
		--covarFile $covarFile \
		--phenoFile $phenoFile \
		$cliOptions \
		--pred $pred \
		--chr $chr \
		--anno-file $annoFile \
		--set-list $setList \
		--mask-def $maskDef \
		--out $out \
		--gz \
		--htp ALL \
		--verbose
	
		#Name	Chr	Pos	Ref	Alt	Trait	Cohort	Model	Effect	LCI_Effect	UCI_Effect	Pval	AAF	Num_Cases	Cases_Ref	Cases_Het	Cases_Alt	Num_Controls	Controls_Ref	Controls_Het	Controls_Alt	Info
	
		if [ ! -f "${out}_${phenoName}.regenie.gz" ]
		then
			EXITCODE=1
		else
			if [[ $cliOptions == *"--qt"* ]]
			then
				echo "quant: $cliOptions" >> check
				id_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Name | awk '{print $1}'`
				model_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Model | awk '{print $1}'`
				ref_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Cases_Ref | awk '{print $1}'`
				het_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Cases_Het | awk '{print $1}'`
				alt_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Cases_Alt | awk '{print $1}'`
				info_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Info | awk '{print $1}'`
				(echo -e "UID\tModel\tnHomRef\tnHet\tnHomAlt\tMAC"; zcat ${out}_${phenoName}.regenie.gz | sed '1,2d' | awk -v id=$id_col -v model=$model_col -v ref=$ref_col -v het=$het_col -v alt=$alt_col -v info=$info_col 'BEGIN { OFS="\t" } {$1=$1; split($info, x, ";"); mac="NA"; for(i in x){split(x[i],y,"="); if(y[1]=="MAC"){mac=y[2]}}; if($model == "ADD-WGR-ACATO" || $model == "ADD-WGR-ACATV" || $model == "ADD-WGR-BURDEN-ACAT" || $model == "ADD-WGR-BURDEN-MINP" || $model == "ADD-WGR-SKAT" || $model == "ADD-WGR-SKATO" || $model == "ADD-WGR-SKATO-ACAT") { TEST=$model; sub("-WGR-","-",TEST) } else { TEST="ADD" }; print $id":"TEST,$model,$ref,$het,$alt,mac}' | sort -T . -k1,1) | $bgzip -c > ${out}.results.htp.tsv.bgz
				#ID	Model	nHomRef	nHet	nHomAlt	MAC
			else
				echo "binary: $cliOptions" >> check
				id_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Name | awk '{print $1}'`
				model_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Model | awk '{print $1}'`
				case_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Num_Cases | awk '{print $1}'`
				ctrl_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Num_Controls | awk '{print $1}'`
				case_ref_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Cases_Ref | awk '{print $1}'`
				case_het_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Cases_Het | awk '{print $1}'`
				case_alt_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Cases_Alt | awk '{print $1}'`
				ctrl_ref_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Controls_Ref | awk '{print $1}'`
				ctrl_het_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Controls_Het | awk '{print $1}'`
				ctrl_alt_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Controls_Alt | awk '{print $1}'`
				info_col=`zcat ${out}_${phenoName}.regenie.gz | head -2 | tail -1 | tr '\t' '\n' | awk '{print NR" "$0}' | grep Info | awk '{print $1}'`
				(echo -e "UID\tModel\tnHomRefCases\tnHetCases\tnHomAltCases\tnHomRefControls\tnHetControls\tnHomAltControls\tMAC"; zcat ${out}_${phenoName}.regenie.gz | sed '1,2d' | awk -v id=$id_col -v model=$model_col -v case_ref=$case_ref_col -v case_het=$case_het_col -v case_alt=$case_alt_col -v ctrl_ref=$ctrl_ref_col -v ctrl_het=$ctrl_het_col -v ctrl_alt=$ctrl_alt_col -v info=$info_col 'BEGIN { OFS="\t" } {$1=$1; split($info, x, ";"); mac="NA"; for(i in x){split(x[i],y,"="); if(y[1]=="MAC"){mac=y[2]}}; if($model == "ADD-WGR-ACATO" || $model == "ADD-WGR-ACATV" || $model == "ADD-WGR-BURDEN-ACAT" || $model == "ADD-WGR-BURDEN-MINP" || $model == "ADD-WGR-SKAT" || $model == "ADD-WGR-SKATO" || $model == "ADD-WGR-SKATO-ACAT") { TEST=$model; sub("-WGR-","-",TEST) } else { TEST="ADD" }; print $id":"TEST,$model,$case_ref,$case_het,$case_alt,$ctrl_ref,$ctrl_het,$ctrl_alt,mac}' | sort -T . -k1,1) | $bgzip -c > ${out}.results.htp.tsv.bgz
				#ID	Model	nHomRefCases	nHetCases	nHomAltCases	nHomRefControls	nHetControls	nHomAltControls	MAC
			fi
			rm ${out}_${phenoName}.regenie.gz
		fi
	
		(join -1 1 -2 1 -t $'\t' <(zcat ${out}.results.standard.tsv.bgz | head -1) <(zcat ${out}.results.htp.tsv.bgz | head -1); join -1 1 -2 1 -t $'\t' <(zcat ${out}.results.standard.tsv.bgz | sed '1d') <(zcat ${out}.results.htp.tsv.bgz | sed '1d')) | cut -f2- | bgzip -c > ${out}.results.tsv.bgz
	
		rm ${out}.results.standard.tsv.bgz
		rm ${out}.results.htp.tsv.bgz
	
	fi
else
	echo -e "#CHROM\tGENPOS\tID\tALLELE0\tALLELE1\tA1FREQ\tN\tTEST\tBETA\tSE\tCHISQ\tLOG10P\tMAF\tMAC\tP" | bgzip -c > ${out}.results.tsv.bgz
fi

exit $EXITCODE
