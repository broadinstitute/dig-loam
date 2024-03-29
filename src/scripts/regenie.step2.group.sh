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
		--batch)
			if [ "$2" ]; then
				batch=$2
				shift
			else
				echo "ERROR: --batch requires a non-empty argument."
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
echo "phenoTable: $phenoTable"
echo "chr: $chr"
echo "batch: $batch"
echo "splitPhenoOut: $splitPhenoOut"
echo "pred: $pred"
echo "annoFile: $annoFile"
echo "setList: $setList"
echo "maskDef: $maskDef"
echo "out: $out"
echo "cliOptions: $cliOptions"
echo ""

if [ $splitPhenoOut -eq 1 ]
then
	splitPhenoOutString=""
else
	splitPhenoOutString="--no-split"
fi

EXITCODE=0

mask=`head -1 $maskDef | awk '{print $1}'`
echo "mask: $mask"

if [ "$chr" != "NA" ]
then
	nVars=`awk -v c=$chr -v m=$mask '{split($1,a,":"); if($3==m && a[1]==c) print $0}' $annoFile | wc -l`
	echo "annotated variants on chromosome ${chr}: $nVars"

	if [ $nVars -gt 0 ]
	then
		$regenie \
		--step 2 \
		--bgen $bgen \
		--sample $sample \
		--ref-first \
		--covarFile $covarFile \
		--phenoFile $phenoFile \
		$cliOptions \
		$splitPhenoOutString \
		--chr $chr \
		--pred $pred \
		--anno-file $annoFile \
		--set-list $setList \
		--mask-def $maskDef \
		--out $out \
		--gz \
		--verbose
	
		if [ $? != 0 ]
		then
			echo "regenie step 2 failed"
			EXITCODE=1
		fi

		if [ $splitPhenoOut -eq 1 ]
		then
			while read line
			do
				p=`echo "${line}" | awk -F'\t' '{print $1}'`
				pAnalyzed=`echo "${line}" | awk -F'\t' '{print $5}'`
				echo "updating $p!"
				if [ ! -f "${out}_${pAnalyzed}.regenie.gz" ]
				then
					echo "no successful tests for phenotype ${p}!"
					EXITCODE=1
				else
					logp_col=`zcat ${out}_${pAnalyzed}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep LOG10P | awk '{print $1}'`
					id_col=`zcat ${out}_${pAnalyzed}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep ID | awk '{print $1}'`
					a1_col=`zcat ${out}_${pAnalyzed}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep ALLELE1 | awk '{print $1}'`
					(zcat ${out}_${pAnalyzed}.regenie.gz | head -2 | tail -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print $0,"P"}'; zcat ${out}_${pAnalyzed}.regenie.gz | sed '1,2d' | awk -v c=$logp_col -v id=$id_col -v a1col=$a1_col 'BEGIN { OFS="\t" } {split($id,a,"."); $id=a[1]; $a1col=a[2]; if(a[3]!="singleton") { print $0,10^(-$c) }}' | sort -T . -k1,1n -k2,2n) | $bgzip -c > ${out}.${p}.results.tsv.bgz
					rm ${out}_${pAnalyzed}.regenie.gz
				fi
			done < <(sed '1d' $phenoTable | awk -v batch=$batch -F'\t' '{if($7 == batch) print $0}')
		else
			if [ ! -f "${out}.regenie.gz" ]
			then
				echo "no successful tests!"
				EXITCODE=1
			else
				#logp_col=`zcat ${out}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep LOG10P | awk '{print $1}'`
				id_col=`zcat ${out}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep ID | awk '{print $1}'`
				a1_col=`zcat ${out}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep ALLELE1 | awk '{print $1}'`
				#(zcat ${out}.regenie.gz | head -2 | tail -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print $0,"P"}'; zcat ${out}.regenie.gz | sed '1,2d' | awk -v c=$logp_col -v id=$id_col -v a1col=$a1_col 'BEGIN { OFS="\t" } {split($id,a,"."); $id=a[1]; $a1col=a[2]; if(a[3]!="singleton") { print $0,10^(-$c) }}' | sort -T . -k1,1n -k2,2n) | $bgzip -c > ${out}.results.tsv.bgz
				(zcat ${out}.regenie.gz | head -2 | tail -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print $0}'; zcat ${out}.regenie.gz | sed '1,2d' | awk -v id=$id_col -v a1col=$a1_col 'BEGIN { OFS="\t" } {split($id,a,"."); $id=a[1]; $a1col=a[2]; if(a[3]!="singleton") { print $0 }}' | sort -T . -k1,1n -k2,2n) | $bgzip -c > ${out}.results.tsv.bgz
				rm ${out}.regenie.gz
			fi
		fi
	else
		if [ -f ${out}.log ]
		then
			rm ${out}.log
		fi
		if [ $splitPhenoOut -eq 1 ]
		then
			while read line
			do
				p=`echo "${line}" | awk -F'\t' '{print $1}'`
				echo "no annotated variants found on chromosome ${chr} for phenotype ${p}"
				echo "no annotated variants found on chromosome ${chr} for phenotype ${p}" >> ${out}.log
				if [[ $cliOptions == *"--af-cc"* ]]
				then
					echo -e "CHROM\tGENPOS\tID\tALLELE0\tALLELE1\tA1FREQ\tA1FREQ_CASES\tA1FREQ_CONTROLS\tN\tTEST\tBETA\tSE\tCHISQ\tLOG10P\tEXTRA\tP" | bgzip -c > ${out}.${p}.results.tsv.bgz
				else
					echo -e "CHROM\tGENPOS\tID\tALLELE0\tALLELE1\tA1FREQ\tN\tTEST\tBETA\tSE\tCHISQ\tLOG10P\tEXTRA\tP" | bgzip -c > ${out}.${p}.results.tsv.bgz
				fi
			done < <(sed '1d' $phenoTable | awk -v batch=$batch -F'\t' '{if($7 == batch) print $0}')
		fi
	fi
else
	$regenie \
	--step 2 \
	--bgen $bgen \
	--sample $sample \
	--ref-first \
	--covarFile $covarFile \
	--phenoFile $phenoFile \
	$cliOptions \
	$splitPhenoOutString \
	--pred $pred \
	--anno-file $annoFile \
	--set-list $setList \
	--mask-def $maskDef \
	--out $out \
	--gz \
	--verbose

	if [ $? != 0 ]
	then
		echo "regenie step 2 failed"
		EXITCODE=1
	fi

	if [ $splitPhenoOut -eq 1 ]
	then
		while read line
		do
			p=`echo "${line}" | awk -F'\t' '{print $1}'`
			pAnalyzed=`echo "${line}" | awk -F'\t' '{print $5}'`
			echo "updating $p!"
			if [ ! -f "${out}_${pAnalyzed}.regenie.gz" ]
			then
				echo "no successful tests for phenotype ${p}!"
				EXITCODE=1
			else
				logp_col=`zcat ${out}_${pAnalyzed}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep LOG10P | awk '{print $1}'`
				id_col=`zcat ${out}_${pAnalyzed}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep ID | awk '{print $1}'`
				a1_col=`zcat ${out}_${pAnalyzed}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep ALLELE1 | awk '{print $1}'`
				(zcat ${out}_${pAnalyzed}.regenie.gz | head -2 | tail -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print $0,"P"}'; zcat ${out}_${pAnalyzed}.regenie.gz | sed '1,2d' | awk -v c=$logp_col -v id=$id_col -v a1col=$a1_col 'BEGIN { OFS="\t" } {split($id,a,"."); $id=a[1]; $a1col=a[2]; if(a[3]!="singleton") { print $0,10^(-$c) }}' | sort -T . -k1,1n -k2,2n) | $bgzip -c > ${out}.${p}.results.tsv.bgz
				rm ${out}_${pAnalyzed}.regenie.gz
			fi
		done < <(sed '1d' $phenoTable | awk -v batch=$batch -F'\t' '{if($7 == batch) print $0}')
	else
		if [ ! -f "${out}.regenie.gz" ]
		then
			echo "no successful tests!"
			EXITCODE=1
		else
			#logp_col=`zcat ${out}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep LOG10P | awk '{print $1}'`
			id_col=`zcat ${out}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep ID | awk '{print $1}'`
			a1_col=`zcat ${out}.regenie.gz | head -2 | tail -1 | tr ' ' '\n' | awk '{print NR" "$0}' | grep ALLELE1 | awk '{print $1}'`
			#(zcat ${out}_${pAnalyzed}.regenie.gz | head -2 | tail -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print $0,"P"}'; zcat ${out}_${pAnalyzed}.regenie.gz | sed '1,2d' | awk -v c=$logp_col -v id=$id_col -v a1col=$a1_col 'BEGIN { OFS="\t" } {split($id,a,"."); $id=a[1]; $a1col=a[2]; if(a[3]!="singleton") { print $0,10^(-$c) }}' | sort -T . -k1,1n -k2,2n) | $bgzip -c > ${out}.${p}.results.tsv.bgz
			(zcat ${out}.regenie.gz | head -2 | tail -1 | awk 'BEGIN { OFS="\t" } {$1=$1; print $0}'; zcat ${out}.regenie.gz | sed '1,2d' | awk -v id=$id_col -v a1col=$a1_col 'BEGIN { OFS="\t" } {split($id,a,"."); $id=a[1]; $a1col=a[2]; if(a[3]!="singleton") { print $0 }}' | sort -T . -k1,1n -k2,2n) | $bgzip -c > ${out}.results.tsv.bgz
			rm ${out}.regenie.gz
		fi
	fi
fi

exit $EXITCODE
