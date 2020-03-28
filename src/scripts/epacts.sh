#!/bin/bash

while :; do
	case $1 in
		--bin)
			if [ "$2" ]; then
				bin=$2
				shift
			else
				echo "ERROR: --bin requires a non-empty argument."
				exit 1
			fi
			;;
		--type)
			if [ "$2" ]; then
				type=$2
				shift
			else
				echo "ERROR: --type requires a non-empty argument."
				exit 1
			fi
			;;
		--vcf)
			if [ "$2" ]; then
				vcf=$2
				shift
			else
				echo "ERROR: --vcf requires a non-empty argument."
				exit 1
			fi
			;;
		--groupfin)
			if [ "$2" ]; then
				groupfin=$2
				shift
			else
				echo "ERROR: --groupf requires a non-empty argument."
				exit 1
			fi
			;;
		--groupfout)
			if [ "$2" ]; then
				groupfout=$2
				shift
			else
				echo "ERROR: --groupf requires a non-empty argument."
				exit 1
			fi
			;;
        --groupid)
			if [ "$2" ]; then
				groupid=$2
				shift
			else
				echo "ERROR: --group requires a non-empty argument."
				exit 1
			fi
			;;
		--region)
			if [ "$2" ]; then
				region=$2
				shift
			else
				echo "ERROR: --region requires a non-empty argument."
				exit 1
			fi
			;;
		--ped)
			if [ "$2" ]; then
				ped=$2
				shift
			else
				echo "ERROR: --ped requires a non-empty argument."
				exit 1
			fi
			;;
		--vars)
			if [ "$2" ]; then
				vars=$2
				shift
			else
				echo "ERROR: --vars requires a non-empty argument."
				exit 1
			fi
			;;
		--test)
			if [ "$2" ]; then
				test=$2
				shift
			else
				echo "ERROR: --test requires a non-empty argument."
				exit 1
			fi
			;;
		--field)
			if [ "$2" ]; then
				field=$2
				shift
			else
				echo "ERROR: --field requires a non-empty argument."
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
		--run)
			if [ "$2" ]; then
				run=$2
				shift
			else
				echo "ERROR: --run requires a non-empty argument."
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

echo "bin: $bin"
echo "type: $type"
echo "vcf: $vcf"
echo "groupfin: $groupfin"
echo "groupid: $groupid"
echo "region: $region"
echo "ped: $ped"
echo "vars: $vars"
echo "test: $test"
echo "field: $field"
echo "groupfout: $groupfout"
echo "out: $out"
echo "run: $run"

i=0
phenoCovars=""
while read line; do \
	i=$((i+1))
	if [ $i -eq 1 ]; then \
		phenoCovars=`echo "--pheno $line"`
	else
		phenoCovars=`echo "$phenoCovars --cov $line"`
	fi
done < $vars

echo "parsed variable file $vars into epacts cli options '$phenoCovars'"

if [[ ! -z "$groupid" && ! -z "${groupfin}" && ! -z "${groupfout}" ]]; then
	grep -w "${groupid}" $groupfin > $groupfout
fi

outBase=`echo $out | sed 's/\.tsv\.bgz//g'`

EXITCODE=0
if [ "$type" == "group" ]; then
	if [ "$test" == "b.collapse" ]; then
		$bin $type \
		--vcf $vcf \
		--groupf $groupfout \
		--ped $ped \
		$phenoCovars \
		--test $test \
		--field $field \
		--out $outBase \
		--run $run \
		--no-plot
	elif [[ "$test" == "b.skat" || "$test" == "q.skat" ]]; then
		$bin $type \
		--vcf $vcf \
		--groupf $groupfout \
		--ped $ped \
		$phenoCovars \
		--test skat \
		--skat-o \
		--field $field \
		--out $outBase \
		--run $run \
		--no-plot
	else
		EXITCODE=1
	fi
	if [ -f "${outBase}.epacts.OK" ]; then
		cat ${outBase}.epacts | bgzip -c > $out
		rm ${outBase}.epacts
	else
		EXITCODE=1
	fi
elif [ "$type" == "single" ]; then
	$bin $type \
	--vcf $vcf \
	--region $region \
	--ped $ped \
	$phenoCovars \
	--test $test \
	--field $field \
	--out $outBase \
	--run $run \
	--no-plot
	if [ -f "${outBase}.epacts.OK" ]; then
		zcat ${outBase}.epacts.gz | bgzip -c > $out
		rm ${outBase}.epacts.gz
		rm ${outBase}.epacts.gz.tbi
	else
		EXITCODE=1
	fi
else
	echo "ERROR: --type argument $type not recognized."
	EXITCODE=1
fi

exit $EXITCODE
