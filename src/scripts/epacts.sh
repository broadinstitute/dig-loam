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
		--groupf)
			if [ "$2" ]; then
				groupf=$2
				shift
			else
				echo "ERROR: --groupf requires a non-empty argument."
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
echo "groupf: $groupf"
echo "region: $region"
echo "ped: $ped"
echo "vars: $vars"
echo "test: $test"
echo "field: $field"
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

EXITCODE=0
if [ "$type" == "group" ]; then
	$bin $type \
	--vcf $vcf \
	--groupf $groupf \
	--ped $ped \
	$phenoCovars \
	--test $test \
	--field $field \
	--out $out \
	--run $run \
	--no-plot
	if [ -f "${out}.epacts.OK" ]; then
		mv ${out}.epacts $out
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
	--out $out \
	--run $run \
	--no-plot
	if [ ! -f "${out}.epacts.OK" ]; then
		EXITCODE=1
	fi
else
	echo "ERROR: --type argument $type not recongnized."
	EXITCODE=1
fi

exit $EXITCODE
