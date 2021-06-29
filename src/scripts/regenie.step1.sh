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
		--bed)
			if [ "$2" ]; then
				bed=$2
				shift
			else
				echo "ERROR: --bed requires a non-empty argument."
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
			#shift
			;;
		--lowmem)
			lowmem=true
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
		--log)
			if [ "$2" ]; then
				log=$2
				shift
			else
				echo "ERROR: --log requires a non-empty argument."
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
echo "bed: $bed"
echo "covarFile: $covarFile"
echo "phenoFile: $phenoFile"
echo "blockSize: $blockSize"
echo "bt: $bt"
echo "lowmem: $lowmem"
echo "out: $out"

if [ $bt ]
then
	btString="--bt"
else
	btString=""
fi

if [ $lowmem ]
then
	lowmemString="--lowmem --lowmem-prefix ${out}.tmp_rg"
else
	lowmemString=""
fi

$regenie \
--step 1 \
--bed $bed \
--covarFile $covarFile \
--phenoFile $phenoFile \
--bsize $blockSize \
$btString \
$lowmemString \
--out $out \
> $log

if [ ! -f "${out}_1.loco" ]
then
	EXITCODE=1
else
	EXITCODE=0
fi

exit $EXITCODE
