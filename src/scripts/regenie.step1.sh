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
        --exclude)
			if [ "$2" ]; then
				exclude=$2
				shift
			else
				echo "ERROR: --exclude requires a non-empty argument."
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
echo "exclude: $exclude"
echo "out: $out"
echo "log: $log"
echo "cliOptions: $cliOptions"

$regenie \
--step 1 \
--bed $bed \
--covarFile $covarFile \
--phenoFile $phenoFile \
--exclude $exclude \
$cliOptions \
--lowmem \
--lowmem-prefix ${out}.tmp_rg \
--out $out \
> $log

if [ ! -f "${out}_1.loco" ]
then
	EXITCODE=1
else
	EXITCODE=0
fi

exit $EXITCODE
