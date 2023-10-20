#!/bin/bash

while :; do
	case $1 in
		--plink)
			if [ "$2" ]; then
				plink=$2
				shift
			else
				echo "ERROR: --plink requires a non-empty argument."
				exit 1
			fi
			;;
		--bfile)
			if [ "$2" ]; then
				bfile=$2
				shift
			else
				echo "ERROR: --bfile requires a non-empty argument."
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

echo "plink: $plink"
echo "bfile: $bfile"
echo "phenoFile: $phenoFile"
echo "out: $out"

sed '1d' $phenoFile | awk '{print $1" "$1}' > ${out}.tmp.keep

EXITCODE=0

$plink \
--bfile $bfile \
--keep ${out}.tmp.keep \
--hardy \
--out ${out}.tmp.hardy

if [ $? != 0 ]
then
	echo "regenie step 0: plink command failed"
	EXITCODE=1
fi

awk '{split($6,a,"/"); if(a[1]a[2] == "00" || a[1]a[3] == "00" || a[2]a[3] == "00") print $2}' ${out}.tmp.hardy.hwe > ${out}.zero_variance_exclude.txt

if [ ! -f "${out}.zero_variance_exclude.txt" ]
then
	EXITCODE=1
else
	rm ${out}.tmp.*
fi

exit $EXITCODE
