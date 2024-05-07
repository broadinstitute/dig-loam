#!/bin/bash

while :; do
	case $1 in
		--plink-path)
			if [ "$2" ]; then
				plinkPath=$2
				shift
			else
				echo "ERROR: --plink-path requires a non-empty argument."
				exit 1
			fi
			;;
		--other-bim)
			if [ "$2" ]; then
				otherBim=$2
				shift
			else
				echo "ERROR: --other-bim requires a non-empty argument."
				exit 1
			fi
			;;
		--other-nonkg-remove)
			if [ "$2" ]; then
				otherNonkgRemove=$2
				shift
			else
				echo "ERROR: --other-nonkg-remove requires a non-empty argument."
				exit 1
			fi
			;;
		--other-nonkg-flip)
			if [ "$2" ]; then
				otherNonkgFlip=$2
				shift
			else
				echo "ERROR: --other-nonkg-flip requires a non-empty argument."
				exit 1
			fi
			;;
        --other-nonkg-force-a1)
			if [ "$2" ]; then
				otherNonkgForceA1=$2
				shift
			else
				echo "ERROR: --other-nonkg-force-a1 requires a non-empty argument."
				exit 1
			fi
			;;
		--snps-huref-bfile)
			if [ "$2" ]; then
				snpsHurefBfile=$2
				shift
			else
				echo "ERROR: --snps-huref-bfile requires a non-empty argument."
				exit 1
			fi
			;;
		--other-huref-bfile)
			if [ "$2" ]; then
				otherHurefBfile=$2
				shift
			else
				echo "ERROR: --other-huref-bfile requires a non-empty argument."
				exit 1
			fi
			;;
		--other-bfile)
			if [ "$2" ]; then
				otherBfile=$2
				shift
			else
				echo "ERROR: --other-bfile requires a non-empty argument."
				exit 1
			fi
			;;
		--ref-bfile)
			if [ "$2" ]; then
				refBfile=$2
				shift
			else
				echo "ERROR: --ref-bfile requires a non-empty argument."
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

echo "--plink-path: $plinkPath"
echo "--other-bim: $otherBim"
echo "--other-nonkg-remove: $otherNonkgRemove"
echo "--other-nonkg-flip: $otherNonkgFlip"
echo "--other-nonkg-force-a1: $otherNonkgForceA1"
echo "--snps-huref-bfile: $snpsHurefBfile"
echo "--other-huref-bfile: $otherHurefBfile"
echo "--other-bfile: $otherBfile"
echo "--ref-bfile: $refBfile"

EXITCODE=0

n=`wc -l $otherBim | awk '{print $1}'`
m=`wc -l $otherNonkgRemove | awk '{print \$1}'`
if [ $n -eq $m ]
then
	echo "${n} == ${m}: excluding other variants"
	touch ${otherHurefBfile}.bed
	touch ${otherHurefBfile}.bim
	touch ${otherHurefBfile}.fam
	$plinkPath \
		--bfile $snpsHurefBfile \
		--allow-no-sex \
		--output-chr MT \
		--make-bed \
		--keep-allele-order \
		--out $refBfile \
		--memory 57600.0 \
		--seed 1
else
	echo "${n} != ${m}: merging snps and other variants"
	$plinkPath \
		--bfile $otherBfile \
		--allow-no-sex \
		--exclude $otherNonkgRemove \
		--flip $otherNonkgFlip \
		--a1-allele $otherNonkgForceA1 \
		--output-chr MT \
		--make-bed \
		--out $otherHurefBfile \
		--memory 57600.0 \
		--seed 1 \
	&& \
	$plinkPath \
		--bfile $snpsHurefBfile \
		--allow-no-sex \
		--bmerge $otherHurefBfile \
		--output-chr MT \
		--make-bed \
		--keep-allele-order \
		--out $refBfile \
		--memory 57600.0 \
		--seed 1
fi

if [ $? != 0 ]
then
	echo "harmonize - merge snps to other failed"
	EXITCODE=1
fi

exit $EXITCODE
