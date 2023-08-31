#!/bin/bash

while :; do
	case $1 in
        --tabix)
			if [ "$2" ]; then
				tabix=$2
				shift
			else
				echo "ERROR: --tabix requires a non-empty argument."
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
		--file)
			if [ "$2" ]; then
				file=$2
				shift
			else
				echo "ERROR: --file requires a non-empty argument."
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

echo "--tabix $tabix"
echo "--bgzip $bgzip"
echo "--file $file"
echo "--region $region"
echo "--out $out"

exitCode=0

$tabix -h $file $region | $bgzip -c > $out
exitCode=$?

$tabix -f -p vcf $out
exitCode=$?

exit $exitCode
