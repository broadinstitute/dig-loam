#!/bin/bash

while :; do
	case $1 in
		--chrs)
			if [ "$2" ]; then
				chrs=$2
				shift
			else
				echo "ERROR: --chrs requires a non-empty argument."
				exit 1
			fi
			;;
        --results)
			if [ "$2" ]; then
				results=$2
				shift
			else
				echo "ERROR: --results requires a non-empty argument."
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

echo "chrs: $chrs"
echo "results: $results"
echo "out: $out"

chrs1=`echo $chrs | awk -F',' '{print $1}'`
results1=`echo $results | sed "s/___CHR___/${chrs1}/g"`; \

IFS=',' read -r -a array <<< "$chrs"

(zcat $results1 | head -2 | tail -1; \
(for chr in "${array[@]}"; do \
	resultsFile=`echo $results | sed "s/___CHR___/${chr}/g"`; \
	zcat $resultsFile | sed '1,2d'; \
done) | awk '{if($1 == "X") { idx=23 } else { if($1 == "Y") { idx=24 } else { if($1 == "MT") { idx=25 } else { idx=$1 } } } print idx"\t"$0 }' | sort -n -k1,1 -k3,3 | cut -f2- \
) | bgzip -c > $out
