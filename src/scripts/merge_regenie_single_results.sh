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

(zcat $results1 | head -1; \
(for chr in "${array[@]}"; do \
	resultsFile=`echo $results | sed "s/___CHR___/${chr}/g"`; \
	zcat $resultsFile | awk 'BEGIN { OFS="\t" } { if($1==23) { $1="X" } else { if($1==24) { idx="Y" } else { if($1==25) { idx="XY" } else { if($1==26) { idx="MT" } } } } print $0 }' | sed '1d'; \
done) | awk 'BEGIN { OFS="\t" } { if($1 == "X") { idx=23 } else { if($1 == "Y") { idx=24 } else { if($1 == "XY") { idx=25 } else { if($1 == "MT") { idx=26 } else { idx=$1 } } } } print idx"\t"$0 }' | sort -T . -n -k1,1 -k3,3 | cut -f2- \
) | bgzip -c > $out
