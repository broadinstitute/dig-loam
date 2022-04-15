#!/bin/bash

while :; do
	case $1 in
		--groupf)
			if [ "$2" ]; then
				groupf=$2
				shift
			else
				echo "ERROR: --groupf requires a non-empty argument."
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

echo "groupf: $groupf"
echo "results: $results"
echo "out: $out"

group1=`head -1 $groupf | awk '{print $1}'`
results1=`echo $results | sed "s/___GROUP___/${group1}/g"`; \

(zcat $results1 | head -1; \
(while read line; do \
	groupid=`echo $line | awk '{print $1}'`; \
	groupResults=`echo $results | sed "s/___GROUP___/${groupid}/g"`; \
	zcat $groupResults | sed '1d'; \
done < $groupf) | awk '{if($1 == "X") { idx=23 } else { if($1 == "Y") { idx=24 } else { if($1 == "MT") { idx=25 } else { idx=$1 } } } print idx"\t"$0 }' | sort -T . -n -k1,1 -k3,3 | cut -f2- \
) | bgzip -c > $out
