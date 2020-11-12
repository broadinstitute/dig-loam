#!/bin/bash

translateAcid() {
	case $1 in
		"A")
			echo "Ala"
			;;
		"R")
			echo "Arg"
			;;
		"N")
			echo "Asn"
			;;
		"D")
			echo "Asp"
			;;
		"C")
			echo "Cys"
			;;
		"E")
			echo "Glu"
			;;
		"Q")
			echo "Gln"
			;;
		"G")
			echo "Gly"
			;;
		"H")
			echo "His"
			;;
		"I")
			echo "Ile"
			;;
		"L")
			echo "Leu"
			;;
		"K")
			echo "Lys"
			;;
		"M")
			echo "Met"
			;;
		"F")
			echo "Phe"
			;;
		"P")
			echo "Pro"
			;;
		"S")
			echo "Ser"
			;;
		"T")
			echo "Thr"
			;;
		"W")
			echo "Trp"
			;;
		"Y")
			echo "Tyr"
			;;
		"V")
			echo "Val"
			;;
		*)
			echo "-"
			;;
	esac
}

sitesVcf=$1
topResults=$2
cpus=$3
fasta=$4
dirCache=$5
dirPlugins=$6
results=$7

vep -i $sitesVcf \
--fork $cpus \
--force_overwrite \
--no_stats \
--offline \
--fasta $fasta \
--tab \
--cache \
--dir_cache $dirCache \
--dir_plugins $dirPlugins \
--canonical \
--symbol \
--nearest symbol \
--ccds \
--appris \
--tsl \
--biotype \
--regulatory \
--pick_allele \
--pick_order tsl,biotype,appris,rank,ccds,canonical,length \
--assembly GRCh37 \
--output_file STDOUT \
--warning_file ${results}.1.tmp.warnings \
| awk -v h=${results}.1.tmp.header '/^##/{print > h; next} 1' > ${results}.1.tmp

if [ -f "${results}.1.tmp.warnings" ]; then
	rm ${results}.1.tmp.warnings
fi

rm ${results}.1.tmp.header

echo -e "ID\tGENE\tCONSEQUENCE\tIMPACT\tCHANGE" > ${results}.2.tmp
while read line; do
	ID=`echo $line | awk '{print $1}'`
	GENE=`echo $line | awk '{print $18}'`
	CONSEQUENCE=`echo $line | awk '{print $7}'`
	IMPACT=`echo $line | awk '{print $14}'`
	AA=`echo $line | awk '{print $11}'`
	PPOS=`echo $line | awk '{print $10}'`
	if [[ "${AA}" != "-" && "${PPOS}" != "-" ]]; then
		A1=`echo $line | awk '{print $11}' | awk -F'/' '{print $1}'`
		A2=`echo $line | awk '{print $11}' | awk -F'/' '{print $2}'`
		A1TRANS=`translateAcid $A1`
		A2TRANS=`translateAcid $A2`
		echo -e "${ID}\t${GENE}\t${CONSEQUENCE}\t${IMPACT}\tp.${A1TRANS}${PPOS}${A2TRANS}" >> ${results}.2.tmp
	else
		echo -e "${ID}\t${GENE}\t${CONSEQUENCE}\t${IMPACT}\t-" >> ${results}.2.tmp
	fi
done < <(sed '1d' ${results}.1.tmp)

(head -1 $topResults | awk 'BEGIN{OFS="\t"}{print "#Uploaded variation\t"$0}'; sed '1d' $topResults | awk 'BEGIN{OFS="\t"}{print $1"\t"$0}') > ${results}.3.tmp

p=`head -1 $topResults | tr '\t' '\n' | grep -n "pvalue" | awk -F':' '{print $1}'`

h1=`head -1 ${results}.3.tmp | cut -f2-`
h2=`head -1 ${results}.2.tmp | cut -f2-`
(echo -e "${h1}\t${h2}"; (join -a 1 -t $'\t' <(sed '1d' $topResults | sort -k1,1) <( sed '1d' ${results}.2.tmp | sort -k1,1) | sort -g -k${p},${p})) > $results

rm ${results}.*.tmp*
