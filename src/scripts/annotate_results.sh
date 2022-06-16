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
referenceGenome=$8

(tabix -H $sitesVcf; \
(while read line; do \
chr=`echo $line | awk '{print $1}'`; \
pos=`echo $line | awk '{print $2}'`; \
rsid=`echo $line | awk '{print $3}'`; \
ref=`echo $line | awk '{print $4}'`; \
alt=`echo $line | awk '{print $5}'`; \
tabix $sitesVcf ${chr}:${pos}-${pos} | awk 'BEGIN { OFS="\t" } {$1=gsub("chr","",$1); print $1"\t"$0}' | \
awk 'BEGIN { OFS="\t" } {if($1 == "X") { $1 = "23" } print $0}' | \
awk 'BEGIN { OFS="\t" } {if($1 == "Y") { $1 = "24" } print $0}' | \
awk 'BEGIN { OFS="\t" } {if($1 == "MT") { $1 = "25" } print $0}' | \
awk -v rsid=$rsid -v ref=$ref -v alt=$alt '{if($4 == rsid && $5 == ref && $6 == alt) print $0}'; \
done < $topResults) | sort -n -k1,1 -k3,3 | cut -f2-) | bgzip -c > ${results}.1.tmp

tabix -p vcf ${results}.1.tmp

vep -i ${results}.1.tmp \
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
--assembly $referenceGenome \
--output_file STDOUT \
--warning_file ${results}.2.tmp.warnings \
| awk -v h=${results}.2.tmp.header '/^##/{print > h; next} 1' > ${results}.2.tmp

if [ -f "${results}.2.tmp.warnings" ]; then
	rm ${results}.2.tmp.warnings
fi

rm ${results}.2.tmp.header

echo -e "ID\tGENE\tCONSEQUENCE\tIMPACT\tCHANGE" > ${results}.3.tmp
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
		echo -e "${ID}\t${GENE}\t${CONSEQUENCE}\t${IMPACT}\tp.${A1TRANS}${PPOS}${A2TRANS}" >> ${results}.3.tmp
	else
		echo -e "${ID}\t${GENE}\t${CONSEQUENCE}\t${IMPACT}\t-" >> ${results}.3.tmp
	fi
done < <(sed '1d' ${results}.2.tmp)

(head -1 $topResults | awk 'BEGIN{OFS="\t"}{print "#Uploaded variation\t"$0}'; sed '1d' $topResults | awk 'BEGIN{OFS="\t"}{print $3"\t"$0}') > ${results}.4.tmp

p=`head -1 $topResults | tr '\t' '\n' | grep -n "pval" | awk -F':' '{print $1}'`

h1=`head -1 ${results}.4.tmp | cut -f2-`
h2=`head -1 ${results}.3.tmp | cut -f2-`
(echo -e "${h1}\t${h2}"; join -1 3 -2 1 -t $'\t' <(sed '1d' $topResults | sort -k3,3) <( sed '1d' ${results}.3.tmp | sort -k1,1)) | sort -n -k${p},${p} | awk 'BEGIN { OFS="\t" } {x=$1; $1=$2; $2=$3; $3=x; print $0}' > $results

rm ${results}.*.tmp*
