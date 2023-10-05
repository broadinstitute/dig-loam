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
results=$6
referenceGenome=$7

echo "sitesVcf=${sitesVcf}"
echo "topResults=${topResults}"
echo "cpus=${cpus}"
echo "fasta=${fasta}"
echo "dirCache=${dirCache}"
echo "results=${results}"
echo "referenceGenome=${referenceGenome}"

(tabix -H $sitesVcf; \
(while read line; do \
chr=`echo $line | awk -v rg=$referenceGenome '{if(rg=="GRCh38") { c="chr" } else { c="" }; print c$1}'`; \
pos=`echo $line | awk '{print $2}'`; \
rsid=`echo $line | awk '{print $3}'`; \
ref=`echo $line | awk '{print $4}'`; \
alt=`echo $line | awk '{print $5}'`; \
tabix $sitesVcf ${chr}:${pos}-${pos} | awk 'BEGIN { OFS="\t" } {gsub("chr","",$1); print $1"\t"$0}' | \
awk 'BEGIN { OFS="\t" } {if($1 == "X") { $1 = "23" } print $0}' | \
awk 'BEGIN { OFS="\t" } {if($1 == "Y") { $1 = "24" } print $0}' | \
awk 'BEGIN { OFS="\t" } {if($1 == "MT") { $1 = "25" } print $0}' | \
awk -v rg=$referenceGenome -v rsid=$rsid -v ref=$ref -v alt=$alt '{if(rg=="GRCh38") { id="chr"rsid } else { id=rsid }; if($4 == id && $5 == ref && $6 == alt) print $0}'; \
done < $topResults) | sort -n -k1,1 -k3,3 | cut -f2-) | bgzip -c > ${results}.1.tmp

tabix -p vcf ${results}.1.tmp

if [ $cpus -gt 1 ]
then
	fork="--fork $cpus"
    echo $fork
else
	fork=""
fi

vep -i ${results}.1.tmp \
$fork \
--format vcf \
--verbose \
--force_overwrite \
--no_stats \
--offline \
--fasta $fasta \
--tab \
--cache \
--dir_cache $dirCache \
--dir_plugins "/usr/local/bin/VEP_plugins" \
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
| awk -v h=${results}.2.tmp.header '/^2023/{print > h; next} 1' \
| awk -v h=${results}.2.tmp.header '/^##/{print >> h; next} 1' > ${results}.2.tmp

exitcodes=("${PIPESTATUS[@]}")

if [ "${exitcodes[0]}" -ne "0" ]
then
	echo ">>> vep first piped command failed with exit code ${exitcodes[0]}"
	exit 1
fi
if [ "${exitcodes[1]}" -ne "0" ]
then
	echo ">>> vep second piped command failed with exit code ${exitcodes[1]}"
	exit 1
fi

warnings=`echo $results | sed 's/tsv/warnings/g'`
if [ -f "${results}.2.tmp.warnings" ]; then
	mv ${results}.2.tmp.warnings $warnings
else
	touch $warnings
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

h1=`head -1 $topResults`
h2=`head -1 ${results}.3.tmp | cut -d$'\t' -f2-`

(echo -e "${h1}\t${h2}"; join -1 3 -2 1 -t $'\t' <(sed '1d' $topResults | sort -k3,3) <( sed '1d' ${results}.3.tmp | sed 's/chr//g' | sort -k1,1) | awk -F'\t' 'BEGIN { OFS="\t" } {x=$1; $1=$2; $2=$3; $3=x; print $0}') > ${results}.4.tmp
p=`head -1 $topResults | tr '\t' '\n' | grep -w -n "P" | awk -F':' '{print $1}'`
sort -n -k${p},${p} ${results}.4.tmp > $results

rm ${results}.*.tmp*

exit 0
