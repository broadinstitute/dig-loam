#!/bin/bash

sitesVcf=$1
topResults=$2
cpus=$3
fasta=$4
dirCache=$5
dirPlugins=$6
results=$7
warnings=$8
header=$9
topResultsSitesVcf=${10}

(tabix -H $sitesVcf; \
(while read line; do \
chr=`echo $line | awk '{print $1}'`; \
pos=`echo $line | awk '{print $2}'`; \
rsid=`echo $line | awk '{print $3}'`; \
ref=`echo $line | awk '{print $4}'`; \
alt=`echo $line | awk '{print $5}'`; \
tabix $sitesVcf ${chr}:${pos}-${pos} | awk '{print $1"\t"$0}' | \
awk 'BEGIN { OFS="\t" } {if($1 == "X") { $1 = "23" } print $0}' | \
awk 'BEGIN { OFS="\t" } {if($1 == "Y") { $1 = "24" } print $0}' | \
awk 'BEGIN { OFS="\t" } {if($1 == "MT") { $1 = "25" } print $0}' | \
awk -v rsid=$rsid -v ref=$ref -v alt=$alt '{if($4 == rsid && $5 == ref && $6 == alt) print $0}'; \
done < $topResults) | sort -n -k1,1 -k3,3 | cut -f2-) | bgzip -c > $topResultsSitesVcf

tabix -p vcf $topResultsSitesVcf

vep -i $topResultsSitesVcf \
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
--warning_file $warnings \
| awk -v h=$header '/^##/{print > h; next} 1' > $results

if [ ! -f "$warnings" ]; then
	touch $warnings
fi
