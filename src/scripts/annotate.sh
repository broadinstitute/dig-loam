#!/bin/bash

sitesVcf=$1
cpus=$2
fasta=$3
dirCache=$4
dirPlugins=$5
dbNSFP=$6
results=$7
warnings=$8
header=$9
referenceGenome=$10

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
--polyphen b \
--sift b \
--ccds \
--canonical \
--appris \
--tsl \
--biotype \
--regulatory \
--assembly $referenceGenome \
--flag_pick_allele \
--pick_order tsl,biotype,appris,rank,ccds,canonical,length \
--domains flags \
--plugin LoF \
--plugin LoFtool \
--plugin dbNSFP,${dbNSFP},ALL \
--output_file STDOUT \
--warning_file $warnings \
| awk -v h=$header '/^##/{print > h; next} 1' | bgzip -c > $results

if [ ! -f "$warnings" ]; then
	touch $warnings
fi
