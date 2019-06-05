#!/bin/bash

sitesVcf=$1
cpus=$2
fasta=$3
dirCache=$4
dirPlugins=$5
dbNSFP=$6
results=$7
warnings=$8

vep -i $sitesVcf \
--fork $cpus \
--force_overwrite \
--no_stats \
--offline \
--fasta $fasta \
--tab \
--compress_output bgzip \
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
--assembly GRCh37 \
--flag_pick_allele \
--pick_order tsl,biotype,appris,rank,ccds,canonical,length \
--domains flags \
--plugin LoFtool \
--plugin dbNSFP,${dbNSFP},ALL \
--output_file $results \
--warning_file $warnings

if [ ! -f "$warnings" ]; then
	touch $warnings
fi
