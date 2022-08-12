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
referenceGenome=${10}
gnomAD=${11}

# removed options
# --af_gnomad \

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
--hgvs \
--hgvsg \
--symbol \
--nearest symbol \
--check_existing \
--assembly $referenceGenome \
--flag_pick_allele \
--pick_order tsl,biotype,appris,rank,ccds,canonical,length \
--domains flags \
--plugin LoF \
--plugin LoFtool \
--plugin dbNSFP,${dbNSFP},ALL \
--custom ${gnomAD},gnomADg,vcf,exact,0,AC,AF,AN,AC_AFR,AC_AMR,AC_ASJ,AC_EAS,AC_FIN,AC_NFE,AC_OTH,AC_SAS,AC_Male,AC_Female,AN_AFR,AN_AMR,AN_ASJ,AN_EAS,AN_FIN,AN_NFE,AN_OTH,AN_SAS,AN_Male,AN_Female,AF_AFR,AF_AMR,AF_ASJ,AF_EAS,AF_FIN,AF_NFE,AF_OTH,AF_SAS,AF_Male,AF_Female,AC_raw,AN_raw,AF_raw,POPMAX,AC_POPMAX,AN_POPMAX,AF_POPMAX \
--output_file STDOUT \
--warning_file $warnings \
| awk -v h=$header '/^##/{print > h; next} 1' | bgzip -c > $results

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
if [ "${exitcodes[2]}" -ne "0" ]
then
	echo ">>> vep third piped command failed with exit code ${exitcodes[2]}"
	exit 1
fi

if [ ! -f "$warnings" ]; then
	touch $warnings
fi

exit 0
