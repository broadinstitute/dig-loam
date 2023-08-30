#!/bin/bash

while :; do
	case $1 in
        --sites-vcf)
			if [ "$2" ]; then
				sitesVcf=$2
				shift
			else
				echo "ERROR: --sites-vcf requires a non-empty argument."
				exit 1
			fi
			;;
		--cpus)
			if [ "$2" ]; then
				cpus=$2
				shift
			else
				echo "ERROR: --cpus requires a non-empty argument."
				exit 1
			fi
			;;
		--fasta)
			if [ "$2" ]; then
				fasta=$2
				shift
			else
				echo "ERROR: --fasta requires a non-empty argument."
				exit 1
			fi
			;;
		--dir-cache)
			if [ "$2" ]; then
				dirCache=$2
				shift
			else
				echo "ERROR: --dir-cache requires a non-empty argument."
				exit 1
			fi
			;;
		--dir-plugins)
			if [ "$2" ]; then
				dirPlugins=$2
				shift
			else
				echo "ERROR: --dir-plugins requires a non-empty argument."
				exit 1
			fi
			;;
		--dbnsfp)
			if [ "$2" ]; then
				dbnsfp=$2
				shift
			else
				echo "ERROR: --dbnsfp requires a non-empty argument."
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
		--warnings)
			if [ "$2" ]; then
				warnings=$2
				shift
			else
				echo "ERROR: --warnings requires a non-empty argument."
				exit 1
			fi
			;;
		--header)
			if [ "$2" ]; then
				header=$2
				shift
			else
				echo "ERROR: --header requires a non-empty argument."
				exit 1
			fi
			;;
		--reference-genome)
			if [ "$2" ]; then
				referenceGenome=$2
				shift
			else
				echo "ERROR: --reference-genome requires a non-empty argument."
				exit 1
			fi
			;;
		--gnomad)
			if [ "$2" ]; then
				gnomad=$2
				shift
			else
				echo "ERROR: --gnomad requires a non-empty argument."
				exit 1
			fi
			;;
		--gerpbw)
			if [ "$2" ]; then
				gerpbw=$2
				shift
			else
				echo "ERROR: --gerpbw requires a non-empty argument."
				exit 1
			fi
			;;
		--conservation)
			if [ "$2" ]; then
				conservation=$2
				shift
			else
				echo "ERROR: --conservation requires a non-empty argument."
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

echo "--sites-vcf $sitesVcf"
echo "--cpus $cpus"
echo "--fasta: $fasta"
echo "--dir-cache $dirCache"
echo "--dir-plugins $dirPlugins"
echo "--dbnsfp $dbnsfp"
echo "--results $results"
echo "--warnings $warnings"
echo "--header $header"
echo "--reference-genome $referenceGenome"
echo "--gnomad $gnomad"
echo "--gerpbw $gerpbw"
echo "--conservation $conservation"

# check loftee requirements
if [ "$referenceGenome" == "GRCh38" ]
then
	lofteeOptions="loftee_path:${dirPlugins},gerp_bigwig:${gerpbw},conservation_file:${conservation},human_ancestor_fa:${fasta}"
elif [ "$referenceGenome" == "GRCh37" ]
then
	lofteeOptions="loftee_path:${dirPlugins},conservation_file:${conservation},human_ancestor_fa:${fasta}"
else
	echo "...reference genome $referenceGenome not currently supported!"
	exit 1
fi
echo "lofteeOptions: $lofteeOptions"

if [ $cpus -gt 1 ]
then
	fork="--fork $cpus"
    echo $fork
else
	fork=""
fi

vep -i $sitesVcf \
$fork \
--format vcf \
--verbose \
--force_overwrite \
--dir_plugins $dirPlugins \
--no_stats \
--offline \
--fasta $fasta \
--tab \
--cache \
--dir_cache $dirCache \
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
--domains \
--plugin LoF,${lofteeOptions} \
--plugin LoFtool \
--plugin dbNSFP,${dbnsfp},"ref","alt","rs_dbSNP","hg19_chr","hg19_pos(1-based)","hg18_chr","hg18_pos(1-based)","Ensembl_geneid","VEP_canonical","refcodon","codon_degeneracy","SIFT_score","SIFT_converted_rankscore","SIFT_pred","SIFT4G_score","SIFT4G_converted_rankscore","SIFT4G_pred","Polyphen2_HDIV_score","Polyphen2_HDIV_rankscore","Polyphen2_HDIV_pred","Polyphen2_HVAR_score","Polyphen2_HVAR_rankscore","Polyphen2_HVAR_pred","LRT_score","LRT_converted_rankscore","LRT_pred","LRT_Omega","MutationTaster_score","MutationTaster_converted_rankscore","MutationTaster_pred","MutationTaster_model","MutationTaster_AAE","MutationAssessor_score","MutationAssessor_rankscore","MutationAssessor_pred","FATHMM_score","FATHMM_converted_rankscore","FATHMM_pred","PROVEAN_score","PROVEAN_converted_rankscore","PROVEAN_pred","VEST4_score","VEST4_rankscore","MetaSVM_score","MetaSVM_rankscore","MetaSVM_pred","MetaLR_score","MetaLR_rankscore","MetaLR_pred","Reliability_index","MetaRNN_score","MetaRNN_rankscore","MetaRNN_pred","M-CAP_score","M-CAP_rankscore","M-CAP_pred","REVEL_score","REVEL_rankscore","MutPred_score","MutPred_rankscore","MutPred_protID","MutPred_AAchange","MutPred_Top5features","MVP_score","MVP_rankscore","MPC_score","MPC_rankscore","PrimateAI_score","PrimateAI_rankscore","PrimateAI_pred","DEOGEN2_score","DEOGEN2_rankscore","DEOGEN2_pred","BayesDel_addAF_score","BayesDel_addAF_rankscore","BayesDel_addAF_pred","BayesDel_noAF_score","BayesDel_noAF_rankscore","BayesDel_noAF_pred","ClinPred_score","ClinPred_rankscore","ClinPred_pred","LIST-S2_score","LIST-S2_rankscore","LIST-S2_pred","Aloft_Fraction_transcripts_affected","Aloft_prob_Tolerant","Aloft_prob_Recessive","Aloft_prob_Dominant","Aloft_pred","Aloft_Confidence","CADD_raw","CADD_raw_rankscore","CADD_phred","CADD_raw_hg19","CADD_raw_rankscore_hg19","CADD_phred_hg19","DANN_score","DANN_rankscore","fathmm-MKL_coding_score","fathmm-MKL_coding_rankscore","fathmm-MKL_coding_pred","fathmm-MKL_coding_group","fathmm-XF_coding_score","fathmm-XF_coding_rankscore","fathmm-XF_coding_pred","Eigen-raw_coding","Eigen-raw_coding_rankscore","Eigen-phred_coding","Eigen-PC-raw_coding","Eigen-PC-raw_coding_rankscore","Eigen-PC-phred_coding","GenoCanyon_score","GenoCanyon_rankscore","integrated_fitCons_score","integrated_fitCons_rankscore","integrated_confidence_value","GM12878_fitCons_score","GM12878_fitCons_rankscore","GM12878_confidence_value","H1-hESC_fitCons_score","H1-hESC_fitCons_rankscore","H1-hESC_confidence_value","HUVEC_fitCons_score","HUVEC_fitCons_rankscore","HUVEC_confidence_value","LINSIGHT","LINSIGHT_rankscore","GERP++_NR","GERP++_RS","GERP++_RS_rankscore","phyloP100way_vertebrate","phyloP100way_vertebrate_rankscore","phyloP470way_mammalian","phyloP470way_mammalian_rankscore","phyloP17way_primate","phyloP17way_primate_rankscore","phastCons100way_vertebrate","phastCons100way_vertebrate_rankscore","phastCons470way_mammalian","phastCons470way_mammalian_rankscore","phastCons17way_primate","phastCons17way_primate_rankscore","SiPhy_29way_pi","SiPhy_29way_logOdds","SiPhy_29way_logOdds_rankscore","bStatistic","bStatistic_converted_rankscore","gnomAD_exomes_AC","gnomAD_exomes_AF","gnomAD_exomes_controls_AC","gnomAD_exomes_controls_AN","gnomAD_exomes_controls_AF","gnomAD_exomes_controls_nhomalt","gnomAD_genomes_AC","gnomAD_genomes_AN","gnomAD_genomes_AF","gnomAD_genomes_nhomalt","clinvar_id","clinvar_clnsig","clinvar_trait" \
--custom ${gnomad},gnomADg,vcf,exact,0,AC,AF,AN,AC_AFR,AC_AMR,AC_ASJ,AC_EAS,AC_FIN,AC_NFE,AC_OTH,AC_SAS,AC_Male,AC_Female,AN_AFR,AN_AMR,AN_ASJ,AN_EAS,AN_FIN,AN_NFE,AN_OTH,AN_SAS,AN_Male,AN_Female,AF_AFR,AF_AMR,AF_ASJ,AF_EAS,AF_FIN,AF_NFE,AF_OTH,AF_SAS,AF_Male,AF_Female,AC_raw,AN_raw,AF_raw,POPMAX,AC_POPMAX,AN_POPMAX,AF_POPMAX \
--output_file STDOUT \
--warning_file $warnings \
| awk -v h=$header '/^2023/{print > h; next} 1' \
| awk -v h=$header '/^##/{print >> h; next} 1' \
| bgzip -c > $results

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

