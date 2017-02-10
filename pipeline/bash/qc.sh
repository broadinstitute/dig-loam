#!/bin/bash

ANCESTRY=NA
CHROMOSOME=NA
REPLACE=false
while getopts "s:q:c:p:r:l:L:" opt; do
	case $opt in
		s)
			SOURCE=$OPTARG
			;;
		l)
			LABEL=$OPTARG
			;;
		L)
			LABELS+=("$OPTARG")
			;;
		q)
			QC_STEP=$OPTARG
			;;
		p)
			ANCESTRY=$OPTARG
			;;
		r)
			REPLACE=true
			;;
		c)
			CHROMOSOME=$OPTARG
			;;
		\?)
			echo "Invalid option: -$OPTARG" >&2
			exit 1
			;;
		:)
			echo "Option -$OPTARG requires an argument." >&2
			exit 1
			;;
	esac
done

echo "SOURCE=${SOURCE}"
echo "LABEL=${LABEL}"
echo "LABELS=${LABELS[@]}"
echo "QC_STEP=${QC_STEP}"
echo "ANCESTRY=${ANCESTRY}"
echo "REPLACE=${REPLACE}"
echo "CHROMOSOME=${CHROMOSOME}"

# Pipeline source file
# Contains input file and external software/script definitions
source $SOURCE

if [ ! -z $LABEL ]; then
	DATA=${LABEL}
fi

echo ""
echo "Performing QC Step ${QC_STEP} on ${DATA}"

# Description: Align data strand to 1KG reference. Also, update reference allele and variant ID to match 1KG
# Requires: Plink1.9 and, at least, Genotype Harmonizer v1.4.18
# Commandline Requirements: -c
# Directories Required: data
# Input: Plink binary fileset ${DATA}.bed/bim/fam, $KG_VCF_BASE_* reference VCF file, $LABEL output file prefix
# Output: data/${LABEL}.chr${CHROMOSOME}.bed/bim/fam, data/${LABEL}.chr${CHROMOSOME}.harmonized.bed/bim/fam/log(/nosex?/hh?), data/${LABEL}.chr${CHROMOSOME}.harmonized_idUpdates.txt, data/${LABEL}.chr${CHROMOSOME}.harmonized_snpLog.log
# Example: ./QCpipeline -s $SOURCE -q harmonize -c 22
# Notes: 
#    Could also add --variants and --mafAlign as pipeline options, but for now these are static
#    Ideally, this will be run in parallel by chromosome number
# Hail?: No
if [ "$QC_STEP" == "harmonize" ]; then

	if [[ "$CHROMOSOME" == "X" || "$CHROMOSOME" == "23" ]]; then
		KG_VCF_BASE=$KG_VCF_BASE_X
	elif [[ "$CHROMOSOME" == "Y" || "$CHROMOSOME" == "24" ]]; then
		KG_VCF_BASE=$KG_VCF_BASE_Y
	else
		KG_VCF_BASE=$KG_VCF_BASE_AUTO
	fi

	$PLINK --bfile ${!DATA} --chr $CHROMOSOME --keep-allele-order --output-chr MT --make-bed --out data/${LABEL}.chr${CHROMOSOME}

	$GENOTYPE_HARMONIZER \
	--input data/${LABEL}.chr${CHROMOSOME} \
	--inputType PLINK_BED \
	--output data/${LABEL}.chr${CHROMOSOME}.harmonized \
	--outputType PLINK_BED \
	--ref $KG_VCF_BASE \
	--refType VCF \
	--keep \
	--update-id \
	--variants 1000 \
	--mafAlign 0.1 \
	--update-id \
	--update-reference-allele \
	--debug

# Description: Compile harmonized data and generate sample file used in Hail VDS creation in next step
# Requires: Plink1.9, Tabix
# Commandline Requirements: 
# Directories Required: data
# Input: data/${LABEL}.chr${CHROMOSOME}.harmonized.bed/bim/fam
# Output: data/${LABEL}.harmonized.bed/bim/fam/log(/nosex?/hh?), data/${LABEL}.harmonized.force_a2, data/${LABEL}.harmonized.ref.bed/bim/fam/log(/nosex?/hh?), data/${LABEL}.harmonized.sample
# Example: ./QCpipeline -s $SOURCE -q compile
# Notes: 
# Hail?: Possible, but not necessary
elif [ "$QC_STEP" == "compile" ]; then

	echo "data/${LABEL}.chr2.harmonized.bed data/${LABEL}.chr2.harmonized.bim data/${LABEL}.chr2.harmonized.fam" > data/${LABEL}.harmonized.merge.txt
	for i in {3..22} X Y; do if [ -f data/${LABEL}.chr${i}.harmonized.bim ]; then echo "data/${LABEL}.chr${i}.harmonized.bed data/${LABEL}.chr${i}.harmonized.bim data/${LABEL}.chr${i}.harmonized.fam" >> data/${LABEL}.harmonized.merge.txt; fi; done
	$PLINK --bfile data/${LABEL}.chr1.harmonized --merge-list data/${LABEL}.harmonized.merge.txt --make-bed --keep-allele-order --out data/${LABEL}.harmonized
	awk '{print $2,$5}' data/${LABEL}.harmonized.bim > data/${LABEL}.harmonized.force_a2
	$PLINK --bfile data/${LABEL}.harmonized --recode vcf-iid bgz --real-ref-alleles --a2-allele data/${LABEL}.harmonized.force_a2 --out data/${LABEL}.harmonized.ref
	tabix -p vcf data/${LABEL}.harmonized.ref.vcf.gz
	echo "IID POP SUPERPOP SEX" > data/${LABEL}.harmonized.sample
	awk -v v=${LABEL} '{if($5 == 1) { sex="male" } else { if($5 == 2) { sex="female" } else { sex="NA" } } print $2" "v" "v" "sex}' data/${LABEL}.harmonized.fam >> data/${LABEL}.harmonized.sample

# Description: Generate the Hail VDS from VCF file and a sample file containing population and sex information
# Requires: $HAIL, Java (version under which Hail was compiled)
# Commandline Requirements:
# Directories Required: data 
# Input: data/${LABEL}.harmonized.vcf.bgz, data/${LABEL}.harmonized.sample
# Output: data/${LABEL}.harmonized.vds.log, data/${LABEL}.harmonized.vds/
# Example: ./QCpipeline -s $SOURCE -q load
# Notes: 
#    Monomorphic variants are automatically removed during import into Hail
# Hail?: Yes
elif [ "$QC_STEP" == "load" ]; then # directory preparations

	$HAIL -l data/${LABEL}.harmonized.ref.vds.log \
	importvcf --force-bgz data/${LABEL}.harmonized.ref.vcf.gz \
	splitmulti \
	deduplicate \
	annotatesamples table \
	--root sa.pheno \
	-e IID \
	-i data/${LABEL}.harmonized.sample \
	-t "IID: String, POP: String, SUPERPOP: String, SEX: String" \
	--missing "NA" \
	--delimiter " " \
	write \
	-o data/${LABEL}.harmonized.ref.vds \
	count \
	-g

# Description: Generate filtered and filtered/pruned filesets for QC
# Requires: $HAIL, Plink, Java (version under which Hail was compiled)
# Commandline Requirements: 
# Directories Required: data
# Input: data/${LABEL}.harmonized.vds/, $REGIONS_EXCLUDE
# Output: data/${LABEL}.filter.log, data/${LABEL}.variantqc.tsv, data/${LABEL}.for_qc.vds, data/${LABEL}.for_qc.bed/bim/fam, data/${LABEL}.for_qc.prune.in, data/${LABEL}.for_qc.prune.out
# Example: ./QCpipeline -s $SOURCE -q filter
# Notes: 
# Hail?: Yes
elif [ "$QC_STEP" == "filter" ]; then # data preparations

	$HAIL -l data/${LABEL}.filter.log \
	read -i data/${LABEL}.harmonized.ref.vds \
	variantqc \
	exportvariants -c "ID = v, Chrom = v.contig, Pos = v.start, Ref = v.ref, Alt = v.alt, va.qc.*" \
	-o data/${LABEL}.variantqc.tsv \
	filtervariants expr -c 'v.altAllele.isSNP && ! v.altAllele.isComplex && ["A","C","G","T"].toSet.contains(v.altAllele.ref) && ["A","C","G","T"].toSet.contains(v.altAllele.alt) && va.qc.AF >= 0.01 && va.qc.callRate >= 0.98' --keep \
	filtervariants intervals -i $REGIONS_EXCLUDE --remove \
	write \
	-o data/${LABEL}.for_qc.vds \
	exportplink \
	-o data/${LABEL}.for_qc
	mv data/${LABEL}.for_qc.fam data/${LABEL}.for_qc.fam.orig
	awk '{print $2"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6}' data/${LABEL}.for_qc.fam.orig >  data/${LABEL}.for_qc.fam
	
	$PLINK --bfile data/${LABEL}.for_qc --indep-pairwise 1500 150 0.2 --out data/${LABEL}.for_qc

	$HAIL -l data/${LABEL}.filter.log \
	read -i data/${LABEL}.for_qc.vds \
	filtervariants list -i data/${LABEL}.for_qc.prune.in --keep \
	write \
	-o data/${LABEL}.for_qc.pruned.vds \
	exportplink \
	-o data/${LABEL}.for_qc.pruned
	awk '{print $2"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6}' data/${LABEL}.for_qc.pruned.fam.orig >  data/${LABEL}.for_qc.pruned.fam

# Description: Merge for_qc datasets
# Requires: $HAIL
# Commandline Requirements: -L X -L Y, where X and Y are LABEL values
# Directories Required: data, qc
# Input: data/${LABELS[0]}.for_qc.vds, data/${LABELS[1]}.for_qc.vds
# Output: 
# Example: ./QCpipeline -s $SOURCE -L X -L Y -q merge_for_qc
# Notes: 
#    should be used on all datasets where a merged qc is needed
#    currently only handles 2 datasets
# Hail?: Yes
elif [ "$QC_STEP" == "merge_for_qc" ]; then

	$HAIL -l data/${LABEL}.merge_for_qc.log \
	read data/${LABELS[0]}.for_qc.vds \
	exportsamples -c 's.id, s.id' \
	-o data/${LABELS[0]}.for_qc.samples.tsv

	cat data/${LABELS[0]}.for_qc.samples.tsv | awk -v tag=${LABELS[0]} '{print $0"___"tag}' > data/${LABELS[0]}.for_qc.samples.tagged.tsv

	$HAIL -l data/${LABEL}.merge_for_qc.log \
	read data/${LABELS[1]}.for_qc.vds \
	exportsamples -c 's.id, s.id' \
	-o data/${LABELS[1]}.for_qc.samples.tsv

	cat data/${LABELS[1]}.for_qc.samples.tsv | awk -v tag=${LABELS[1]} '{print $0"___"tag}' > data/${LABELS[1]}.for_qc.samples.tagged.tsv

	$HAIL -l data/${LABEL}.merge_for_qc.log \
	read data/${LABELS[0]}.for_qc.vds \
	renamesamples -i data/${LABELS[0]}.for_qc.samples.tagged.tsv \
	put -n ${LABELS[0]} \
	read -i data/${LABELS[1]}.for_qc.vds \
	renamesamples -i data/${LABELS[1]}.for_qc.samples.tagged.tsv \
	join --right ${LABELS[0]} \
	exportsamples -c 'IID = s.id' \
	-o data/${LABEL}.for_qc.samples.tsv \
	exportvariants -c "ID = v" \
	-o data/${LABEL}.for_qc.variants.tsv \
	write \
	-o data/${LABEL}.for_qc.vds \
	exportplink \
	-o data/${LABEL}.for_qc

	$PLINK --bfile data/${LABEL}.for_qc --indep-pairwise 1500 150 0.2 --out data/${LABEL}.for_qc

	$HAIL -l data/${LABEL}.merge_for_qc.log \
	read -i data/${LABEL}.for_qc.vds \
	filtervariants list -i data/${LABEL}.for_qc.prune.in --keep \
	exportsamples -c 'IID = s.id' \
	-o data/${LABEL}.for_qc.pruned.samples.tsv \
	exportvariants -c "ID = v" \
	-o data/${LABEL}.for_qc.pruned.variants.tsv \
	write \
	-o data/${LABEL}.for_qc.pruned.vds \
	exportplink \
	-o data/${LABEL}.for_qc.pruned

# Description: Calculate kinship to identify duplicates and any samples exhibiting abnormal (excessive) sharing
# Requires: $KING, R, $CALC_KINSHIP_SAMPLE_SHARING_R
# Commandline Requirements: 
# Directories Required: data, kinship
# Input: data/${LABEL}.for_qc.bed/bim/fam
# Output: kinship/${LABEL}.kinshipTMP.dat, kinship/${LABEL}.kinshipTMP.ped, kinship/${LABEL}.kinship.kin, kinship/${LABEL}.kinship.kin0, kinship/${LABEL}.kinship.kin0.related, kinship/${LABEL}.kinship.sharing_counts.txt
# Example: ./QCpipeline -s $SOURCE -q kinship
# Notes: 
#    King is preferred to Plink or Hail based IBD calcs due to robust algorithm handling of population stratification. This step should be followed by a visual inspection for duplicates or excessive sharing
# Hail?: No
elif [ "$QC_STEP" == "kinship" ]; then

	$KING -b data/${LABEL}.for_qc.pruned.bed --kinship --prefix qc/${LABEL}.kinship
	if [ -f qc/${LABEL}.kinship.kin0 ]; then
		(head -1 qc/${LABEL}.kinship.kin0; sed '1d' qc/${LABEL}.kinship.kin0 | awk '{if($8 >= 0.0884) print $0}' | sort -rn -k8,8) > qc/${LABEL}.kinship.kin0.related
	else
		head -1 qc/${LABEL}.kinship.kin > qc/${LABEL}.kinship.kin0
		cp qc/${LABEL}.kinship.kin0 qc/${LABEL}.kinship.kin0.related
	fi
	$R --vanilla --args qc/${LABEL}.kinship.kin0.related qc/${LABEL}.kinship.sharing_counts.txt < $CALC_KINSHIP_SAMPLE_SHARING_R



## Human interaction required here to decide on duplicate removal, including cross array duplicates



# Description: Calculate PCs combined with 1KG Phase 3 Purcell 5k data
# Requires: $HAIL, R, $PLOT_ANCESTRY_PCA_R
# Commandline Requirements: 
# Directories Required: data, ancestry
# Input: data/${LABEL}.harmonized.ref.vds, $KG_HAIL, $KG_V3_5K_AF
# Output: ancestry/${LABEL}.KG.pca.scores.log, ancestry/${LABEL}.KG.pca.samples.scores.tsv, ancestry/${LABEL}.KG.pca.variants.loadings.tsv, ancestry/.${LABEL}.KG.pca.samples.scores.tsv.crc, ancestry/${LABEL}.KG.pca.variants.loadings.tsv.crc ancestry/${LABEL}.KG.pca.samples.scores.plots.pdf
# Example: ./QCpipeline -s $SOURCE -q ancestry_pca
# Notes: 
#    To perform ancestry inference and clustering with 1KG data, we must combine on common variants with reference data (clustering does not work when only using PCA loadings and projecting)
# Hail?: Yes
elif [ "$QC_STEP" == "ancestry_pca" ]; then

	$HAIL -l qc/${LABEL}.KG.pca.scores.log \
	read $KG_HAIL \
	put -n KG \
	read -i data/${LABEL}.for_qc.vds \
	join --right KG \
	annotatevariants table $KG_V3_5K_AF \
	-e Variant \
	-c 'va.refPanelAF = table.refPanelAF' \
	--impute \
	pca -k 10 \
	--scores sa.pca.scores \
	--eigenvalues global.pca.evals \
	--loadings va.pca.loadings \
	exportsamples -c 'IID = sa.pheno.IID, POP = sa.pheno.POP, SUPERPOP = sa.pheno.SUPERPOP, SEX = sa.pheno.SEX, PC1 = sa.pca.scores.PC1, PC2 = sa.pca.scores.PC2, PC3 = sa.pca.scores.PC3, PC4 = sa.pca.scores.PC4, PC5 = sa.pca.scores.PC5, PC6 = sa.pca.scores.PC6, PC7 = sa.pca.scores.PC7, PC8 = sa.pca.scores.PC8, PC9 = sa.pca.scores.PC9, PC10 = sa.pca.scores.PC10' \
	-o qc/${LABEL}.KG.pca.samples.scores.tsv \
	exportvariants -c 'ID = v, PC1 = va.pca.loadings.PC1, PC2 = va.pca.loadings.PC2, PC3 = va.pca.loadings.PC3, PC4 = va.pca.loadings.PC4, PC5 = va.pca.loadings.PC5, PC6 = va.pca.loadings.PC6, PC7 = va.pca.loadings.PC7, PC8 = va.pca.loadings.PC8, PC9 = va.pca.loadings.PC9, PC10 = va.pca.loadings.PC10' \
	-o qc/${LABEL}.KG.pca.variants.loadings.tsv \

	$R --vanilla --args qc/${LABEL}.KG.pca.samples.scores.tsv qc/${LABEL}.KG.pca.samples.scores.plots.pdf < $PLOT_ANCESTRY_PCA_R

# Description: Cluster with 1KG samples using Gaussian Mixture Modeling and infer ancestry
# Requires: $HAIL, R, $PLOT_ANCESTRY_CLUSTER_R, $LABEL, $PHENO_ID_COL, $PHENO_RACE_COL
# Commandline Requirements: 
# Directories Required: ancestry
# Input: ancestry/${LABEL}.KG.pca.samples.scores.tsv, $PHENO
# Output: ancestry/${LABEL}.KG.fet.1, ancestry/${LABEL}.KG.temp.clu.1, ancestry/${LABEL}.KG.clu.1, ancestry/${LABEL}.KG.klg.1, ancestry/${LABEL}.KG.cluster_plots.pdf, ancestry/${LABEL}.KG.cluster_xtabs, ancestry/${LABEL}.KG.cluster_plots.centers.pdf, ancestry/${LABEL}.KG.clusters_assigned, ancestry/${LABEL}.ancestry
# Example: ./QCpipeline -s $SOURCE -q ancestry_cluster
# Notes: 
#    ${LABEL}.ancestry contains the final inferred ancestry for each sample, including OUTLIERS
#    This file may be updated after reconciling with other arrays
# Hail?: No
elif [ "$QC_STEP" == "ancestry_cluster" ]; then

	echo 10 > qc/${LABEL}.KG.fet.1
	sed '1d' qc/${LABEL}.KG.pca.samples.scores.tsv | cut -f5- | sed 's/\t/ /g' >> qc/${LABEL}.KG.fet.1
	$KLUSTAKWIK qc/$LABEL.KG 1 -UseFeatures 1110000000 -UseDistributional 0
	$R --vanilla --args qc/${LABEL}.KG.pca.samples.scores.tsv qc/${LABEL}.KG.clu.1 $PHENO $LABEL $PHENO_ID_COL $PHENO_RACE_COL \
		qc/${LABEL}.KG.cluster_plots.pdf qc/${LABEL}.KG.cluster_xtabs qc/${LABEL}.KG.cluster_plots.centers.pdf \
		qc/${LABEL}.KG.clusters_assigned qc/${LABEL}.ancestry qc/${LABEL}.cluster_plots.pdf < $PLOT_ANCESTRY_CLUSTER_R

## Ancestry Cluster Merge
elif [ "$QC_STEP" == "ancestry_cluster_merge" ]; then # merge ancestry information hierarchically

	if [ ! -f qc/chip.hierarchy ]; then
		echo "file chip.hierarchy missing!"
		exit
	fi

	$R --vanilla --args qc/chip.hierarchy < $ANCESTRY_CLUSTER_MERGE_R

	if [ -f qc/ancestry.OUTLIERS ]; then
		cp qc/ancestry.OUTLIERS samples_flagged/ancestry_outliers.remove
	else
		touch samples_flagged/ancestry_outliers.remove
	fi

# Description: Calculate PCs for all non-outlier samples combined (to be used for adjustment during sample outlier removal)
# Requires: $HAIL
# Commandline Requirements: 
# Directories Required: data, ancestry
# Input: data/${LABEL}.for_qc.pruned.vds, ancestry/${LABEL}.ancestry
# Output: ancestry/${LABEL}.CLUSTERED.pca.log, ancestry/${LABEL}.CLUSTERED.pca.samples.scores.tsv, ancestry/${LABEL}.CLUSTERED.pca.variants.loadings.tsv, ancestry/.${LABEL}.CLUSTERED.pca.samples.scores.tsv.crc, ancestry/${LABEL}.CLUSTERED.pca.variants.loadings.tsv.crc
# Example: ./QCpipeline -s $SOURCE -q ancestry_cluster_pca
# Notes: 
# Hail?: Yes
elif [ "$QC_STEP" == "ancestry_cluster_pca" ]; then

	$HAIL -l qc/${LABEL}.CLUSTERED.pca.log \
	read data/${LABEL}.for_qc.pruned.vds \
	annotatesamples table \
	-i qc/${LABEL}.ancestry \
	--no-header \
	-e _0 \
	--code "sa.pheno.IID = table._0, sa.pheno.POP = table._1, sa.pheno.SUPERPOP = table._1" \
	filtersamples expr -c 'sa.pheno.SUPERPOP != "OUTLIERS"' --keep \
	pca -k 10 \
	--scores sa.pca.scores \
	--eigenvalues global.pca.evals \
	--loadings va.pca.loadings \
	exportsamples -c 'IID = sa.pheno.IID, POP = sa.pheno.POP, SUPERPOP = sa.pheno.SUPERPOP, SEX = sa.pheno.SEX, PC1 = sa.pca.scores.PC1, PC2 = sa.pca.scores.PC2, PC3 = sa.pca.scores.PC3, PC4 = sa.pca.scores.PC4, PC5 = sa.pca.scores.PC5, PC6 = sa.pca.scores.PC6, PC7 = sa.pca.scores.PC7, PC8 = sa.pca.scores.PC8, PC9 = sa.pca.scores.PC9, PC10 = sa.pca.scores.PC10' \
	-o qc/${LABEL}.CLUSTERED.pca.samples.scores.tsv \
	exportvariants -c 'ID = v, PC1 = va.pca.loadings.PC1, PC2 = va.pca.loadings.PC2, PC3 = va.pca.loadings.PC3, PC4 = va.pca.loadings.PC4, PC5 = va.pca.loadings.PC5, PC6 = va.pca.loadings.PC6, PC7 = va.pca.loadings.PC7, PC8 = va.pca.loadings.PC8, PC9 = va.pca.loadings.PC9, PC10 = va.pca.loadings.PC10' \
	-o qc/${LABEL}.CLUSTERED.pca.variants.loadings.tsv

#elif [ "$QC_STEP" == "plinkseq" ]; then # prepare Plink/Seq project
#	reuse PSEQ
#	reuse Tabix
#
#	# run Plink/Seq i-stats
#	cd data
#	if [ ! -f ../data/${LABEL}.qc.bi.chr1-22.bed ]; then
#		$PLINK --bfile ${LABEL}.qc.bi --chr 1-22 --make-bed --out ${LABEL}.qc.bi.chr1-22
#	fi
#	if [ ! -f ../data/${LABEL}.qc.bi.chr1-22.vcf.gz ]; then
#		$PLINK --bfile ${LABEL}.qc.bi.chr1-22 --recode vcf-iid bgz --out ${LABEL}.qc.bi.chr1-22.temp
#		tabix -p vcf ${LABEL}.qc.bi.chr1-22.temp.vcf.gz
#		tabix -H ${LABEL}.qc.bi.chr1-22.temp.vcf.gz > ${LABEL}.qc.bi.chr1-22.temp.header
#		sed -i 's/VCFv4\.2/VCFv4\.0/g' ${LABEL}.qc.bi.chr1-22.temp.header
#		tabix -r ${LABEL}.qc.bi.chr1-22.temp.header ${LABEL}.qc.bi.chr1-22.temp.vcf.gz > ${LABEL}.qc.bi.chr1-22.vcf.gz
#		tabix -p vcf ${LABEL}.qc.bi.chr1-22.vcf.gz
#		rm ${LABEL}.qc.bi.chr1-22.temp*
#	fi
#	cd ../istats
#	if [ ! -f ${LABEL}.pseq ]; then
#		pseq ${LABEL} new-project
#		pseq ${LABEL} index-vcf --vcf ../data/${LABEL}.qc.bi.chr1-22.vcf.gz --id ${LABEL}
#	fi
#	cd ..
#
#elif [ "$QC_STEP" == "sexcheck" ]; then # calculate sample stats for each 1kg population and all together
#	cd istats
#	$PLINK --bfile ../data/${LABEL}.base --check-sex --out ${LABEL}.sexcheck
#	sed '1d' ${LABEL}.sexcheck.sexcheck | awk '{if($5 == "PROBLEM") print $2}' > ../samples_flagged/${LABEL}.failed_sexcheck.remove
#	cd ..
#
#elif [ "$QC_STEP" == "istats" ]; then # calculate sample stats for each 1kg population and all together
#	reuse R-3.1
#	reuse PSEQ
#
#	cd istats
#	if [ ! -s ${LABEL}.istats ]; then
#		pseq ${LABEL} i-stats > ${LABEL}.istats
#	fi
#    
#	$PLINK --bfile ../data/${LABEL}.qc.bi.chr1-22 --het --out ${LABEL}.het
#	$PLINK --bfile ../data/${LABEL}.qc.bi.chr1-22 --het --maf 0.03 --out ${LABEL}.het.high
#	$PLINK --bfile ../data/${LABEL}.qc.bi.chr1-22 --het --max-maf 0.03 --out ${LABEL}.het.low
#    
#	# compile all sample stats into single file
#	$R --vanilla --args ${LABEL}.istats ${LABEL}.het.het ${LABEL}.het.high.het ${LABEL}.het.low.het ${LABEL}.istats.all < $ISTATS_COMPILE_R
#
#	# calculate PC adjusted istats
#	$R --vanilla --args ${LABEL}.istats.all ../ancestry_pca_clustered/${LABEL}_1kg.ref.CLUSTERED.pca.evec ${LABEL}.CLUSTERED.istats.all.adj < $CALC_ISTATS_ADJ_R
#    
#	# calculate PCs for PC adjusted istats metrics
#	$R --vanilla --args ${LABEL}.CLUSTERED.istats.all.adj ${LABEL}.CLUSTERED.istats.all.adj.corr.pdf ${LABEL}.CLUSTERED.istats.all.adj.pca.loadings ${LABEL}.CLUSTERED.istats.all.adj.pcs.pdf ${LABEL}.CLUSTERED.istats.all.adj.pcs < $ISTATS_ADJ_PCA_R
#	cd ..

# Description: Calculate sexcheck and sample by variant statistics for all samples
# Requires: $HAIL
# Commandline Requirements: 
# Directories Required: data, istats
# Input: 
# Output: 
# Example: ./QCpipeline -s $SOURCE -q sample_stats
# Notes: 
# Hail?: Yes
elif [ "$QC_STEP" == "sample_qc" ]; then

	$HAIL -l qc/${LABEL}.sampleqc.log \
	read data/${LABEL}.for_qc.vds \
	annotatesamples table \
	-i qc/${LABEL}.ancestry \
	--no-header \
	-e _0 \
	--code "sa.pheno.IID = table._0, sa.pheno.POP = table._1, sa.pheno.SUPERPOP = table._1" \
	filtersamples expr -c 'sa.pheno.SUPERPOP != "OUTLIERS"' --keep \
	imputesex \
	annotatesamples expr -c 'sa.sexcheck = if((sa.pheno.SEX == "female" && ! isMissing(sa.imputesex.isFemale) && sa.imputesex.isFemale) || (sa.pheno.SEX == "male" && ! isMissing(sa.imputesex.isFemale) && ! sa.imputesex.isFemale)) "OK" else "PROBLEM"' \
	sampleqc \
	variantqc \
	annotatesamples expr -c "sa.qc.nHetLow = gs.filter(v => va.qc.AF < 0.03).filter(g => g.isHet).count(), sa.qc.nHetHigh = gs.filter(v => va.qc.AF >= 0.03).filter(g => g.isHet).count(), sa.qc.nCalledLow = gs.filter(v => va.qc.AF < 0.03).filter(g => g.isCalled).count(), sa.qc.nCalledHigh = gs.filter(v => va.qc.AF >= 0.03).filter(g => g.isCalled).count()" \
	exportsamples -c "IID = sa.pheno.IID, POP = sa.pheno.POP, SUPERPOP = sa.pheno.SUPERPOP, SEX = sa.pheno.SEX, sa.imputesex.*, sexCheck = sa.sexcheck" \
	-o qc/${LABEL}.sampleqc.sexcheck.tsv \
	exportsamples -c "IID = sa.pheno.IID, nNonRef = sa.qc.nNonRef, nHet = sa.qc.nHet, nCalled = sa.qc.nCalled, callRate = sa.qc.callRate, nSingleton = sa.qc.nSingleton, rTiTv = sa.qc.rTiTv, het = sa.qc.nHet / sa.qc.nCalled, hetLow = sa.qc.nHetLow / sa.qc.nCalledLow, hetHigh = sa.qc.nHetHigh / sa.qc.nCalledHigh, nHomVar = sa.qc.nHomVar, rHetHomVar = sa.qc.rHetHomVar" \
	-o qc/${LABEL}.sampleqc.stats.tsv \
	filtersamples expr -c 'sa.sexcheck == "PROBLEM"' --keep \
	exportsamples -c "IID = sa.pheno.IID, POP = sa.pheno.POP, SUPERPOP = sa.pheno.SUPERPOP, SEX = sa.pheno.SEX, sa.imputesex.*, sexCheck = sa.sexcheck" \
	-o qc/${LABEL}.sampleqc.sexcheck.problems.tsv \

	$R --vanilla --args qc/${LABEL}.sampleqc.stats.tsv qc/${LABEL}.CLUSTERED.pca.samples.scores.tsv qc/${LABEL}.sampleqc.stats.adj.tsv < $CALC_ISTATS_ADJ_R
    
	# calculate PCs for PC adjusted istats metrics
	$R --vanilla --args qc/${LABEL}.sampleqc.stats.adj.tsv qc/${LABEL}.sampleqc.stats.adj.corr.pdf qc/${LABEL}.sampleqc.stats.adj.pca.loadings qc/${LABEL}.sampleqc.stats.adj.pcs.pdf qc/${LABEL}.sampleqc.stats.adj.pcs < $ISTATS_ADJ_PCA_R

elif [ "$QC_STEP" == "sample_qc_cluster_pca" ]; then # cluster PCs of adjusted istats metrics
	reuse R-3.1

	# run klustakwik
	n=`head -1 qc/${LABEL}.sampleqc.stats.adj.pcs | wc | awk '{print $2-1}'`
	echo $n > qc/${LABEL}.sampleqc.stats.adj.fet.1
	sed '1d' qc/${LABEL}.sampleqc.stats.adj.pcs | cut -f2- | sed 's/\t/ /g' >> qc/${LABEL}.sampleqc.stats.adj.fet.1
	features=1
	for i in `seq 2 $n`; do
		features=${features}1
	done
	echo -e "${LABEL}\t${features}"
	$KLUSTAKWIK qc/${LABEL}.sampleqc.stats.adj 1 -UseFeatures $features -UseDistributional 0

	$R --vanilla --args qc/${LABEL}.sampleqc.stats.adj.pcs qc/${LABEL}.sampleqc.stats.adj.clu.1 qc/${LABEL}.sampleqc.stats.adj.pcs.outliers qc/${LABEL}.sampleqc.stats.adj.pcs.clusters.pdf qc/${LABEL}.sampleqc.stats.adj.pcs.clusters_xtab $LABEL < $ISTATS_PCS_GMM_CLUSTER_PLOT_R

	$R --vanilla --args qc/${LABEL}.sampleqc.stats.adj.tsv qc/${LABEL}.sampleqc.stats.adj.pcs.outliers qc/${LABEL}.sampleqc.stats.adj.stripchart.pdf < $ISTATS_PCS_GMM_PLOT_METRICS_R

elif [ "$QC_STEP" == "sample_qc_cluster_individual" ]; then # cluster adjusted istats metrics and list all outliers
	reuse R-3.1

	# run klustakwik
	n=`head -1 qc/${LABEL}.sampleqc.stats.adj.tsv | wc | awk '{print $2-1}'`
	for feature in `seq 2 $((n+1))`; do
		id=`head -1 qc/${LABEL}.sampleqc.stats.adj.tsv | awk -v c=$feature '{print $c}'`
		echo $n > qc/${LABEL}.sampleqc.stats.adj.${id}.fet.1
		sed '1d' qc/${LABEL}.sampleqc.stats.adj.tsv | cut -f2- | sed 's/\t/ /g' >> qc/${LABEL}.sampleqc.stats.adj.${id}.fet.1
		include=''
		for i in `seq 2 $((n+1))`; do
			if [ $feature -eq $i ]; then
				include=${include}1
			else
				include=${include}0
			fi
		done
		echo -e "${LABEL}\t${include}"
		$KLUSTAKWIK qc/${LABEL}.sampleqc.stats.adj.${id} 1 -UseFeatures $include -UseDistributional 0 > qc/${LABEL}.sampleqc.stats.adj.${id}.fet.1.klustakwik.log
	done
    
	$R --vanilla --args \
		"qc/${LABEL}.sampleqc.stats.adj.*.clu.1" \
		qc/${LABEL}.sampleqc.stats.tsv \
		qc/${LABEL}.sampleqc.stats.adj.tsv \
		qc/${LABEL}.sampleqc.stats.adj.pcs.outliers \
		qc/${LABEL}.sampleqc.stats.adj.individual.boxplot.pdf \
		qc/${LABEL}.sampleqc.stats.adj.individual.discreteness \
		qc/${LABEL}.sampleqc.stats.adj.individual.outliers.table \
		samples_flagged/${LABEL}.sampleqc.stats.outliers.remove \
		qc/${LABEL}.sampleqc.stats.adj.individual.stripchart.pdf \
		qc/${LABEL}.ancestry \
		< $ISTATS_ADJ_GMM_PLOT_METRICS_R





elif [ "$QC_STEP" == "make_master_remove_lists" ]; then
	cd samples_flagged
	(cat ${LABEL}.CLUSTERED.istats.outliers.remove; cat ${LABEL}.duplicates.remove; cat ${LABEL}.excessive_sharing.remove; cat ancestry_outliers.remove; cat ${LABEL}.failed_sexcheck.remove) | sort -u > ${LABEL}.remove
	awk '{print $1" "$1}' ${LABEL}.remove >  ${LABEL}.remove.plink
	cd ..

elif [ "$QC_STEP" == "pedigree" ]; then
	if [ "$ANCESTRY" == "NA" ]; then
		echo "population (-p) must be set"
		exit
	fi
	reuse R-3.1
	reuse Python-2.7
	cd data_post_qc

	### use PRIMUS Maximum unrelated set identification to generate unrelated samples list for calculating variant stats with founders only
	$PLINK --bfile ../data/${LABEL}.qc.bi.chr1-22 --keep ../ancestry_cluster/ancestry.${ANCESTRY}.plink --remove ../samples_flagged/${LABEL}.remove.plink --make-bed --out ${LABEL}.qc.bi.chr1-22.${ANCESTRY}
	$PLINK --bfile ${LABEL}.qc.bi.chr1-22.${ANCESTRY} --geno 0.02 --maf 0.01 --exclude $REGIONS_EXCLUDE --indep-pairwise 1500 150 0.2 --make-bed --out ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune
	$PLINK --bfile ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune --genome --out ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd
	(head -1 ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome; sed '1d' ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome | awk '{if(($2 != $4 && $10 >= 0.25) || (($2 != $4 && $10 >= 0.7) || ($2 == $4 && $10 <= 0.7 && $10 >= 0.25))) print $0}' | sort -nr -k10,10) > ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related
	(head -1 ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome; sed '1d' ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome | awk '{if($2 == $4 && $10 < 0.25) print $0}' | sort -nr -k10,10) > ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.unrelated
	$KING -b ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.bed --kinship --prefix ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.king
	$KING -b ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.bed --unrelated --degree 2 --prefix ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.king.unrelated
	(head -1 ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.king.kin0; sed '1d' ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.king.kin0 | awk '{if($8 >= 0.0884) print $0}') > ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.king.kin0.related
	$R --vanilla --args ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.king.kin0.related ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related.robust < $COMPARE_PLINK_KING_R
	$PRIMUS -p ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related.robust -d 2 --max_gens 3
	python $COMPILE_PEDIGREE_PY ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related.robust_PRIMUS/${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related.robust_unrelated_samples.txt "${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related.robust_PRIMUS/*/" ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related.robust_PRIMUS/${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related.robust_networkXXXXX/${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related.robust_networkXXXXX_1.fam ${LABEL}.${ANCESTRY}.pedigree
	$R --vanilla --args ${LABEL}.${ANCESTRY}.pedigree ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related.robust_PRIMUS/${LABEL}.qc.bi.chr1-22.${ANCESTRY}.prune.ibd.genome.problems.related.robust_unrelated_samples.txt ${LABEL}.qc.bi.chr1-22.${ANCESTRY}.fam ${LABEL}.${ANCESTRY}.unrel ${LABEL}.${ANCESTRY}.unrel.plink < $GENERATE_UNREL_FILE_R
	cd ..

elif [ "$QC_STEP" == "pedigree_merged" ]; then
	reuse R-3.1
	reuse Python-2.7
	cd data_post_qc_merged

	### use PRIMUS Maximum unrelated set identification to generate unrelated samples list for calculating variant stats with founders only
	echo "../data_impute/BIOME_AFFY.impute.harmonized.chr2.bed ../data_impute/BIOME_AFFY.impute.harmonized.chr2.bim ../data_impute/BIOME_AFFY.impute.harmonized.chr2.fam" > BIOME_AFFY.merge_list.txt
	for i in {3..22}; do echo "../data_impute/BIOME_AFFY.impute.harmonized.chr${i}.bed ../data_impute/BIOME_AFFY.impute.harmonized.chr${i}.bim ../data_impute/BIOME_AFFY.impute.harmonized.chr${i}.fam" >> BIOME_AFFY.merge_list.txt; done
	echo "../data_impute/BIOME_ILL.impute.harmonized.chr2.bed ../data_impute/BIOME_ILL.impute.harmonized.chr2.bim ../data_impute/BIOME_ILL.impute.harmonized.chr2.fam" > BIOME_ILL.merge_list.txt
	for i in {3..22}; do echo "../data_impute/BIOME_ILL.impute.harmonized.chr${i}.bed ../data_impute/BIOME_ILL.impute.harmonized.chr${i}.bim ../data_impute/BIOME_ILL.impute.harmonized.chr${i}.fam" >> BIOME_ILL.merge_list.txt; done
	echo "../data_impute/BIOME_EX.impute.harmonized.chr2.bed ../data_impute/BIOME_EX.impute.harmonized.chr2.bim ../data_impute/BIOME_EX.impute.harmonized.chr2.fam" > BIOME_EX.merge_list.txt
	for i in {3..22}; do echo "../data_impute/BIOME_EX.impute.harmonized.chr${i}.bed ../data_impute/BIOME_EX.impute.harmonized.chr${i}.bim ../data_impute/BIOME_EX.impute.harmonized.chr${i}.fam" >> BIOME_EX.merge_list.txt; done
	$PLINK --bfile ../data_impute/BIOME_AFFY.impute.harmonized.chr1 --merge-list BIOME_AFFY.merge_list.txt --make-bed --keep-allele-order --out BIOME_AFFY.impute.harmonized
	$PLINK --bfile BIOME_AFFY.impute.harmonized --exclude ../data/BIOME_AFFY.multiallelic --make-bed --keep-allele-order --out BIOME_AFFY.impute.harmonized.bi
	mv BIOME_AFFY.impute.harmonized.bi.fam BIOME_AFFY.impute.harmonized.bi.fam.orig
	awk '{$1=$2; print $0}' BIOME_AFFY.impute.harmonized.bi.fam.orig > BIOME_AFFY.impute.harmonized.bi.fam
	$PLINK --bfile ../data_impute/BIOME_ILL.impute.harmonized.chr1 --merge-list BIOME_ILL.merge_list.txt --make-bed --keep-allele-order --out BIOME_ILL.impute.harmonized
	$PLINK --bfile BIOME_ILL.impute.harmonized --exclude ../data/BIOME_ILL.multiallelic --make-bed --keep-allele-order --out BIOME_ILL.impute.harmonized.bi
	mv BIOME_ILL.impute.harmonized.bi.fam BIOME_ILL.impute.harmonized.bi.fam.orig
	awk '{$1=$2; print $0}' BIOME_ILL.impute.harmonized.bi.fam.orig > BIOME_ILL.impute.harmonized.bi.fam
	$PLINK --bfile ../data_impute/BIOME_EX.impute.harmonized.chr1 --merge-list BIOME_EX.merge_list.txt --make-bed --keep-allele-order --out BIOME_EX.impute.harmonized
	$PLINK --bfile BIOME_EX.impute.harmonized --exclude ../data/BIOME_EX.multiallelic --make-bed --keep-allele-order --out BIOME_EX.impute.harmonized.bi
	mv BIOME_EX.impute.harmonized.bi.fam BIOME_EX.impute.harmonized.bi.fam.orig
	awk '{$1=$2; print $0}' BIOME_EX.impute.harmonized.bi.fam.orig > BIOME_EX.impute.harmonized.bi.fam
	echo "BIOME_ILL.impute.harmonized.bi.bed BIOME_ILL.impute.harmonized.bi.bim BIOME_ILL.impute.harmonized.bi.fam" > merge_list.txt
	echo "BIOME_AFFY.impute.harmonized.bi.bed BIOME_AFFY.impute.harmonized.bi.bim BIOME_AFFY.impute.harmonized.bi.fam" >> merge_list.txt
	$PLINK --bfile BIOME_EX.impute.harmonized.bi --merge-list merge_list.txt --merge-equal-pos --make-bed --out BIOME_MERGED.impute.harmonized
	awk '{if(x[$1":"$4]) {x_count[$1":"$4]++; print $2; if(x_count[$1":"$4] == 1) {print x[$1":"$4]}} x[$1":"$4] = $2}' BIOME_MERGED.impute.harmonized.bim > BIOME_MERGED.impute.harmonized.multiallelic
	$PLINK --bfile BIOME_MERGED.impute.harmonized --exclude BIOME_MERGED.impute.harmonized.multiallelic --make-bed --keep-allele-order --out BIOME_MERGED.impute.harmonized.bi
    
	$PLINK --bfile BIOME_MERGED.impute.harmonized.bi --keep ../ancestry_cluster/ancestry.AFR.plink --hardy --out BIOME_MERGED.AFR.hwe
	$PLINK --bfile BIOME_MERGED.impute.harmonized.bi --keep ../ancestry_cluster/ancestry.EUR.plink --hardy --out BIOME_MERGED.EUR.hwe
	sed '1d' BIOME_MERGED.AFR.hwe.hwe | awk '{if($9 < 1e-6) print $2}' > BIOME_MERGED.AFR.hwe.failed.variants
	sed '1d' BIOME_MERGED.EUR.hwe.hwe | awk '{if($9 < 1e-6) print $2}' > BIOME_MERGED.EUR.hwe.failed.variants
	cat BIOME_MERGED.EUR.hwe.failed.variants BIOME_MERGED.AFR.hwe.failed.variants | sort -u > BIOME_MERGED.EUR_AFR.hwe.failed.variants
	$PLINK --bfile BIOME_MERGED.impute.harmonized.bi --exclude BIOME_MERGED.EUR_AFR.hwe.failed.variants --geno 0.02 --maf 0.01 --make-bed --keep-allele-order --out BIOME_MERGED.ibd
    
	$PLINK --bfile BIOME_MERGED.ibd --exclude $REGIONS_EXCLUDE --indep-pairwise 1500 150 0.2 --make-bed --out BIOME_MERGED.ibd.prune
	$PLINK --bfile BIOME_MERGED.ibd.prune --genome --out BIOME_MERGED.ibd.prune.genome
	(head -1 BIOME_MERGED.ibd.prune.genome.genome; sed '1d' BIOME_MERGED.ibd.prune.genome.genome | awk '{if(($2 != $4 && $10 >= 0.25) || (($2 != $4 && $10 >= 0.7) || ($2 == $4 && $10 <= 0.7 && $10 >= 0.25))) print $0}' | sort -nr -k10,10) > BIOME_MERGED.ibd.prune.genome.genome.problems.related
	(head -1 BIOME_MERGED.ibd.prune.genome.genome; sed '1d' BIOME_MERGED.ibd.prune.genome.genome | awk '{if($2 == $4 && $10 < 0.25) print $0}' | sort -nr -k10,10) > BIOME_MERGED.ibd.prune.genome.genome.problems.unrelated
	$KING -b BIOME_MERGED.ibd.prune.bed --kinship --prefix BIOME_MERGED.ibd.prune.king > BIOME_MERGED.ibd.prune.king.kinship.log
	$KING -b BIOME_MERGED.ibd.prune.bed --unrelated --degree 2 --prefix BIOME_MERGED.ibd.prune.king.unrelated > BIOME_MERGED.ibd.prune.king.unrelated.log
	(head -1 BIOME_MERGED.ibd.prune.king.kin0; sed '1d' BIOME_MERGED.ibd.prune.king.kin0 | awk '{if($8 >= 0.0884) print $0}') > BIOME_MERGED.ibd.prune.king.kin0.related
	$R --vanilla --args BIOME_MERGED.ibd.prune.king.kin0.related BIOME_MERGED.ibd.prune.king.kin0.related.sharing_counts.txt < $KINSHIP_CALC_SAMPLE_SHARING_R
	$R --vanilla --args BIOME_MERGED.ibd.prune.genome.genome.problems.related BIOME_MERGED.ibd.prune.king.kin0.related BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust < $COMPARE_PLINK_KING_R
	(sed '1d' BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust | awk '{print $1}'; sed '1d' BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust | awk '{print $3}') | sort -u > BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust.ids
	(while read line; do grep -w "$line" $PHENO | awk -F'\t' '{if($14 == 1) { $14 = 3 } else { if($14 == 0) { $14 = 2 } else { $14 = 1 } } print $1,$1,$14}'; done < BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust.ids) > BIOME_MERGED.primus.weights
	$PRIMUS -p BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust -d 2 --max_gens 3 --no_PR --high_qtrait BIOME_MERGED.primus.weights
	(cat BIOME_MERGED.ibd.prune.king.unrelatedunrelated.txt; sed '1d' BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust_PRIMUS/BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust_maximum_independent_set) | sort -u > BIOME_MERGED.unrel.plink
	awk '{print $2}' BIOME_MERGED.unrel.plink > BIOME_MERGED.unrel
	cd ..
	
elif [ "$QC_STEP" == "hwe" ]; then # 
	if [ "$ANCESTRY" == "NA" ]; then
		echo "population (-p) must be set for QC step 4"
		exit
	fi
	if [ ! -d hwe ]; then
		mkdir hwe
	fi
	cd hwe
	if [ "$REPLACE" = true ]; then
		rm ${LABEL}*[!qlog]
	fi

	# calculate HWE P-value within populations using maximum unrelated sets
	$PLINK --bfile ../data/${LABEL}.base --keep ../data_post_qc/${LABEL}.${ANCESTRY}.unrel.plink --hardy --out ${LABEL}.${ANCESTRY}.hwe
	sed '1d' ${LABEL}.${ANCESTRY}.hwe.hwe | awk '{if($9 < 1e-6) print $2}' > ${LABEL}.${ANCESTRY}.hwe.failed.variants
	cd ..

elif [ "$QC_STEP" == "clean_analysis" ]; then # 
	if [ "$ANCESTRY" == "NA" ]; then
		echo "population (-p) must be set for QC step 4"
		exit
	fi

	cd hwe
	if [ ! -f ${LABEL}.EUR_AFR.hwe.failed.variants ]; then
		cat ${LABEL}.EUR.hwe.failed.variants ${LABEL}.AFR.hwe.failed.variants | sort -u > ${LABEL}.EUR_AFR.hwe.failed.variants
	fi

	# generate analysis ready VCFs
	cd ../data_clean
	cat ../data/${LABEL}.variants.unplaced ../data/${LABEL}.variants.monomorphic ../data/${LABEL}.variants.duplicate.remove ../data/${LABEL}.missing.lmiss.high ../hwe/${LABEL}.EUR_AFR.hwe.failed.variants | sort -u > ${LABEL}.variants.exclude
	$PLINK --bfile ../data/${LABEL}.base --keep ../ancestry_cluster/ancestry.${ANCESTRY}.plink --remove ../samples_flagged/${LABEL}.remove.plink --exclude ${LABEL}.variants.exclude --make-bed --out ${LABEL}.${ANCESTRY}.clean
	$PLINK --bfile ${LABEL}.${ANCESTRY}.clean --recode vcf-iid bgz --out ${LABEL}.${ANCESTRY}.clean
	$PLINK --bfile ../data/${LABEL}.base --keep ../ancestry_cluster/ancestry.${ANCESTRY}.plink --remove ../samples_flagged/${LABEL}.remove.plink --exclude ${LABEL}.variants.exclude --chr 1-22 --make-bed --out ${LABEL}.${ANCESTRY}.clean.chr1-22
	$PLINK --bfile ${LABEL}.${ANCESTRY}.clean.chr1-22 --recode vcf-iid bgz --out ${LABEL}.${ANCESTRY}.clean.chr1-22
	cd ..

elif [ "$QC_STEP" == "clean_impute" ]; then # 
	cd hwe
	if [ ! -f ${LABEL}.EUR_AFR.hwe.failed.variants ]; then
		cat ${LABEL}.EUR.hwe.failed.variants ${LABEL}.AFR.hwe.failed.variants | sort -u > ${LABEL}.EUR_AFR.hwe.failed.variants
	fi
    
	# generate impute ready Plink files
	cd ../data_impute
	$PLINK --bfile ../data/${LABEL}.base --geno 0.02 --exclude ../hwe/${LABEL}.EUR_AFR.hwe.failed.variants --make-bed --out ${LABEL}.impute
	cd ..
    
elif [ "$QC_STEP" == "phase" ]; then # 
	if [ "$CHROMOSOME" == "NA" ]; then
		echo "population (-p) must be set for QC step 4"
		exit
	fi
	reuse Java-1.8
	reuse Tabix
	cd data_impute
    
	if [ ! -f ${LABEL}.impute.chr${CHROMOSOME}.vcf.gz ]; then
		#$PLINK --bfile ${LABEL}.impute --chr $CHROMOSOME --make-bed --out ${LABEL}.impute.chr${CHROMOSOME}
		$PLINK --bfile ${LABEL}.impute --chr $CHROMOSOME --recode vcf-iid bgz --out ${LABEL}.impute.chr${CHROMOSOME}
		tabix -p vcf ${LABEL}.impute.chr${CHROMOSOME}.vcf.gz
	fi
	
	#$EAGLE \
	#--vcf ${LABEL}.impute.chr${CHROMOSOME}.vcf.gz \
	#--geneticMapFile $EAGLE_GENETIC_MAP \
	#--outPrefix ${LABEL}.impute.phased.chr${CHROMOSOME} \
	#--numThreads 32
	#tabix -p vcf ${LABEL}.impute.phased.chr${CHROMOSOME}.vcf.gz
    
	$GENOTYPE_HARMONIZER \
	--input ${LABEL}.impute.chr${CHROMOSOME} \
	--inputType VCF \
	--output ${LABEL}.impute.harmonized.chr${CHROMOSOME} \
	--outputType PLINK_BED \
	--ref $KG_VCF_BASE \
	--refType VCF \
	--keep \
	--update-id \
	--variants 1000 \
	--mafAlign 0.1 \
	--debug
    
    $PLINK --bfile ${LABEL}.impute.harmonized.chr${CHROMOSOME} --recode vcf-iid bgz --keep-allele-order --out ${LABEL}.impute.harmonized.chr${CHROMOSOME}
	tabix -p vcf ${LABEL}.impute.harmonized.chr${CHROMOSOME}.vcf.gz
	cd ..

elif [ "$QC_STEP" == "prepare_vcfs" ]; then
	reuse Tabix

	cd data_clean
	# modify FILTER column to make compatible with EPACTS
	if [ -f ${LABEL}.${ANCESTRY}.clean.vcf.gz ]; then
		(zcat ${LABEL}.${ANCESTRY}.clean.vcf.gz | grep "^#"; zcat ${LABEL}.${ANCESTRY}.clean.vcf.gz | grep -v "^#" | awk '{OFS="\t"; $7 = "PASS"; print $0}') | bgzip -c > ${LABEL}.${ANCESTRY}.clean.epacts.vcf.gz
		tabix -p vcf ${LABEL}.${ANCESTRY}.clean.epacts.vcf.gz
	fi
	if [ -f ${LABEL}.${ANCESTRY}.clean.chr1-22.vcf.gz ]; then
		(zcat ${LABEL}.${ANCESTRY}.clean.chr1-22.vcf.gz | grep "^#"; zcat ${LABEL}.${ANCESTRY}.clean.chr1-22.vcf.gz | grep -v "^#" | awk '{OFS="\t"; $7 = "PASS"; print $0}') | bgzip -c > ${LABEL}.${ANCESTRY}.clean.chr1-22.epacts.vcf.gz
		tabix -p vcf ${LABEL}.${ANCESTRY}.clean.chr1-22.epacts.vcf.gz
	fi

elif [ "$QC_STEP" == "final_pca" ]; then
	if [ "$ANCESTRY" == "NA" ]; then
		echo "population (-p) must be set for QC step 4"
		exit
	fi
	reuse R-3.1

	# calculate PCs on clean datasets for inclusion as covariates in analysis
	cd data_clean_pca
	$PLINK --bfile ../data_clean/${LABEL}.${ANCESTRY}.clean.chr1-22 --indep-pairwise 1500 150 0.2 --out ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pre
	$PLINK --bfile ../data_clean/${LABEL}.${ANCESTRY}.clean.chr1-22 --extract ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pre.prune.in --exclude $REGIONS_EXCLUDE --maf 0.01 --make-bed --out ${LABEL}.${ANCESTRY}.clean.chr1-22.eig
	mv ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.fam ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.fam.orig
	$R --vanilla --args ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.fam.orig $KG_ETHNICITY ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.fam $LABEL < $ADD_ETHNICITY_R
	$PLINK --bfile ${LABEL}.${ANCESTRY}.clean.chr1-22.eig --recode --out ${LABEL}.${ANCESTRY}.clean.chr1-22.eig
	echo "genotypename:       ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.ped" > ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "snpname:            ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.map" >> ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "indivname:          ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.fam" >> ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "evecoutname:        ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.evec" >> ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "evaloutname:        ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.eval" >> ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "altnormstyle:       NO" >> ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "numoutevec:         10" >> ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "numoutlieriter:     0" >> ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "nsnpldregress:      0" >> ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "noxdata:            YES" >> ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "numoutlierevec:     10" >> ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "outliersigmathresh: 6" >> ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par
	echo "outlieroutname:     ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.outliers" >> ${LABEL}.${ANCESTRY}.clean.eig.pca.par
	echo "snpweightoutname:   ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.snpwts" >> ${LABEL}.${ANCESTRY}.clean.eig.pca.par
	$SMARTPCA -p ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.par > ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.log
	$R --vanilla --args ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.pca.evec < $PLOT_FINAL_PCS_R
	cd ..

elif [ "$QC_STEP" == "epacts_kinship" ]; then # run this interactively in gsa5
	reuse GCC-5.2
	reuse Tabix
	cd epacts_kinship_matrix

	$PLINK --bfile ../data_clean_pca/${LABEL}.${ANCESTRY}.clean.chr1-22.eig --recode vcf-iid bgz --out ${LABEL}.${ANCESTRY}.clean.chr1-22.eig
	tabix -p vcf ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.vcf.gz

	$EPACTS make-kin --vcf ${LABEL}.${ANCESTRY}.clean.chr1-22.eig.vcf.gz --out ${LABEL}.${ANCESTRY}.clean.chr1-22.epacts.kinf --run 16
	# to view the kinship matrix, use
	# /humgen/diabetes/users/ryank/software/EPACTS-3.2.6/bin/pEmmax kin-util --kinf ${LABEL}.biallelic.chr1-22.ALL.clean.epacts.kinf --outf test --dump
	cd ..

else
	echo "not a valid QC step"
fi