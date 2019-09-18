#!/bin/bash

# run eigenstrat on merged data, projecting data onto 1kg PCs for corresponding population
awk '{print $1,$2}' ../data/${LABEL}.base.fam > ${LABEL}.keep
plink --bfile ../ancestry_pca/${LABEL}_1kg.ref --keep ../ancestry_cluster/ancestry.CLUSTERED.plink --make-bed --out ${LABEL}_1kg.ref.CLUSTERED
plink --bfile ${LABEL}_1kg.ref.CLUSTERED --recode --out ${LABEL}_1kg.ref.CLUSTERED
# generate parameter file for smartpca
fastmode: YES
echo "genotypename:       ${LABEL}_1kg.ref.CLUSTERED.ped" > ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "snpname:            ${LABEL}_1kg.ref.CLUSTERED.map" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "indivname:          ${LABEL}_1kg.ref.CLUSTERED.fam" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "evecoutname:        ${LABEL}_1kg.ref.CLUSTERED.pca.evec" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "evaloutname:        ${LABEL}_1kg.ref.CLUSTERED.pca.eval" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "altnormstyle:       NO" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "numoutevec:         10" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "numoutlieriter:     0" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "nsnpldregress:      0" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "noxdata:            YES" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "numoutlierevec:     10" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "outliersigmathresh: 6" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "outlieroutname:     ${LABEL}_1kg.ref.CLUSTERED.pca.outliers" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
echo "snpweightoutname:   ${LABEL}_1kg.ref.CLUSTERED.pca.snpwts" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par

# run smartpca
$SMARTPCA -p ${LABEL}_1kg.ref.CLUSTERED.pca.par > ${LABEL}_1kg.ref.CLUSTERED.pca.log
