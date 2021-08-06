in=$1
out=$2
gene_map=$3
python=$4
python_script=$5

cat $gene_map | sed -i '1d'

zcat $1 | sed -e 's/#//g' | awk -F "\t" 'NR>1 && $3 !~ /singleton/ {print $3, $3, $15, $9}' OFS='\t'|\
awk -F "\t" '{sub(/\..*$/,"", $1); print}' OFS="\t" | awk -F "\t" '{sub(/^.{15}\./,"", $2); print}' OFS="\t"| sed '1iID\tID_num\tP\tEffect' > ${out}.temp01.tsv

cat $gene_map ${out}.temp01.tsv |
awk -F "\t" 'NF==2 {map[$1]=$2} NF>2 && map[$1] {$1=map[$1]; print $0}' OFS="\t" | awk -F "\t" 'NR>1 {print $0, $1 "." $2}' OFS="\t" | \
awk -F "\t" '{print $1, $5, $3, $4}' OFS="\t" | sed '1iID\tID_num\tP\tEffect' > ${out}.temp1.tsv


zcat $in | sed -e 's/#//g' | awk -F "\t" 'NR>1 && $3 !~ /singleton/ {print $3, $3, $13}' OFS='\t'| awk -F "\t" '{sub(/\..*$/,"", $1); print}' OFS='\t' |\
awk -F "\t" '{sub(/^.{15}\./,"", $2); print}' OFS='\t' | sed '1iID_num\tID_haha\tMAF' > ${out}.temp02.tsv

cat $gene_map ${out}.temp02.tsv | \
awk -F " " 'NF==2 {map[$1]=$2} NF>2 && map[$1] {$1=map[$1]; print $0}' OFS="\t" | awk -F "\t" 'NR>1 {print $0, $1 "." $2}' OFS='\t' | \
awk -F "\t" '{print $4, "Var_" $4, $3}' OFS='\t'| sed '1iID_num\tVar\tMAF' > ${out}.temp2.tsv

rm ${out}.temp01.tsv
rm ${out}.temp02.tsv

rm ${out}.temp1.tsv
rm ${out}.temp2.tsv

$python $python_script --p-value-file ${out}.temp1.tsv \
--gene-group-file ${out}.temp1.tsv --group-variant-file ${out}.temp2.tsv --p-value-file-id-col 2 --p-value-file-p-col 3 \
--p-value-file-effect-col 4 --gene-group-file-gene-col 1 --gene-group-file-id-col 2 --group-variant-file-id-col 1 \
--group-variant-file-variant-col 2 --group-variant-file-maf-col 3 --basic --out-file $out
