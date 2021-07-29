gene_map=55k.gencode.gene.map

min_p_file01=`zcat $1 | sed -e 's/#//g' | awk -F "\t" 'NR>1 && $3 !~ /singleton/ {print $3, $3, $15, $9}' OFS='\t'|\
awk -F "\t" '{sub(/\..*$/,"", $1); print}' OFS="\t" | awk -F "\t" '{sub(/^.{15}\./,"", $2); print}' OFS="\t"| sed '1iID\tID_num\tP\tEffect'`

min_p_file1=`cat ${gene_map} ${min_p_file01} | \
awk -F " " 'NF==2 {map[$1]=$2} NF>2 && map[$1] {$1=map[$1]; print $0}' OFS="\t" | awk -F "\t" 'NR>1 {print $0, $1 "." $2}' OFS="\t" | \
awk -F "\t" '{print $1, $5, $3, $4}' OFS="\t" | sed '1iID\tID_num\tP\tEffect'`


min_p_file02=`zcat $1 | sed -e 's/#//g' | awk -F "\t" 'NR>1 && $3 !~ /singleton/ {print $3, $3, $13}' OFS='\t'| awk -F "\t" '{sub(/\..*$/,"", $1); print}' OFS='\t' |\
awk -F "\t" '{sub(/^.{15}\./,"", $2); print}' OFS='\t' | sed '1iID_num\tID_haha\tMAF'`

min_p_file2=`cat ${gene_map} ${min_p_file02} | \
awk -F " " 'NF==2 {map[$1]=$2} NF>2 && map[$1] {$1=map[$1]; print $0}' OFS="\t" | awk -F "\t" 'NR>1 {print $0, $1 "." $2}' OFS='\t' | \
awk -F "\t" '{print $4, "Var_" $4, $3}' OFS='\t'| sed '1iID_num\tVar\tMAF'`

python utils.python.pyMinPValTest --p-value-file ${min_p_file1} \
--gene-group-file ${min_p_file1} --group-variant-file ${min_p_file2} --p-value-file-id-col 2 --p-value-file-p-col 3 \
--p-value-file-effect-col 4 --gene-group-file-gene-col 1 --gene-group-file-id-col 2 --group-variant-file-id-col 1 \
--group-variant-file-variant-col 2 --group-variant-file-maf-col 3 --basic --out-file min_p_all.tsv