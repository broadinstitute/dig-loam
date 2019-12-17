library(reshape2)
library(ggplot2)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--pca-scores", dest="pca_scores", type="character", help="A file containing PCA scores")
parser$add_argument("--update-pop", nargs=3, dest="update_pop", default=NULL, type="character", help="A column name for sample ID, a column name for POP, and the filename. This argument updates the POP field for all overlapping sample IDs in the file")
parser$add_argument("--update-group", nargs=3, dest="update_group", default=NULL, type="character", help="A column name for sample ID, a column name for GROUP, and the filename. This argument updates the GROUP field for all overlapping sample IDs in the file")
parser$add_argument("--cluster", dest="cluster", type="character", help="A klustakwik cluster file")
parser$add_argument("--sample-file", dest="sample_file", type="character", help="A sample file")
parser$add_argument("--project-id", dest="project_id", type="character", help="A project id")
parser$add_argument("--sample-id", dest="sample_id", type="character", help="A sample file id column name")
parser$add_argument("--sr-race", dest="sr_race", type="character", help="A sample file self reported race column name")
parser$add_argument("--afr-codes", dest="afr_codes", type="character", help="A comma separated list of codes for AFR group in sr_race column")
parser$add_argument("--amr-codes", dest="amr_codes", type="character", help="A comma separated list of codes for AMR group in sr_race column")
parser$add_argument("--eur-codes", dest="eur_codes", type="character", help="A comma separated list of codes for EUR group in sr_race column")
parser$add_argument("--eas-codes", dest="eas_codes", type="character", help="A comma separated list of codes for EAS group in sr_race column")
parser$add_argument("--sas-codes", dest="sas_codes", type="character", help="A comma separated list of codes for SAS group in sr_race column")
parser$add_argument("--cluster-plots", dest="cluster_plots", type="character", help="An output filename for cluster plots")
parser$add_argument("--xtabs", dest="xtabs", type="character", help="An output filename for cross tabs")
parser$add_argument("--plots-centers", dest="plots_centers", type="character", help="An output filename for cluster plots with centers")
parser$add_argument("--cluster-groups", dest="cluster_groups", type="character", help="An output filename for cluster groups")
parser$add_argument("--ancestry-inferred", dest="ancestry_inferred", type="character", help="An output filename for inferred ancestry")
parser$add_argument("--cluster-plots-no1kg", dest="cluster_plots_no1kg", type="character", help="An output filename for cluster plots without 1kg samples")
args<-parser$parse_args()

print(args)

gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}

pcs<-read.table(args$pca_scores, header=T, as.is=T, stringsAsFactors=F)
pcs$POP<-args$project_id
pcs$GROUP<-args$project_id

if(! is.null(args$update_pop)) {
	print("updating population information from file")
	pop_df<-read.table(file=args$update_pop[3],header=TRUE,as.is=T,stringsAsFactors=FALSE)
	pop_df<-pop_df[,c(args$update_pop[1],args$update_pop[2])]
	names(pop_df)[1]<-"IID"
	names(pop_df)[2]<-"POP_NEW"
	pcs<-merge(pcs,pop_df,all.x=TRUE)
	pcs$POP[! is.na(pcs$POP_NEW)]<-pcs$POP_NEW[! is.na(pcs$POP_NEW)]
	pcs$POP_NEW<-NULL
}

if(! is.null(args$update_group)) {
	print("updating group information from file")
	group_df<-read.table(file=args$update_group[3],header=TRUE,as.is=T,stringsAsFactors=FALSE)
	group_df<-group_df[,c(args$update_group[1],args$update_group[2])]
	names(group_df)[1]<-"IID"
	names(group_df)[2]<-"GROUP_NEW"
	pcs<-merge(pcs,group_df,all.x=TRUE)
	pcs$GROUP[! is.na(pcs$GROUP_NEW)]<-pcs$GROUP_NEW[! is.na(pcs$GROUP_NEW)]
	pcs$GROUP_NEW<-NULL
}

cl<-read.table(args$cluster, as.is=T, skip=1, stringsAsFactors=F)
names(cl)[1]<-"CLUSTER"
pcs<-cbind(pcs,cl)
sample_file<-read.table(args$sample_file, header=T, as.is=T, stringsAsFactors=F, sep="\t")
sample_file<-sample_file[,c(args$sample_id,args$sr_race)]
sample_file[,args$sr_race]<-paste0("sr_",sample_file[,args$sr_race])
names(sample_file)[1]<-"IID"
pheno<-merge(pcs,sample_file,all.x=T)

color<-gg_color_hue(max(pheno$CLUSTER))
pheno$COLOR<-color[pheno$CLUSTER]
print(head(pheno))

cat("\nplotting clusters\n")
pdf(args$cluster_plots,width=7, height=7)
for(i in grep("^PC",names(pheno))[-length(grep("^PC",names(pheno)))]) {
	p<-ggplot(pheno, aes(pheno[,i],pheno[,i+1])) +
		geom_point(aes(color=factor(COLOR),shape=factor(GROUP))) +
		labs(x=paste("PC",i-grep("^PC",names(pheno))[1]+1,sep=""),y=paste("PC",i-grep("^PC",names(pheno))[1]+2,sep=""),shape="COHORT",colour="CLUSTER") +
		scale_color_manual(labels = sort(unique(pheno$CLUSTER)), values = color) +
		theme_bw() +
		guides(col = guide_legend(override.aes = list(shape = 15, size = 6))) +
		theme(axis.line = element_line(colour = "black"), 
		plot.title = element_blank(),
		panel.grid.major = element_blank(),
		panel.grid.minor = element_blank(),
		panel.border = element_blank(),
		panel.background = element_blank(),
		legend.key = element_blank())
	plot(p)
}
dev.off()

cat("\ndetermining unknown clusters\n")
clusters_unknown<-c()
clusters_exclude<-c()
for(i in unique(pheno$CLUSTER)[unique(pheno$CLUSTER) != 1]) {
	if(nrow(pheno[pheno$CLUSTER == i & pheno$GROUP == args$project_id,]) > 0) {
		clusters_unknown<-c(clusters_unknown,i)
	} else {
		clusters_exclude<-c(clusters_exclude,i)
	}
}
print(sort(clusters_unknown))

cat("\ndetermining centers of 1kg super populations\n")
i<-0
cohorts_1kg<-unique(pheno$GROUP)[unique(pheno$GROUP) != args$project_id]
for(c in cohorts_1kg) {
	i<-i+1
	if(i == 1) {
		centers_1kg<-colMeans(pheno[,c("PC1","PC2","PC3")][pheno$GROUP == c,])
	} else {
		centers_1kg<-rbind(centers_1kg,colMeans(pheno[,c("PC1","PC2","PC3")][pheno$GROUP == c,]))
	}
}
row.names(centers_1kg)<-cohorts_1kg
centers_1kg<-as.data.frame(centers_1kg)
print(centers_1kg)

cat("\ndetermining centers of project clusters\n")
i<-0
for(c in clusters_unknown) {
	i<-i+1
	if(i == 1) {
		centers_project<-colMeans(pheno[,c("PC1","PC2","PC3")][pheno$CLUSTER == c & pheno$GROUP == args$project_id,])
	} else {
		centers_project<-rbind(centers_project,colMeans(pheno[,c("PC1","PC2","PC3")][pheno$CLUSTER == c & pheno$GROUP == args$project_id,]))
	}
}
for(c in clusters_exclude) {
	centers_project<-rbind(centers_project,data.frame(PC1=NA,PC2=NA,PC3=NA))
}
centers_project<-rbind(centers_project,data.frame(PC1=NA,PC2=NA,PC3=NA))
row.names(centers_project)<-c(clusters_unknown,c(clusters_exclude,1))
centers_project<-as.data.frame(centers_project)
print(centers_project)

cat("\ncalculate ditances from project centers to 1kg centers\n")
centers_project$dist_AMR<-NA
centers_project$dist_AFR<-NA
centers_project$dist_EAS<-NA
centers_project$dist_EUR<-NA
centers_project$dist_SAS<-NA
for(i in 1:(nrow(centers_project)-(1+length(clusters_exclude)))) {
	centers_project$dist_AMR[i]<-sqrt(sum((centers_project[i,1:3]-centers_1kg["AMR",1:3])^2))
	centers_project$dist_AFR[i]<-sqrt(sum((centers_project[i,1:3]-centers_1kg["AFR",1:3])^2))
	centers_project$dist_EAS[i]<-sqrt(sum((centers_project[i,1:3]-centers_1kg["EAS",1:3])^2))
	centers_project$dist_EUR[i]<-sqrt(sum((centers_project[i,1:3]-centers_1kg["EUR",1:3])^2))
	centers_project$dist_SAS[i]<-sqrt(sum((centers_project[i,1:3]-centers_1kg["SAS",1:3])^2))
}
centers_project$closest1<-NA
centers_project$closest2<-NA
centers_project$ratio<-NA
for(i in 1:(nrow(centers_project)-(1+length(clusters_exclude)))) {
	s<-sort(centers_project[i,c("dist_AMR","dist_AFR","dist_EAS","dist_EUR","dist_SAS")])
	centers_project$closest1[i]<-gsub("dist_","",names(s)[1])
	centers_project$closest2[i]<-gsub("dist_","",names(s)[2])
	centers_project$ratio[i]<-as.numeric(s[2])/as.numeric(s[1])
}
print(centers_project)

cat("\ngenerate cluster table and add centers\n")
cluster_table<-as.data.frame.matrix(table(pheno[,c("CLUSTER","GROUP")]))
cluster_table<-cbind(cluster_table,as.data.frame.matrix(table(pheno[,c("CLUSTER",args$sr_race)])))
cluster_table$cluster<-as.integer(row.names(cluster_table))
centers_project$ASSIGNED<-"OUTLIERS"
centers_project$cluster<-as.integer(row.names(centers_project))
cluster_table<-merge(cluster_table,centers_project,all=T)
print(cluster_table)

cat("\nassign each cluster to nearest 1kg super population\n")
for(i in 1:nrow(cluster_table)) {
	print(cluster_table[i,])
	if(! is.na(cluster_table$ratio[i])) {
		if(cluster_table$ratio[i] >= 1.5) {
			cat("  ",paste0("assigning closest1 to cluster ",cluster_table$cluster[i]),"\n")
			cluster_table$ASSIGNED[i]<-cluster_table$closest1[i]
		} else {
			cat("  ",paste0("recalculating super population membership based on self report for cluster ",cluster_table$cluster[i]),"\n")
			c<-cluster_table[i,c("AMR","AFR","EAS","EUR","SAS")]
			if(! is.null(args$afr_codes)) {
				for(afr_code in afr_codes[paste0("sr_",afr_codes) %in% names(cluster_table)]) {
					c$AFR <- c$AFR + cluster_table[i,paste0("sr_",afr_code)]
				}
			}
			if(! is.null(args$amr_codes)) {
				for(amr_code in amr_codes[paste0("sr_",amr_codes) %in% names(cluster_table)]) {
					c$AMR <- c$AMR + cluster_table[i,paste0("sr_",amr_code)]
				}
			}
			if(! is.null(args$eur_codes)) {
				for(eur_code in eur_codes[paste0("sr_",eur_codes) %in% names(cluster_table)]) {
					c$EUR <- c$EUR + cluster_table[i,paste0("sr_",eur_code)]
				}
			}
			if(! is.null(args$eas_codes)) {
				for(eas_code in eas_codes[paste0("sr_",eas_codes) %in% names(cluster_table)]) {
					c$EAS <- c$EAS + cluster_table[i,paste0("sr_",eas_code)]
				}
			}
			if(! is.null(args$sas_codes)) {
				for(sas_code in sas_codes[paste0("sr_",sas_codes) %in% names(cluster_table)]) {
					c$SAS <- c$SAS + cluster_table[i,paste0("sr_",sas_code)]
				}
			}
			c<-sort(c,decreasing=TRUE)
			if(c[1] == 0) {
				cat("  ",paste0("assigning closest1 to cluster ",cluster_table$cluster[i]),"\n")
				cluster_table$ASSIGNED[i]<-cluster_table$closest1[i]
			} else {
				cat("  ",paste0("assigning recalculated super population to cluster ",cluster_table$cluster[i]),"\n")
				cluster_table$ASSIGNED[i]<-names(c)[1]
			}
		}
	}
}
sink(file=args$xtabs)
print(cluster_table)
sink()

cat("\nplotting cluster centers\n")
cluster_table_included<-cluster_table[! cluster_table$cluster %in% c(clusters_exclude,1),]
pdf(args$plots_centers,width=7, height=7)
for(i in seq(1,2)) {
	p<-ggplot(pheno, aes(pheno[,paste("PC",i,sep="")],pheno[,paste("PC",i+1,sep="")])) +
		geom_point(aes(color=factor(CLUSTER),shape=factor(GROUP))) +
		geom_point(data=centers_1kg, aes(centers_1kg[,paste("PC",i,sep="")],centers_1kg[,paste("PC",i+1,sep="")], shape=row.names(centers_1kg)), size=4, colour="black") +
		geom_text(data=cluster_table_included, aes(cluster_table_included[,paste("PC",i,sep="")],cluster_table_included[,paste("PC",i+1,sep="")], label=cluster), colour="black") +
		labs(x=paste("PC",i,sep=""),y=paste("PC",i+1,sep=""),shape="COHORT",colour="CLUSTER") +
		theme_bw() +
		guides(col = guide_legend(override.aes = list(shape = 15, size = 10))) +
		theme(axis.line = element_line(colour = "black"), 
		plot.title = element_blank(),
		panel.grid.major = element_blank(),
		panel.grid.minor = element_blank(),
		panel.border = element_blank(),
		panel.background = element_blank(),
		legend.key = element_blank())
	plot(p)
}
dev.off()

cat("\nexporting cluster groups table\n")
a<-data.frame(ASSIGNED_COHORT=c("AFR","AMR","EAS","EUR","SAS"),stringsAsFactors=F)
a$CLUSTERS<-NA
for(i in 1:nrow(cluster_table)) {
	if(cluster_table$ASSIGNED[i] != "OUTLIERS") {
		if(is.na(a$CLUSTERS[a$ASSIGNED_COHORT == cluster_table$ASSIGNED[i]])) {
			a$CLUSTERS[a$ASSIGNED_COHORT == cluster_table$ASSIGNED[i]]<-row.names(cluster_table)[i]
		} else {
			a$CLUSTERS[a$ASSIGNED_COHORT == cluster_table$ASSIGNED[i]]<-paste(a$CLUSTERS[a$ASSIGNED_COHORT == cluster_table$ASSIGNED[i]],row.names(cluster_table)[i],sep=",")
		}
	}
}
write.table(a,args$cluster_groups,row.names=F,col.names=F,sep="\t",append=F,quote=F)

cat("\nassigning each sample to a 1kg super population based on cluster membership\n")
pheno$ASSIGNED<-"OUTLIERS"
for(i in 1:nrow(a)) {
	pheno$ASSIGNED[pheno$CLUSTER %in% as.integer(unlist(strsplit(as.character(a$CLUSTERS[i]),split=",")))]<-a$ASSIGNED_COHORT[i]
}
write.table(pheno[which(pheno$GROUP == args$project_id),c("IID","ASSIGNED")],args$ancestry_inferred,col.names=F,row.names=F,quote=F,append=F,sep="\t")

cat("\nplotting clusters without 1kg\n")
pheno<-pheno[which(pheno$GROUP == args$project_id & pheno$ASSIGNED != "OUTLIERS"),]
pdf(args$cluster_plots_no1kg,width=7, height=7)
for(i in seq(1,9)) {
	p<-ggplot(pheno, aes(pheno[,paste("PC",i,sep="")],pheno[,paste("PC",i+1,sep="")])) +
		geom_point(aes(color=factor(CLUSTER),shape=factor(ASSIGNED))) +
		labs(x=paste("PC",i,sep=""),y=paste("PC",i+1,sep=""),shape="COHORT",colour="CLUSTER") +
		theme_bw() +
		guides(col = guide_legend(override.aes = list(shape = 15, size = 10))) +
		theme(axis.line = element_line(colour = "black"), 
		plot.title = element_blank(),
		panel.grid.major = element_blank(),
		panel.grid.minor = element_blank(),
		panel.border = element_blank(),
		panel.background = element_blank(),
		legend.key = element_blank())
	plot(p)
}
dev.off()
