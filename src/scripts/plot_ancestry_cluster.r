library(reshape2)
library(ggplot2)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--pca-scores", dest="pca_scores", type="character", help="A file containing PCA scores")
parser$add_argument("--cluster", dest="cluster", type="character", help="A klustakwik cluster file")
parser$add_argument("--sample-file", dest="sample_file", type="character", help="A phenotype file")
parser$add_argument("--project-id", dest="project_id", type="character", help="A project id")
parser$add_argument("--sample-id", dest="sample_id", type="character", help="A phenotype file column name")
parser$add_argument("--sr-race", dest="sr_race", type="character", help="A phenotype file self reported race column name")
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

dat<-read.table(args$pca_scores, header=T, as.is=T, stringsAsFactors=F)
cl<-read.table(args$cluster, as.is=T, skip=1, stringsAsFactors=F)
names(cl)[1]<-"CLUSTER"
dat<-cbind(dat,cl)
pheno<-read.table(args$sample_file, header=T, as.is=T, stringsAsFactors=F, sep="\t")
pheno<-pheno[,c(args$sample_id,args$sr_race)]
pheno[,args$sr_race]<-paste0("sr_",pheno[,args$sr_race])
names(pheno)[1]<-"IID"
dat<-merge(dat,pheno,all.x=T)

outfile<-paste(unlist(strsplit(args$pca_scores,"/"))[1:(length(unlist(strsplit(args$pca_scores,"/")))-1)],args$project_id,sep="/")
gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}
color<-gg_color_hue(max(dat$CLUSTER))

pdf(args$cluster_plots,width=7, height=7)
for(i in grep("^PC",names(dat))[-length(grep("^PC",names(dat)))]) {
	p<-ggplot(dat, aes(dat[,i],dat[,i+1])) +
		geom_point(aes(color=factor(CLUSTER),shape=factor(GROUP))) +
		labs(x=paste("PC",i-grep("^PC",names(dat))[1]+1,sep=""),y=paste("PC",i-grep("^PC",names(dat))[1]+2,sep=""),shape="COHORT",colour="CLUSTER") +
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

clusters_unknown<-c()
clusters_exclude<-c()
for(i in unique(dat$CLUSTER)[unique(dat$CLUSTER) != 1]) {
	if(nrow(dat[dat$CLUSTER == i & dat$GROUP == args$project_id,]) > 0) {
		clusters_unknown<-c(clusters_unknown,i)
	} else {
		clusters_exclude<-c(clusters_exclude,i)
	}
}

i<-0
cohorts_1kg<-unique(dat$GROUP)[unique(dat$GROUP) != args$project_id]
for(c in cohorts_1kg) {
	i<-i+1
	if(i == 1) {
		centers_1kg<-colMeans(dat[,c("PC1","PC2","PC3")][dat$GROUP == c,])
	} else {
		centers_1kg<-rbind(centers_1kg,colMeans(dat[,c("PC1","PC2","PC3")][dat$GROUP == c,]))
	}
}
row.names(centers_1kg)<-cohorts_1kg
centers_1kg<-as.data.frame(centers_1kg)

i<-0
for(c in clusters_unknown) {
	i<-i+1
	if(i == 1) {
		centers_unknown<-colMeans(dat[,c("PC1","PC2","PC3")][dat$CLUSTER == c & dat$GROUP == args$project_id,])
	} else {
		centers_unknown<-rbind(centers_unknown,colMeans(dat[,c("PC1","PC2","PC3")][dat$CLUSTER == c & dat$GROUP == args$project_id,]))
	}
}
for(c in clusters_exclude) {
	centers_unknown<-rbind(centers_unknown,data.frame(PC1=NA,PC2=NA,PC3=NA))
}
centers_unknown<-rbind(centers_unknown,data.frame(PC1=NA,PC2=NA,PC3=NA))
row.names(centers_unknown)<-c(clusters_unknown,c(clusters_exclude,1))
centers_unknown<-as.data.frame(centers_unknown)

centers_unknown$dist_AMR<-NA
centers_unknown$dist_AFR<-NA
centers_unknown$dist_EAS<-NA
centers_unknown$dist_EUR<-NA
centers_unknown$dist_SAS<-NA
for(i in 1:(nrow(centers_unknown)-(1+length(clusters_exclude)))) {
	centers_unknown$dist_AMR[i]<-sqrt(sum((centers_unknown[i,1:3]-centers_1kg["AMR",1:3])^2))
	centers_unknown$dist_AFR[i]<-sqrt(sum((centers_unknown[i,1:3]-centers_1kg["AFR",1:3])^2))
	centers_unknown$dist_EAS[i]<-sqrt(sum((centers_unknown[i,1:3]-centers_1kg["EAS",1:3])^2))
	centers_unknown$dist_EUR[i]<-sqrt(sum((centers_unknown[i,1:3]-centers_1kg["EUR",1:3])^2))
	centers_unknown$dist_SAS[i]<-sqrt(sum((centers_unknown[i,1:3]-centers_1kg["SAS",1:3])^2))
}

centers_unknown$closest1<-NA
centers_unknown$closest2<-NA
centers_unknown$ratio<-NA
for(i in 1:(nrow(centers_unknown)-(1+length(clusters_exclude)))) {
	s<-sort(centers_unknown[i,c("dist_AMR","dist_AFR","dist_EAS","dist_EUR","dist_SAS")])
	centers_unknown$closest1[i]<-gsub("dist_","",names(s)[1])
	centers_unknown$closest2[i]<-gsub("dist_","",names(s)[2])
	centers_unknown$ratio[i]<-as.numeric(s[2])/as.numeric(s[1])
}
# centers_unknown$ASSIGNED[! as.integer(row.names(centers_unknown)) %in% c(clusters_exclude,1) & centers_unknown$ratio >= 1.5]<-centers_unknown$closest1[! as.integer(row.names(centers_unknown)) %in% c(clusters_exclude,1) & centers_unknown$ratio >= 1.5]
# centers_unknown$ASSIGNED[! as.integer(row.names(centers_unknown)) %in% c(clusters_exclude,1) & centers_unknown$ratio < 1.5]<-centers_unknown$closest1[! as.integer(row.names(centers_unknown)) %in% c(clusters_exclude,1) & centers_unknown$ratio < 1.5]

bd<-as.data.frame.matrix(table(dat[,c("CLUSTER","GROUP")]))
bd<-cbind(bd,as.data.frame.matrix(table(dat[,c("CLUSTER",args$sr_race)])))
bd$cluster<-as.integer(row.names(bd))
centers_unknown$ASSIGNED<-"OUTLIERS"
centers_unknown$cluster<-as.integer(row.names(centers_unknown))
centers_unknown<-merge(bd,centers_unknown,all=T)
for(i in 1:nrow(centers_unknown)) {
	if(! is.na(centers_unknown$ratio[i])) {
		if(centers_unknown$ratio[i] >= 1.5) {
			centers_unknown$ASSIGNED[i]<-centers_unknown$closest1[i]
		} else {
			c<-centers_unknown[i,c("AMR","AFR","EAS","EUR","SAS")]
			if(args$afr_codes != "") {
				for(afr_code in unlist(strsplit(args$afr_codes,split=","))) {
					c$AFR <- c$AFR + centers_unknown[i,paste0("sr_",afr_code)]
				}
			}
			if(args$amr_codes != "") {
				for(amr_code in unlist(strsplit(args$amr_codes,split=","))) {
					c$AMR <- c$AMR + centers_unknown[i,paste0("sr_",amr_code)]
				}
			}
			if(args$eur_codes != "") {
				for(eur_code in unlist(strsplit(args$eur_codes,split=","))) {
					c$EUR <- c$EUR + centers_unknown[i,paste0("sr_",eur_code)]
				}
			}
			if(args$eas_codes != "") {
				for(eas_code in unlist(strsplit(args$eas_codes,split=","))) {
					c$EAS <- c$EAS + centers_unknown[i,paste0("sr_",eas_code)]
				}
			}
			if(args$sas_codes != "") {
				for(sas_code in unlist(strsplit(args$sas_codes,split=","))) {
					c$SAS <- c$SAS + centers_unknown[i,paste0("sr_",sas_code)]
				}
			}
			c<-sort(c,decreasing=TRUE)
			if(c[1] == 0) {
				centers_unknown$ASSIGNED[i]<-centers_unknown$closest1[i]
			} else {
				centers_unknown$ASSIGNED[i]<-names(c)[1]
			}
		}
	}
}
sink(file=args$xtabs)
print(centers_unknown)
sink()

centers_unknown_included<-centers_unknown[! centers_unknown$cluster %in% c(clusters_exclude,1),]
pdf(args$plots_centers,width=7, height=7)
for(i in seq(1,2)) {
	p<-ggplot(dat, aes(dat[,paste("PC",i,sep="")],dat[,paste("PC",i+1,sep="")])) +
		geom_point(aes(color=factor(CLUSTER),shape=factor(GROUP))) +
		geom_point(dat=centers_1kg, aes(centers_1kg[,paste("PC",i,sep="")],centers_1kg[,paste("PC",i+1,sep="")], shape=row.names(centers_1kg)), size=4, colour="black") +
		geom_text(dat=centers_unknown_included, aes(centers_unknown_included[,paste("PC",i,sep="")],centers_unknown_included[,paste("PC",i+1,sep="")], label=cluster), colour="black") +
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

a<-data.frame(ASSIGNED_COHORT=c("AFR","AMR","EAS","EUR","SAS"),stringsAsFactors=F)
a$CLUSTERS<-NA
for(i in 1:nrow(centers_unknown)) {
	if(centers_unknown$ASSIGNED[i] != "OUTLIERS") {
		if(is.na(a$CLUSTERS[a$ASSIGNED_COHORT == centers_unknown$ASSIGNED[i]])) {
			a$CLUSTERS[a$ASSIGNED_COHORT == centers_unknown$ASSIGNED[i]]<-row.names(centers_unknown)[i]
		} else {
			a$CLUSTERS[a$ASSIGNED_COHORT == centers_unknown$ASSIGNED[i]]<-paste(a$CLUSTERS[a$ASSIGNED_COHORT == centers_unknown$ASSIGNED[i]],row.names(centers_unknown)[i],sep=",")
		}
	}
}
write.table(a,args$cluster_groups,row.names=F,col.names=F,sep="\t",append=F,quote=F)

dat$ASSIGNED<-"OUTLIERS"
for(i in 1:nrow(a)) {
	dat$ASSIGNED[dat$CLUSTER %in% as.integer(unlist(strsplit(as.character(a$CLUSTERS[i]),split=",")))]<-a$ASSIGNED_COHORT[i]
}

### WRITE ASSIGMENTS TO FILES
write.table(dat[which(dat$GROUP == args$project_id),c("IID","ASSIGNED")],args$ancestry_inferred,col.names=F,row.names=F,quote=F,append=F,sep="\t")

gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}
color<-gg_color_hue(max(dat$CLUSTER))

dat<-dat[which(dat$GROUP == args$project_id & dat$ASSIGNED != "OUTLIERS"),]
pdf(args$cluster_plots_no1kg,width=7, height=7)
for(i in seq(1,9)) {
	p<-ggplot(dat, aes(dat[,paste("PC",i,sep="")],dat[,paste("PC",i+1,sep="")])) +
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
