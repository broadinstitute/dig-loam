library(reshape2)
library(ggplot2)
library(argparse)
library(class)

parser <- ArgumentParser()
parser$add_argument("--pca-scores", dest="pca_scores", type="character", help="A file containing PCA scores")
parser$add_argument("--update-pop", nargs=3, dest="update_pop", default=NULL, type="character", help="A column name for sample ID, a column name for POP, and the filename. This argument updates the POP field for all overlapping sample IDs in the file")
parser$add_argument("--update-group", nargs=3, dest="update_group", default=NULL, type="character", help="A column name for sample ID, a column name for GROUP, and the filename. This argument updates the GROUP field for all overlapping sample IDs in the file")
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
parser$add_argument("--dump", dest="dump", type="character", help="An output filename for data table dump")
parser$add_argument("--plots-centers", dest="plots_centers", type="character", help="An output filename for cluster plots with centers")
parser$add_argument("--plots-individual", dest="plots_individual", type="character", help="An output filename for individual cluster plots with centers")
parser$add_argument("--cluster-groups", dest="cluster_groups", type="character", help="An output filename for cluster groups")
parser$add_argument("--ancestry-inferred", dest="ancestry_inferred", type="character", help="An output filename for inferred ancestry")
parser$add_argument("--cluster-plots-no1kg", dest="cluster_plots_no1kg", type="character", help="An output filename for cluster plots without 1kg samples")
args<-parser$parse_args()

print(args)

gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}

pcs<-read.table(args$pca_scores, header=T, as.is=T, stringsAsFactors=F,colClasses=c("IID"="character"))
pcs$POP<-args$project_id
pcs$GROUP<-args$project_id

print(table(pcs$GROUP))

if(! is.null(args$update_pop)) {
	print("updating population information from file")
	idcol<-args$update_pop[1]
	pop_df<-read.table(file=args$update_pop[3],header=TRUE,as.is=T,stringsAsFactors=FALSE,colClasses=c(eval(parse(text=paste0(idcol,"=\"character\"")))))
	pop_df<-pop_df[,c(args$update_pop[1],args$update_pop[2])]
	names(pop_df)[1]<-"IID"
	names(pop_df)[2]<-"POP_NEW"
	pcs<-merge(pcs,pop_df,all.x=TRUE)
	pcs$POP[! is.na(pcs$POP_NEW)]<-pcs$POP_NEW[! is.na(pcs$POP_NEW)]
	pcs$POP_NEW<-NULL
}

if(! is.null(args$update_group)) {
	print("updating group information from file")
	idcol<-args$update_group[1]
	group_df<-read.table(file=args$update_group[3],header=TRUE,as.is=T,stringsAsFactors=FALSE,colClasses=c(eval(parse(text=paste0(idcol,"=\"character\"")))))
	group_df<-group_df[,c(args$update_group[1],args$update_group[2])]
	names(group_df)[1]<-"IID"
	names(group_df)[2]<-"GROUP_NEW"
	pcs<-merge(pcs,group_df,all.x=TRUE)
	pcs$GROUP[! is.na(pcs$GROUP_NEW)]<-pcs$GROUP_NEW[! is.na(pcs$GROUP_NEW)]
	pcs$GROUP_NEW<-NULL
}

if(! is.null(args$sr_race)) {
	idcol<-args$sample_id
	sample_file<-read.table(args$sample_file, header=T, as.is=T, stringsAsFactors=F, sep="\t", colClasses=c(eval(parse(text=paste0(idcol,"=\"character\"")))))
	sample_file<-sample_file[,c(args$sample_id,args$sr_race)]
	sample_file[,args$sr_race]<-paste0("sr_",sample_file[,args$sr_race])
	names(sample_file)[1]<-"IID"
	pheno<-merge(pcs,sample_file,all.x=T)
	print(head(pheno))
	print(table(pheno$GROUP,pheno[,args$sr_race]))
}

pheno<-pcs
nor <-function(x) { (x -min(x))/(max(x)-min(x))   }
accuracy <- function(x){sum(diag(x)/(sum(rowSums(x)))) * 100}

##set seed
#set.seed(1234)

###extract only 1kg
#pheno_1kg<-pheno[pheno$GROUP != args$project_id,]
#
#ran <- sample(1:nrow(pheno_1kg), 0.9 * nrow(pheno_1kg)) 
#
#accuracydf<-data.frame(Trial = integer(), N_PCs = integer(), Accuracy = numeric())
#
#for(k in 1:10) {
#	for(i in 5:22) {
#
#		print(paste0("...running trial ",k,"/100 - ",i,"/22"))
#		pheno_norm <- as.data.frame(lapply(pheno_1kg[,3:i], nor))
#		summary(pheno_norm)
#		
#		pheno_train <- pheno_norm[ran,] 
#		pheno_test <- pheno_norm[-ran,] 
#		
#		pheno_target_category <- pheno_1kg[ran,24]
#		table(pheno_target_category)
#		
#		pheno_test_category <- pheno_1kg[-ran,24]
#		table(pheno_test_category)
#		
#		pr <- knn(pheno_train,pheno_test,cl=pheno_target_category,k=floor(sqrt(nrow(pheno_test))))
#		tab <- table(pr,pheno_test_category)
#		
#		accuracydf<-rbind(accuracydf,data.frame(Trial = k, N_PCs = c(i-2), Accuracy = c(accuracy(tab))))
#		
#	}
#}
#
#print(accuracydf)
#pc_performance<-aggregate(Accuracy ~ N_PCs, data = accuracydf, mean)
#print(pc_performance)
#
#N <- pc_performance$N_PCs[pc_performance$Accuracy == max(pc_performance$Accuracy)][1]
#print(paste0("PCs required for maximum accuracy: ",N))

accuracydf<-data.frame(N_PCs = integer(), AFR = integer(), AMR = integer(), EAS = integer(), EUR = integer(), SAS = integer())

study_samples<-which(pheno$GROUP == args$project_id)
print(table(pheno[-study_samples,24]))
print(table(pheno[study_samples,24]))
pheno_study <- pheno[study_samples,]

##set seed
set.seed(1234)

for(N in 5:22) {
	pheno_norm <- as.data.frame(lapply(pheno[,3:N], nor))
	pheno_train <- pheno_norm[-study_samples,]
	pheno_test <- pheno_norm[study_samples,]
	pheno_target_category <- pheno[-study_samples,24]
	pr <- knn(pheno_train,pheno_test,cl=pheno_target_category,k=floor(sqrt(nrow(pheno_test))))
	pheno_study[,paste0("pred_",N-2)]<-pr
	print(paste0("adding values for ",N-2," PCs"))
	prt<-table(pr)
	accuracydf<-rbind(accuracydf,data.frame(N_PCs = c(N-2), AFR = c(prt[["AFR"]]), AMR = c(prt[["AMR"]]), EAS = c(prt[["EAS"]]), EUR = c(prt[["EUR"]]), SAS = c(prt[["SAS"]])))
}
pheno_study$best<-apply(pheno_study[,names(pheno_study)[grep("pred_",names(pheno_study))]],1,function(x) names(which.max(table(x))))
pheno_study$best_pct<-apply(pheno_study[,names(pheno_study)[grep("pred_",names(pheno_study))]],1,function(x) table(x)[names(table(x)) == names(which.max(table(x)))]/18)

#write.table(pheno_study)
print(accuracydf)
print(table(pheno_study$best))
print(head(pheno_study))

write.table(accuracydf,"counts.txt",col.names=T,row.names=F,quote=F,sep="\t",append=F)
write.table(pheno_study,"predictions.txt",col.names=T,row.names=F,quote=F,sep="\t",append=F)

#pheno_1kg$pr<-pheno_1kg$GROUP
#final<-rbind(cbind(pheno_study,pr),pheno_1kg)
#print(head(final))
#
#cat("\nplotting groups\n")
#pdf(args$cluster_plots,width=7, height=7)
#for(i in seq(1,N-3)) {
#	p<-ggplot(final, aes(final[,paste("PC",i,sep="")],final[,paste("PC",i+1,sep="")])) +
#		geom_point(aes(color=factor(GROUP),shape=factor(pr))) +
#		labs(x=paste("PC",i,sep=""),y=paste("PC",i+1,sep=""),shape="pr",colour="GROUP") +
#		theme_bw() +
#		guides(col = guide_legend(override.aes = list(shape = 15, size = 10))) +
#		theme(axis.line = element_line(colour = "black"), 
#		plot.title = element_blank(),
#		panel.grid.major = element_blank(),
#		panel.grid.minor = element_blank(),
#		panel.border = element_blank(),
#		panel.background = element_blank(),
#		legend.key = element_blank())
#	plot(p)
#}
#dev.off()
