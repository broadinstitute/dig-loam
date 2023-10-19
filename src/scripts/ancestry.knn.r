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
parser$add_argument("--out-predictions", dest="out_predictions", type="character", help="An output filename for predictions")
parser$add_argument("--out-inferred", dest="out_inferred", type="character", help="An output filename for predictions")
parser$add_argument("--out-plots", dest="out_plots", type="character", help="An output filename for plots ending in .pdf")
args<-parser$parse_args()

print(args)

pcs<-read.table(args$pca_scores, header=T, as.is=T, stringsAsFactors=F,colClasses=c("IID"="character"))
pcs$POP<-args$project_id
pcs$GROUP<-args$project_id

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

pheno<-pcs
nor <-function(x) { (x -min(x))/(max(x)-min(x))   }
accuracy <- function(x){sum(diag(x)/(sum(rowSums(x)))) * 100}

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
print(accuracydf)

pheno_study$best<-apply(pheno_study[,names(pheno_study)[grep("pred_",names(pheno_study))]],1,function(x) names(which.max(table(x))))
pheno_study$best_pct<-apply(pheno_study[,names(pheno_study)[grep("pred_",names(pheno_study))]],1,function(x) table(x)[names(table(x)) == names(which.max(table(x)))]/18)

write.table(pheno_study,args$out_predictions,col.names=T,row.names=F,quote=F,sep="\t",append=F)
write.table(pheno_study[,c("IID","best")],args$out_inferred,col.names=F,row.names=F,quote=F,sep="\t",append=F)

plot_data<-merge(pcs,pheno_study[,c("IID","best")],all.x=T)

plot_data$GROUP<-factor(plot_data$GROUP)
plot_data$best<-factor(plot_data$best)

gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}
color<-gg_color_hue(length(unique(plot_data$GROUP)))
color[grep(args$project_id,levels(plot_data$GROUP))]<-"#191919"

print(table(plot_data$GROUP))
print(table(plot_data$best))

pdf(args$out_plots,width=7,height=7)
p<-ggplot() +
	geom_point(data=plot_data[which(plot_data$GROUP != args$project_id),],aes(PC1,PC2,color=GROUP), size=2, shape=16) +
	geom_point(data=plot_data[which(plot_data$GROUP == args$project_id),],aes(PC1,PC2,color=GROUP, shape=best), size=2) +
	scale_colour_manual(name = "GROUP",values = color) +
	scale_shape_manual(name = "PRED",values = c(0,1,2,5,6)) +
	theme_bw() +
	guides(col = guide_legend(override.aes=list(shape = as.integer(levels(plot_data$GROUP) != args$project_id)*15 + 1, size=rep(4, length(levels(plot_data$GROUP)))))) +
	theme(axis.line = element_line(colour = "black"), 
	panel.grid.major = element_blank(),
	panel.grid.minor = element_blank(),
	panel.border = element_blank(),
	panel.background = element_blank(),
	legend.key = element_blank())
plot(p)

p<-ggplot() +
	geom_point(data=plot_data[which(plot_data$GROUP != args$project_id),],aes(PC2,PC3,color=GROUP), size=2, shape=16) +
	geom_point(data=plot_data[which(plot_data$GROUP == args$project_id),],aes(PC2,PC3,color=GROUP, shape=best), size=2) +
	scale_colour_manual(name = "GROUP",values = color) +
	scale_shape_manual(name = "PRED",values = c(0,1,2,5,6)) +
	theme_bw() +
	guides(col = guide_legend(override.aes=list(shape = as.integer(levels(plot_data$GROUP) != args$project_id)*15 + 1, size=rep(4, length(levels(plot_data$GROUP)))))) +
	theme(axis.line = element_line(colour = "black"), 
	panel.grid.major = element_blank(),
	panel.grid.minor = element_blank(),
	panel.border = element_blank(),
	panel.background = element_blank(),
	legend.key = element_blank())
plot(p)

dev.off()
