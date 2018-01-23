args<-commandArgs(trailingOnly=T)
x<-try(read.table(args[1],header=T,as.is=T,stringsAsFactors=F), silent=TRUE)
if(inherits(x, "try-error")) {
	file.create(args[2])
} else {
	ids<-c(x$ID1,x$ID2)
	out<-as.data.frame(sort(table(ids),decreasing=T))
	names(out)[1]<-"FREQ"
	out$IID<-row.names(out)
	write.table(out[,c("IID","FREQ")],args[2],row.names=F,col.names=F,quote=F,append=F,sep="\t")
}
