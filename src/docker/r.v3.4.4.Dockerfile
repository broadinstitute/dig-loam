FROM rocker/r-ver:3.4.4

# all main work in work
WORKDIR /work

# install R packages
RUN R -e 'install.packages(c("argparse", "reshape2", "ggplot2", "gridExtra", "caret", "corrplot", "gtable", "UpSetR","pryr"), repos="http://cran.us.r-project.org", dependencies=TRUE)' && \
	R -e 'source("http://bioconductor.org/biocLite.R"); biocLite(c("GENESIS", "SNPRelate", "GWASTools", "gdsfmt"))'

# open /work directory permissions
RUN chmod -R 777 /work
