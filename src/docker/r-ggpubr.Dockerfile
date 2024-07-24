FROM debian:bookworm-slim
# FROM python:3.12.4-slim-bookworm

# all main work in work
WORKDIR /work

# https support, libcurl, locales
RUN apt-get update && \
	apt-get -y install apt-utils apt-transport-https libcurl4-openssl-dev locales dirmngr ca-certificates software-properties-common gnupg2 && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# Set the locale
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8     

# install debian package for R
RUN echo "deb http://cloud.r-project.org/bin/linux/debian bookworm-cran40/" >> /etc/apt/sources.list && \
	apt-key adv --keyserver keyserver.ubuntu.com --recv-key '95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7' && \
	apt-get update && \
	apt-get -y install r-base && \
	ln -s /usr/bin/R /usr/local/bin/R && \
	ln -s /usr/bin/Rscript /usr/local/bin/Rscript && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# install R packages
RUN R -e 'install.packages(c("argparse", "reshape2", "ggplot2", "gridExtra", "corrplot", "gtable", "UpSetR","pryr"), repos="http://cran.us.r-project.org", dependencies=TRUE)'

# install nloptr older version
RUN R -e 'install.packages("https://cran.r-project.org/src/contrib/Archive/nloptr/nloptr_1.2.1.tar.gz", repos=NULL, type="source")'

# install ggpubr
RUN R -e 'install.packages("ggpubr", repos="http://cran.us.r-project.org", dependencies=TRUE)'

# open /work directory permissions
RUN chmod -R 777 /work
