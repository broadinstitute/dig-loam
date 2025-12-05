FROM debian:bookworm-slim

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

Run R -e 'install.packages(c("devtools"))' && \
	R -e 'install.packages(c("dplyr","tidyr","ggplot2","MASS","meta","ggrepel","DT"))' && \
	R -e 'devtools::install_github("PheWAS/PheWAS")'

# open /work directory permissions
RUN chmod -R 777 /work
