FROM python:3.6.9-slim-stretch

# all main work in work
WORKDIR /work

# https support, libcurl, locales
RUN apt-get update && \
	apt-get -y install wget apt-utils apt-transport-https libcurl4-openssl-dev locales dirmngr ca-certificates software-properties-common gnupg2 && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# Set the locale
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8     

# install debian package for R
RUN echo "deb https://cloud.r-project.org/bin/linux/debian stretch-cran35/" >> /etc/apt/sources.list && \
	apt-key adv --keyserver keys.gnupg.net --recv-key 'E19F5F87128899B192B1A2C2AD5F960A256A04AF' && \
	apt-get update && \
	apt-get -y install r-base && \
	ln -s /usr/bin/R /usr/local/bin/R && \
	ln -s /usr/bin/Rscript /usr/local/bin/Rscript && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# install R packages
RUN R -e 'install.packages(c("argparse", "reshape2"), repos="http://cran.us.r-project.org", dependencies=TRUE)'

# install Plink
RUN wget http://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20181202.zip && \
	unzip plink_linux_x86_64_20181202.zip && \
	chmod +x plink && \
	ln -s /work/plink /usr/local/bin/plink

# install flashpca
RUN wget https://github.com/gabraham/flashpca/releases/download/v2.0/flashpca_x86-64.gz && \
	gunzip flashpca_x86-64.gz && \
	ln -s /work/flashpca_x86-64 /usr/local/bin/flashpca

# open /work directory permissions
RUN chmod -R 777 /work

# remove python entry point from parent image
CMD ["bash"]
