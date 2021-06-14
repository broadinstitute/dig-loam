FROM docker.pkg.github.com/rgcgithub/regenie/regenie:v2.0.2.gz

# install locales
RUN apt-get update && \
	apt-get -y install locales && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# set the locale
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8     
