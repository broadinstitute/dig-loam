FROM ghcr.io/rgcgithub/regenie/regenie:v3.1.2.gz

# install locales
RUN apt-get update && \
	apt-get -y install locales tabix && \
    ln -s /usr/bin/tabix /usr/local/bin/tabix && \
	ln -s /usr/bin/bgzip /usr/local/bin/bgzip && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# set the locale
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8     
