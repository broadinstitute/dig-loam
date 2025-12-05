FROM debian:bullseye-slim

# all main work in work
WORKDIR /work

# install standard libraries and tools
RUN apt-get update && \
	apt-get -y install wget zip unzip tar && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN wget https://s3.amazonaws.com/plink2-assets/alpha6/plink2_linux_x86_64_20241111.zip && \
	unzip plink2_linux_x86_64_20241111.zip && \
	chmod +x plink2 && \
	ln -s /work/plink2 /usr/local/bin/plink2

RUN apt-get update && \
	apt-get -y install tabix && \
	ln -s /usr/bin/tabix /usr/local/bin/tabix && \
	ln -s /usr/bin/bgzip /usr/local/bin/bgzip && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN wget https://www.well.ox.ac.uk/~gav/resources/bgen_v1.1.4-Ubuntu16.04-x86_64.tgz && \
	tar -zxvf bgen_v1.1.4-Ubuntu16.04-x86_64.tgz && \
	chmod +x bgen_v1.1.4-Ubuntu16.04-x86_64 && \
	cd bgen_v1.1.4-Ubuntu16.04-x86_64 && \
	chmod +x bgenix && \
	chmod +x cat-bgen && \
	chmod +x edit-bgen && \
	ln -s /work/bgen_v1.1.4-Ubuntu16.04-x86_64/bgenix /usr/local/bin/bgenix && \
	ln -s /work/bgen_v1.1.4-Ubuntu16.04-x86_64/cat-bgen /usr/local/bin/cat-bgen && \
	ln -s /work/bgen_v1.1.4-Ubuntu16.04-x86_64/edit-bgen /usr/local/bin/edit-bgen

# open /work directory permissions
RUN chmod -R 777 /work
