FROM debian:stretch-slim

# all main work in work
WORKDIR /work

# install standard libraries and tools
RUN apt-get update && \
	apt-get -y install wget tar && \
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
