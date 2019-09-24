FROM debian:stretch-slim

# all main work in work
WORKDIR /work

# install standard libraries and tools
RUN apt-get update && \
	apt-get -y install wget && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# install flashpca
RUN wget https://github.com/gabraham/flashpca/releases/download/v2.0/flashpca_x86-64.gz && \
	gunzip flashpca_x86-64.gz && \
	ln -s /work/flashpca_x86-64 /usr/local/bin/flashpca

# link flashpca and open /work directory permissions
RUN chmod -R 777 /work
