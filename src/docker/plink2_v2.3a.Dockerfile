FROM debian:stretch-slim

# all main work in work
WORKDIR /work

# install standard libraries and tools
RUN apt-get update && \
	apt-get -y install wget zip unzip && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN wget https://s3.amazonaws.com/plink2-assets/alpha2/plink2_linux_x86_64.zip && \
	unzip plink2_linux_x86_64.zip && \
	chmod +x plink2 && \
	ln -s /work/plink2 /usr/local/bin/plink2

# open /work directory permissions
RUN chmod -R 777 /work
