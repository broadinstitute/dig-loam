FROM continuumio/anaconda3:latest

# all main work in work
WORKDIR /work

# install standard libraries and tools
RUN apt-get update && \
	apt-get -y install wget && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN wget -O DENTIST_1.1.0.0.gz https://www.dropbox.com/s/1mtskir8qzqsmee/DENTIST.1.1.0.0.gz?dl=0 && \
	gunzip DENTIST_1.1.0.0.gz && \
	chmod +x DENTIST_1.1.0.0 && \
	ln -s /work/DENTIST_1.1.0.0 /usr/local/bin/dentist

# open /work directory permissions
RUN chmod -R 777 /work
