FROM debian:stretch-slim

# all main work in work
WORKDIR /work

# install standard libraries and tools
RUN apt-get update && \
	apt-get -y install wget zlib1g-dev bzip2 zip unzip dos2unix libgomp1 software-properties-common build-essential && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN wget https://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20220402.zip && \
	unzip plink_linux_x86_64_20220402.zip && \
	chmod +x plink && \
	ln -s /work/plink /usr/local/bin/plink

# install king and modify ReadPLINK.cpp so that bim files with chr in chromosome codes is removed (they cause king to fail to find snps)
RUN mkdir king && \
	cd king && \
	wget -q https://www.kingrelatedness.com/KINGcode.tar.gz && \
	tar -zxvf KINGcode.tar.gz && \
	sed -i 's/if(tokens.Length() < 6) continue;/if(tokens.Length() < 6) continue; if(tokens[0].Find("chr") != -1) tokens[0].ExcludeCharacters("chr");/g' ReadPLINK.cpp && \
	c++ -lm -lz -O2 -fopenmp -o king *.cpp && \
	chmod +x king && \
	ln -s /work/king/king /usr/local/bin/king && \
	rm KINGcode.tar.gz

# open /work directory permissions
RUN chmod -R 777 /work
