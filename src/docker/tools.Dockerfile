FROM debian:stretch-slim

# all main work in work
WORKDIR /work

# install standard libraries and tools
RUN apt-get update && \
	apt-get -y install wget zlib1g-dev bzip2 zip unzip dos2unix libgomp1

# install Java JDK
RUN apt-get update && \
	apt-get -y install software-properties-common && \
	mkdir -p /usr/share/man/man1 && \
	add-apt-repository -y ppa:openjdk-r/ppa && \
	apt-get update && \
	apt-get -y install openjdk-8-jdk && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# set JAVA_HOME and add it to PATH
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME

# install genotype harmonizer, klustakwik, plink1.9, tabix/bgzip, liftover, king
RUN wget -q https://molgenis26.gcc.rug.nl/downloads/GenotypeHarmonizer/GenotypeHarmonizer-1.4.20-dist.tar.gz && \
	tar -zxvf GenotypeHarmonizer-1.4.20-dist.tar.gz && \
	dos2unix GenotypeHarmonizer-1.4.20-SNAPSHOT/GenotypeHarmonizer.sh && \
	chmod +x GenotypeHarmonizer-1.4.20-SNAPSHOT/GenotypeHarmonizer.sh && \
	ln -s /work/GenotypeHarmonizer-1.4.20-SNAPSHOT/GenotypeHarmonizer.sh /work/GenotypeHarmonizer-1.4.20-SNAPSHOT/GenotypeHarmonizer && \
	rm GenotypeHarmonizer-1.4.20-dist.tar.gz

# set GENOTYPE_HARMONIZER_HOME and add the directory to the system path
ENV GENOTYPE_HARMONIZER_HOME=/work/GenotypeHarmonizer-1.4.20-SNAPSHOT/
ENV PATH=$PATH:$GENOTYPE_HARMONIZER_HOME

RUN apt-get update && \
	apt-get -y install klustakwik && \
	ln -s /usr/bin/KlustaKwik /usr/local/bin/KlustaKwik && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN wget http://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20181202.zip && \
	unzip plink_linux_x86_64_20181202.zip && \
	chmod +x plink && \
	ln -s /work/plink /usr/local/bin/plink

RUN apt-get update && \
	apt-get -y install tabix && \
	ln -s /usr/bin/tabix /usr/local/bin/tabix && \
	ln -s /usr/bin/bgzip /usr/local/bin/bgzip && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN wget -q http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/liftOver && \
	chmod +x liftOver && \
	ln -s /work/liftOver /usr/local/bin/liftOver

RUN wget -q http://people.virginia.edu/~wc9c/KING/executables/Linux-king212.tar.gz && \
	tar zxvf Linux-king212.tar.gz && \
	chmod +x king && \
	ln -s /work/king /usr/local/bin/king && \
	rm Linux-king212.tar.gz

# open /work directory permissions
RUN chmod -R 777 /work
