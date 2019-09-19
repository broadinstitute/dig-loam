FROM debian:stretch-slim

# all main work in work
WORKDIR /work

# install standard libraries and tools
RUN apt-get update && \
	apt-get -y install wget build-essential git && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# install eigen, spectra, and boost
RUN wget -q http://bitbucket.org/eigen/eigen/get/3.3.7.tar.gz && \
	tar -zxvf 3.3.7.tar.gz && \
	rm 3.3.7.tar.gz && \
	mv eigen-* eigen-3.3.7 && \
	wget -q https://github.com/yixuan/spectra/archive/v0.8.1.tar.gz && \
	tar -zxvf v0.8.1.tar.gz && \
	rm v0.8.1.tar.gz && \
	wget https://dl.bintray.com/boostorg/release/1.71.0/source/boost_1_71_0.tar.gz && \
	tar -zxvf boost_1_71_0.tar.gz && \
	rm boost_1_71_0.tar.gz && \
	cd boost_1_71_0 && \
	./bootstrap.sh --prefix=/work/boost_1_71_0/build && \
	./b2 install

# add boost lib to LD_LIBRARY_PATH
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/work/boost_1_71_0/build/lib

# install flashpca
RUN git clone git://github.com/gabraham/flashpca && \
	cd flashpca && \
	make all EIGEN_INC=/work/eigen-3.3.7 BOOST_INC=/work/boost_1_71_0/build/include BOOST_LIB=/work/boost_1_71_0/build/lib SPECTRA_INC=/work/spectra-0.8.1/include

# make flashpca accessible with appropriate library links
RUN echo '#!/bin/bash' > /usr/local/bin/flashpca.sh && \
	echo 'export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/work/boost_1_71_0/build/lib' >> /usr/local/bin/flashpca.sh && \
	echo '/work/flashpca/flashpca "$@"' >> /usr/local/bin/flashpca.sh && \
	chmod +x /usr/local/bin/flashpca.sh

# open /work directory permissions
RUN chmod 777 /work && \
	chmod 777 /work/eigen-3.3.7 && \
	chmod 777 /work/spectra-0.8.1 && \
	chmod 777 /work/boost_1_71_0 && \
	chmod 777 /work/flashpca
