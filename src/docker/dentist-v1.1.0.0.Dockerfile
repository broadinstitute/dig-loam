FROM debian:stretch-slim

# all main work in work
WORKDIR /work

RUN apt-get update && \
	apt-get -y install wget git gnupg2 apt-transport-https ca-certificates build-essential libz-dev libblas3 liblapack3 liblapack-dev libblas-dev && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# install standard libraries and tools
RUN wget https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB && \
	apt-key add GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB && \
	sh -c 'echo deb https://apt.repos.intel.com/mkl all main > /etc/apt/sources.list.d/intel-mkl.list' && \
	apt-get update && \
	apt-get -y install intel-mkl-64bit-2018.2-046 && \
	update-alternatives --install /usr/lib/x86_64-linux-gnu/libblas.so libblas.so-x86_64-linux-gnu /opt/intel/mkl/lib/intel64/libmkl_rt.so 50 && \
	update-alternatives --install /usr/lib/x86_64-linux-gnu/libblas.so.3 libblas.so.3-x86_64-linux-gnu /opt/intel/mkl/lib/intel64/libmkl_rt.so 50 && \
	update-alternatives --install /usr/lib/x86_64-linux-gnu/liblapack.so liblapack.so-x86_64-linux-gnu /opt/intel/mkl/lib/intel64/libmkl_rt.so 50 && \
	update-alternatives --install /usr/lib/x86_64-linux-gnu/liblapack.so.3 liblapack.so.3-x86_64-linux-gnu /opt/intel/mkl/lib/intel64/libmkl_rt.so 50 && \
	echo "/opt/intel/lib/intel64" > /etc/ld.so.conf.d/mkl.conf && \
	echo "/opt/intel/mkl/lib/intel64" >> /etc/ld.so.conf.d/mkl.conf && \
	ldconfig && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
	apt-get -y install libboost-all-dev && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN wget https://gitlab.com/libeigen/eigen/-/archive/3.4.0/eigen-3.4.0.tar.gz && \
	tar -xvf eigen-3.4.0.tar.gz

#ENV PATH=$PATH:/usr/local/include
ENV BOOST_PATH=/usr/lib/x86_64-linux-gnu
ENV MKLROOT=/opt/intel/compilers_and_libraries/linux/mkl
ENV EIGEN3_INCLUDE_DIR=/work/eigen-3.4.0
#ENV EIGEN3_INCLUDE_DIR=/usr/include/eigen3

#RUN ln -s /usr/include/eigen3/Eigen /usr/local/include/Eigen && \
#	ln -s /usr/include/eigen3/unsupported /usr/local/include/unsupported && \
#	ln -s /usr/include/eigen3/signature_of_eigen3_matrix_library /usr/local/include/signature_of_eigen3_matrix_library

RUN /opt/intel/bin/compilervars.sh intel64

RUN git clone https://github.com/Yves-CHEN/DENTIST && \
	cd DENTIST && \
	#sed -i 's/-DMKL_ILP64/-DMKL_LP64/g' Makefile && \
	#sed -i 's/libmkl_intel_ilp64/libmkl_intel_lp64/g' Makefile && \
	make && \
	chmod +x /work/DENTIST/builts/DENTIST.tmp2 && \
	ln -s /work/DENTIST/builts/DENTIST.tmp2 /usr/local/bin/dentist

# open /work directory permissions
RUN chmod -R 777 /work
