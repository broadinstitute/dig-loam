# Must be compiled inside of EPACTS-3.4.2 (https://github.com/statgen/EPACTS/archive/v3.4.2.tar.gz) release source directory
# This version removes ENTRYPOINT and CMD to allow for Loamstream style

FROM ubuntu:16.04

ENV SRC_DIR /tmp/epacts-src

RUN set -x \
    && apt-get update && apt-get install -y \
        build-essential \
		r-mathlib \
        cmake \
        curl \
        ghostscript \
        git \
        gnuplot \
        groff \
        help2man \
        lsb-release \
        python \
        python-pip \
        r-base \
        rpm \
		wget \
		zlib1g-dev \
    && pip install cget

WORKDIR ${SRC_DIR}
COPY requirements.txt ${SRC_DIR}
RUN cget install -DCMAKE_C_FLAGS="-fPIC" -DCMAKE_CXX_FLAGS="-fPIC" -f requirements.txt \
    && mkdir -p ${SRC_DIR}/build

COPY . ${SRC_DIR}
WORKDIR ${SRC_DIR}/build
RUN cmake -DCMAKE_TOOLCHAIN_FILE=../cget/cget/cget.cmake -DCMAKE_BUILD_TYPE=Release .. \
    && make install \
    && rm -rf ${SRC_DIR}

WORKDIR /work

# install raremetal
RUN git clone https://github.com/statgen/raremetal.git && \
	cd raremetal && \
	cget install -f requirements.txt && \
	mkdir build && \
	cd build && \
	cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=../cget/cget/cget.cmake -DBUILD_TESTS=1 .. && \
	make && \
	ln -s /work/raremetal/build/raremetal /usr/local/bin/raremetal && \
	ln -s /work/raremetal/build/raremetalworker /usr/local/bin/raremetalworker

# install rvtest
RUN mkdir -p rvtests_linux64 && \
	cd rvtests_linux64 && \
	wget https://github.com/zhanxw/rvtests/releases/download/v2.1.0/rvtests_linux64.tar.gz && \
	tar zxvf rvtests_linux64.tar.gz && \
	chmod +x executable/rvtest && \
	chmod +x executable/vcf2kinship && \
	ln -s /work/rvtests_linux64/executable/rvtest /usr/local/bin/rvtest && \
	ln -s /work/rvtests_linux64/executable/vcf2kinship /usr/local/bin/vcf2kinship && \
	rm rvtests_linux64.tar.gz

# install metal
RUN wget http://csg.sph.umich.edu/abecasis/Metal/download/Linux-metal.tar.gz && \
	tar zxvf Linux-metal.tar.gz && \
	chmod +x generic-metal/metal && \
	ln -s /work/generic-metal/metal /usr/local/bin/metal && \
	rm Linux-metal.tar.gz

# open /work directory permissions
RUN chmod -R 777 /work
