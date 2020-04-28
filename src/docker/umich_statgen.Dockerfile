FROM ubuntu:16.04

RUN set -x && \
    apt-get update && \
	apt-get install -y \
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
		locales \
        python \
        python-pip \
        r-base \
        rpm \
		wget \
		zlib1g-dev && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/* && \
    pip install cget

WORKDIR /work

# Set the locale
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8     

# install epacts
RUN wget https://github.com/statgen/EPACTS/archive/v3.3.2.tar.gz && \
	tar zxvf v3.3.2.tar.gz && \
	rm v3.3.2.tar.gz && \
	cd EPACTS-3.3.2 && \
	sed -i 's/single\.b\.spa\.R//g' data/#_assh_bfan_b_ahome_ahmkang_acode_aworking_aEPACTS_adata_aMakefile.am# && \
	sed -i 's/single\.b\.spa\.R//g' data/Makefile && \
	sed -i 's/single\.b\.spa\.R//g' data/Makefile.in && \
	mkdir build && \
	./configure --prefix /work/EPACTS-3.3.2/build && \
	make && \
	make install && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts /usr/local/bin/epacts && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-anno /usr/local/bin/epacts-anno && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-cat /usr/local/bin/epacts-cat && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-cis-extract /usr/local/bin/epacts-cis-extract && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-download /usr/local/bin/epacts-download && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-enrich /usr/local/bin/epacts-enrich && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-group /usr/local/bin/epacts-group && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-make-group /usr/local/bin/epacts-make-group && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-make-kin /usr/local/bin/epacts-make-kin && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-multi /usr/local/bin/epacts-multi && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-pca-plot /usr/local/bin/epacts-pca-plot && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-plot /usr/local/bin/epacts-plot && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-single /usr/local/bin/epacts-single && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts-zoom /usr/local/bin/epacts-zoom && \
	ln -s /work/EPACTS-3.3.2/build/bin/epacts.pm /usr/local/bin/epacts.pm && \
	ln -s /work/EPACTS-3.3.2/build/bin/epstopdf /usr/local/bin/epstopdf && \
	ln -s /work/EPACTS-3.3.2/build/bin/test_run_epacts.sh /usr/local/bin/test_run_epacts.sh && \
	ln -s /work/EPACTS-3.3.2/build/bin/wGetOptions.pm /usr/local/bin/wGetOptions.pm && \
	ln -s /work/EPACTS-3.3.2/build/bin/anno /usr/local/bin/anno && \
	ln -s /work/EPACTS-3.3.2/build/bin/bgzip /usr/local/bin/bgzip && \
	ln -s /work/EPACTS-3.3.2/build/bin/chaps /usr/local/bin/chaps && \
	ln -s /work/EPACTS-3.3.2/build/bin/pEmmax /usr/local/bin/pEmmax && \
	ln -s /work/EPACTS-3.3.2/build/bin/tabix /usr/local/bin/tabix && \
	ln -s /work/EPACTS-3.3.2/build/bin/vcfast /usr/local/bin/vcfast && \
	rm /work/EPACTS-3.3.2/build/share/EPACTS/*.gz && \
	rm /work/EPACTS-3.3.2/build/share/EPACTS/*.tbi && \
	rm -r data && \
	rm -r src && \
	rm -r scripts && \
	rm -r m4 && \
	rm Makefile && \
	rm config.log && \
	rm config.status && \
	rm libtool && \
	rm stamp-h1 && \
	rm .gitignore && \
	rm LICENSE && \
	rm Makefile.am && \
	rm Makefile.in && \
	rm README.md && \
	rm aclocal.m4 && \
	rm config.guess && \
	rm config.h && \
	rm config.h.in && \
	rm config.sub && \
	rm configure && \
	rm configure.ac && \
	rm depcomp && \
	rm install-sh && \
	rm ltmain.sh && \
	rm missing && \
	cp -r /work/EPACTS-3.3.2/build/share/EPACTS /usr/local/share/ && \
	cp -r /work/EPACTS-3.3.2/build/lib/* /usr/local/lib/

# install raremetal
RUN git clone https://github.com/statgen/raremetal.git && \
	cd raremetal && \
	cget install -f requirements.txt && \
	mkdir build && \
	cd build && \
	cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=../cget/cget/cget.cmake -DBUILD_TESTS=1 .. && \
	make && \
	ln -s /work/raremetal/build/raremetal /usr/local/bin/raremetal && \
	ln -s /work/raremetal/build/raremetalworker /usr/local/bin/raremetalworker && \
	cd ../ && \
	rm -r .git && \
	rm -r cget && \
	rm -r data && \
	rm -r dep && \
	rm -r libRareMetal && \
	rm -r otherLib && \
	rm -r raremetal && \
	rm -r raremetalworker && \
	rm -r script && \
	rm -r tests && \
	rm tutorial && \
	rm .gitignore && \
	rm CMakeLists.txt && \
	rm README.md && \
	rm requirements.txt

# install rvtest
RUN mkdir rvtests_linux64 && \
	cd rvtests_linux64 && \
	wget https://github.com/zhanxw/rvtests/releases/download/v2.1.0/rvtests_linux64.tar.gz && \
	tar zxvf rvtests_linux64.tar.gz && \
	rm rvtests_linux64.tar.gz && \
	chmod +x executable/rvtest && \
	chmod +x executable/vcf2kinship && \
	ln -s /work/rvtests_linux64/executable/bgenFileInfo /usr/local/bin/bgenFileInfo && \
	ln -s /work/rvtests_linux64/executable/bgen2vcf /usr/local/bin/bgen2vcf && \
	ln -s /work/rvtests_linux64/executable/createVCFIndex /usr/local/bin/createVCFIndex && \
	ln -s /work/rvtests_linux64/executable/explainCSI2 /usr/local/bin/explainCSI2 && \
	ln -s /work/rvtests_linux64/executable/extractVCFIndex /usr/local/bin/extractVCFIndex && \
	ln -s /work/rvtests_linux64/executable/queryVCFIndex /usr/local/bin/queryVCFIndex && \
	ln -s /work/rvtests_linux64/executable/explainCSI1 /usr/local/bin/explainCSI1 && \
	ln -s /work/rvtests_linux64/executable/explainTabix /usr/local/bin/explainTabix && \
	ln -s /work/rvtests_linux64/executable/combineKinship /usr/local/bin/combineKinship && \
	ln -s /work/rvtests_linux64/executable/kinshipDecompose /usr/local/bin/kinshipDecompose && \
	ln -s /work/rvtests_linux64/executable/vcf2ld_neighbor /usr/local/bin/vcf2ld_neighbor && \
	ln -s /work/rvtests_linux64/executable/vcfPeek /usr/local/bin/vcfPeek && \
	ln -s /work/rvtests_linux64/executable/vcf2kinship /usr/local/bin/vcf2kinship && \
	ln -s /work/rvtests_linux64/executable/vcfPair /usr/local/bin/vcfPair && \
	ln -s /work/rvtests_linux64/executable/vcfIndvSummary /usr/local/bin/vcfIndvSummary && \
	ln -s /work/rvtests_linux64/executable/vcfVariantSummaryLite /usr/local/bin/vcfVariantSummaryLite && \
	ln -s /work/rvtests_linux64/executable/vcfAnnoSummaryLite /usr/local/bin/vcfAnnoSummaryLite && \
	ln -s /work/rvtests_linux64/executable/vcfSummaryLite /usr/local/bin/vcfSummaryLite && \
	ln -s /work/rvtests_linux64/executable/vcf2ld_window /usr/local/bin/vcf2ld_window && \
	ln -s /work/rvtests_linux64/executable/vcf2ld_gene /usr/local/bin/vcf2ld_gene && \
	ln -s /work/rvtests_linux64/executable/vcfExtractSite /usr/local/bin/vcfExtractSite && \
	ln -s /work/rvtests_linux64/executable/plink2vcf /usr/local/bin/plink2vcf && \
	ln -s /work/rvtests_linux64/executable/vcf2geno /usr/local/bin/vcf2geno && \
	ln -s /work/rvtests_linux64/executable/vcfConcordance /usr/local/bin/vcfConcordance && \
	ln -s /work/rvtests_linux64/executable/vcfSummary /usr/local/bin/vcfSummary && \
	ln -s /work/rvtests_linux64/executable/vcf2plink /usr/local/bin/vcf2plink && \
	ln -s /work/rvtests_linux64/executable/rvtest /usr/local/bin/rvtest && \
	rm -r example && \
	rm README.md

# install metal
RUN wget http://csg.sph.umich.edu/abecasis/Metal/download/Linux-metal.tar.gz && \
	tar zxvf Linux-metal.tar.gz && \
	rm Linux-metal.tar.gz && \
	chmod +x generic-metal/metal && \
	ln -s /build/metal/generic-metal/metal /usr/local/bin/metal && \
	rm -r generic-metal/examples && \
	rm generic-metal/ChangeLog && \
	rm generic-metal/LICENSE.twister && \
	rm generic-metal/README

# add custom epacts r scripts
COPY . /usr/local/share/EPACTS/

# open /work directory permissions
RUN chmod -R 777 /work
