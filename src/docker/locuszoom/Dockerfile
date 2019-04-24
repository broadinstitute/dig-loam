FROM broadinstitute/dig-loam:python27_base

# all main work in work
WORKDIR /work

# install wget
RUN apt-get update && \
	apt-get -y install wget zlib1g-dev bzip2 build-essential

# install new_fugue
RUN cd /usr/local/src && \
	wget -q http://www.sph.umich.edu/csg/abecasis/downloads/generic-new_fugue-2010-06-02.tar.gz && \
	tar zxvf generic-new_fugue-2010-06-02.tar.gz && \
	cd generic-new_fugue && make all && make install && cd ../ && \
	rm generic-new_fugue-2010-06-02.tar.gz

# install plink1.9 and ghostscript
RUN apt-get update && \
	apt-get -y install plink1.9 && \
	ln -s /usr/bin/plink1.9 /usr/local/bin/plink && \
	apt-get -y install ghostscript && \
	ln -s /usr/bin/gs /usr/local/bin/gs

# install htslib
RUN apt-get update && \
	apt-get -y install libbz2-dev liblzma-dev libcurl4-gnutls-dev libssl-dev && \
	wget https://github.com/samtools/htslib/releases/download/1.9/htslib-1.9.tar.bz2 && \
	tar -jxvf htslib-1.9.tar.bz2 && \
	cd htslib-1.9 && \
	./configure --prefix=/usr/local/src/htslib-1.9 && \
	make && \
	make install && \
	cd .. && \
	rm -r htslib-1.9 && \
	ln -s /usr/local/src/htslib-1.9/bin/tabix /usr/local/bin/tabix && \
	ln -s /usr/local/src/htslib-1.9/bin/bgzip /usr/local/bin/bgzip

ENV PATH=/usr/local/src/htslib-1.9/bin:$PATH

# install debian package for R 3.4 and gridExtra package
RUN echo "deb https://cloud.r-project.org/bin/linux/ubuntu xenial/" >> /etc/apt/sources.list && \
	apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9 && \
	apt-get update && \
	apt-get -y install r-base && \
	ln -s /usr/bin/R /usr/local/bin/R && \
	ln -s /usr/bin/Rscript /usr/local/bin/Rscript && \
	R -e 'install.packages(c("gridExtra"), repos="http://cran.us.r-project.org")'

# install locuszoom
RUN cd /usr/local/src && \
	wget -q https://statgen.sph.umich.edu/locuszoom/download/locuszoom_1.4_srconly.tgz && \
	tar zxvf locuszoom_1.4_srconly.tgz && \
	ln -s /usr/local/src/locuszoom/bin/locuszoom /usr/local/bin/locuszoom && \
	rm locuszoom_1.4_srconly.tgz

# make conf dir in locuszoom/
RUN mkdir /usr/local/src/locuszoom/conf && \
	mkdir /usr/local/src/locuszoom/data

# make conf file in locuszoom/conf
RUN echo """# Required programs.\n\
METAL2ZOOM_PATH = \"bin/locuszoom.R\";\n\
NEWFUGUE_PATH = \"new_fugue\";\n\
PLINK_PATH = \"plink\";\n\
RSCRIPT_PATH = \"Rscript\";\n\
TABIX_PATH = \"tabix\";\n\
\n\
# SQLite database settings.\n\
SQLITE_DB = {\n\
  'hg18' : \"/usr/local/src/locuszoom/data/database/locuszoom_hg18.db\",\n\
  'hg19' : \"/usr/local/src/locuszoom/data/database/locuszoom_hg19.db\",\n\
  'hg38' : \"/usr/local/src/locuszoom/data/database/locuszoom_hg38.db\",\n\
};\n\
\n\
# GWAS catalog files\n\
GWAS_CATS = {\n\
  'hg18' : {\n\
    'whole-cat_significant-only' : {\n\
      'file' : \"/usr/local/src/locuszoom/data/gwas_catalog/gwas_catalog_hg18.txt\",\n\
      'desc' : \"The NHGRI GWAS catalog, filtered to SNPs with p-value < 5E-08\"\n\
    }\n\
  },\n\
  'hg19' : {\n\
    'whole-cat_significant-only' : {\n\
      'file' : \"/usr/local/src/locuszoom/data/gwas_catalog/gwas_catalog_hg19.txt\",\n\
      'desc' : \"The EBI GWAS catalog, filtered to SNPs with p-value < 5E-08\"\n\
    }\n\
  },\n\
  'hg38' : {\n\
    'whole-cat_significant-only' : {\n\
      'file' : \"/usr/local/src/locuszoom/data/gwas_catalog/gwas_catalog_hg38.txt\",\n\
      'desc' : \"The EBI GWAS catalog, filtered to SNPs with p-value < 5E-08\"\n\
    }\n\
  }\n\
}\n\
\n\
# Location of genotypes to use for LD calculations.\n\
LD_DB = {\n\
  # 1000G phase 3\n\
  '1000G_Nov2014' : {\n\
    'hg19' : {\n\
      'EUR' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2014-10-14/EUR/\",\n\
      },\n\
      'ASN' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2014-10-14/ASN/\",\n\
      },\n\
      'AFR' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2014-10-14/AFR/\",\n\
      },\n\
      'AMR' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2014-10-14/AMR/\",\n\
      }\n\
    },\n\
    'hg38' : {\n\
      'EUR' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2017-04-10/EUR/\",\n\
      },\n\
      'AFR' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2017-04-10/AFR/\",\n\
      },\n\
      'AMR' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2017-04-10/AMR/\",\n\
      },\n\
      'EAS' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2017-04-10/EAS/\",\n\
      },\n\
      'SAS' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2017-04-10/SAS/\",\n\
      }\n\
    }\n\
  },\n\
  '1000G_March2012' : {\n\
    'hg19' : {\n\
      'EUR' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2012-03/EUR/\",\n\
      },\n\
      'ASN' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2012-03/ASN/\",\n\
      },\n\
      'AFR' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2012-03/AFR/\",\n\
      },\n\
      'AMR' : {\n\
        'bim_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2012-03/AMR/\",\n\
      }\n\
    }\n\
  },\n\
  '1000G_June2010' : {\n\
    'hg18' : {\n\
      'CEU' : {\n\
        'ped_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2010-06/CEU/pedFiles/\",\n\
        'map_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2010-06/CEU/mapFiles/\"\n\
      },\n\
      'JPT+CHB' : {\n\
        'ped_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2010-06/JPT+CHB/pedFiles/\",\n\
        'map_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2010-06/JPT+CHB/mapFiles/\"\n\
      },\n\
      'YRI' : {\n\
        'ped_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2010-06/YRI/pedFiles/\",\n\
        'map_dir' : \"/usr/local/src/locuszoom/data/1000G/genotypes/2010-06/YRI/mapFiles/\"\n\
      }\n\
    }\n\
  },\n\
  'hapmap' : {\n\
    'hg18' : {\n\
      'CEU' : {\n\
        'ped_dir' : \"/usr/local/src/locuszoom/data/hapmap/genotypes/2008-10_phaseII/CEU/pedFiles/\",\n\
        'map_dir' : \"/usr/local/src/locuszoom/data/hapmap/genotypes/2008-10_phaseII/CEU/mapFiles/\"\n\
      },\n\
      'JPT+CHB' : {\n\
        'ped_dir' : \"/usr/local/src/locuszoom/data/hapmap/genotypes/2008-10_phaseII/JPT+CHB/pedFiles/\",\n\
        'map_dir' : \"/usr/local/src/locuszoom/data/hapmap/genotypes/2008-10_phaseII/JPT+CHB/mapFiles/\"\n\
      },\n\
      'YRI' : {\n\
        'ped_dir' : \"/usr/local/src/locuszoom/data/hapmap/genotypes/2008-10_phaseII/YRI/pedFiles/\",\n\
        'map_dir' : \"/usr/local/src/locuszoom/data/hapmap/genotypes/2008-10_phaseII/YRI/mapFiles/\"\n\
      }\n\
    }\n\
  }\n\
}""" > /usr/local/src/locuszoom/conf/m2zfast.conf

# open /work directory permissions
RUN chmod -R 777 /work
