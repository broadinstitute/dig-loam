FROM continuumio/anaconda

# all main work in work
WORKDIR /work

RUN conda install -n root conda=4.6

# install ldsc
RUN git clone https://github.com/bulik/ldsc.git && \
	cd ldsc && \
	echo "name: base" > environment.yml && \
	echo "channels:" >> environment.yml && \
	echo "- bioconda" >> environment.yml && \
	echo "dependencies:" >> environment.yml && \
	echo "- python=2.7" >> environment.yml && \
	echo "- bitarray=0.8" >> environment.yml && \
	echo "- nose=1.3" >> environment.yml && \
	echo "- pybedtools=0.7" >> environment.yml && \
	echo "- pip" >> environment.yml && \
	echo "- pandas=0.19" >> environment.yml && \
	echo "- pip:" >> environment.yml && \
	echo "  - scipy==0.18" >> environment.yml && \
	echo "  - numpy==1.11" >> environment.yml && \
	conda env update --name base --file environment.yml && \
	conda clean -afy

RUN ln -s /opt/conda/envs/env/bin/python2 /usr/local/bin/python && \
	ln -s /work/ldsc/ldsc.py /usr/local/bin/ldsc.py && \
	ln -s /work/ldsc/munge_sumstats.py /usr/local/bin/munge_sumstats.py && \
	cd ldsc && \
	mv ldscore/sumstats.py ldscore/sumstats.py.orig && \
	sed -n '1,53p' ldscore/sumstats.py.orig > ldscore/sumstats.py && \
	echo "    paths = [os.path.expanduser(os.path.expandvars(x)) for x in flist]" >> ldscore/sumstats.py && \
	sed -n '57,$p' ldscore/sumstats.py.orig >> ldscore/sumstats.py

# activate the myapp environment
ENV PATH /opt/conda/envs/base/bin:$PATH

# open /work directory permissions
RUN chmod -R 777 /work
