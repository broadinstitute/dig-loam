FROM continuumio/miniconda

# all main work in work
WORKDIR /work

# update conda
RUN conda update -y conda

# install SAIGE
RUN conda create -n RSAIGE r-essentials r-base=3.6.1 python=2.7 && \
	conda activate RSAIGE && \
	conda install -c anaconda cmake && \
	conda install -c conda-forge gettext lapack r-matrix && \
	conda install -c r r-rcpp  r-rcpparmadillo r-data.table r-bh && \
	conda install -c conda-forge r-spatest r-rcppeigen r-devtools  r-skat r-rcppparallel r-optparse boost openblas && \
	pip3 install cget click

RUN ln -s /opt/conda/envs/env/bin/saige /usr/local/bin/saige

# add conda env bin to path
ENV PATH /opt/conda/envs/env/bin:$PATH

# open /work directory permissions
RUN chmod -R 777 /work
