FROM continuumio/miniconda

# all main work in work
WORKDIR /work

# install libGL.so.1
RUN apt-get update && \
	apt-get install -y libgl1-mesa-glx gcc && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN conda update -y conda

# install ldsc
RUN git clone https://github.com/bulik/ldsc.git && \
	cd ldsc && \
	echo "- cython" >> environment.yml && \
	conda env create -f environment.yml

RUN	ln -s /work/ldsc/ldsc.py /usr/local/bin/ldsc.py && \
	ln -s /work/ldsc/munge_sumstats.py /usr/local/bin/munge_sumstats.py

# open /work directory permissions
RUN chmod -R 777 /work
