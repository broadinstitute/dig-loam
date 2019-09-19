FROM continuumio/miniconda

# all main work in work
WORKDIR /work

# install libGL.so.1
RUN apt-get update && \
	apt-get install -y libgl1-mesa-glx

RUN conda update -y conda

RUN echo "name: env" > /work/environment.yml && \
	echo "channels:" >> /work/environment.yml && \
	echo "- conda-forge" >> /work/environment.yml && \
	echo "- anaconda" >> /work/environment.yml && \
	echo "dependencies:" >> /work/environment.yml && \
	echo "- numpy" >> /work/environment.yml && \
	echo "- pandas" >> /work/environment.yml && \
	echo "- python=2.7" >> /work/environment.yml && \
	echo "- scipy" >> /work/environment.yml && \
	echo "- matplotlib" >> /work/environment.yml && \
	echo "- seaborn" >> /work/environment.yml && \
	echo "- pysam" >> /work/environment.yml && \
	echo "- ghostscript" >> /work/environment.yml && \
	echo "- decorator" >> /work/environment.yml && \
	echo "- libgcc" >> /work/environment.yml && \
	conda env create -f /work/environment.yml

RUN ln -s /opt/conda/envs/env/bin/python3 /usr/local/bin/python

# activate the myapp environment
ENV PATH /opt/conda/envs/env/bin:$PATH

# open /work directory permissions
RUN chmod -R 777 /work
