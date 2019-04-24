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
	echo "dependencies:" >> /work/environment.yml && \
	echo "- numpy=1.13.3=py27_blas_openblas_200" >> /work/environment.yml && \
	echo "- pandas=0.21.0=py27_0" >> /work/environment.yml && \
	echo "- python=2.7.13=0" >> /work/environment.yml && \
	echo "- scipy=0.19.1=py27_blas_openblas_202" >> /work/environment.yml && \
	echo "- matplotlib=2.1.0=py27_0" >> /work/environment.yml && \
	echo "- seaborn=0.8.1=py27_0" >> /work/environment.yml && \
	echo "- pysam=0.6=py27_0" >> /work/environment.yml && \
	echo "- ghostscript=9.20=0" >> /work/environment.yml && \
	echo "- decorator=4.3.0=py_0" >> /work/environment.yml && \
	echo "- libgcc=7.2.0=h69d50b8_2" >> /work/environment.yml && \
	conda env create -f /work/environment.yml

RUN ln -s /opt/conda/envs/env/bin/python /usr/local/bin/python

# activate the myapp environment
ENV PATH /opt/conda/envs/env/bin:$PATH

# open /work directory permissions
RUN chmod -R 777 /work
