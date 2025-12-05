FROM python:3.7.9-slim-stretch

# all main work in work
WORKDIR /work

# install modules
RUN pip3 install numpy && \
	pip3 install pandas && \
	pip3 install datatable && \
	pip3 install scipy && \
	pip3 install matplotlib && \
	pip3 install seaborn && \
	pip3 install pysam && \
	pip3 install biomart && \
	pip3 install h5py

# open /work directory permissions
RUN chmod -R 777 /work
