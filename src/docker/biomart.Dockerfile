FROM python:3.6.9-slim-stretch

# install numpy
RUN pip3 install numpy

# install pandas
RUN pip3 install pandas

# install biomart
RUN pip3 install biomart
