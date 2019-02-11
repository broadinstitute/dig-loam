FROM ubuntu:bionic

# all main work in work
WORKDIR /work

# set locale
RUN apt-get update && \
	apt-get -y install locales && \
	locale-gen en_US.UTF-8

ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8  

# add unicode support for python
ENV PYTHONIOENCODING UTF-8

# install standard libraries and tools
RUN apt-get update && \
	apt-get -y install apt-utils apt-transport-https libjpeg8-dev libfreetype6-dev libpng-dev libagg-dev software-properties-common

# install python 3
RUN apt-get update && \
	apt-get -y install python3-pip && \
    ln -s /usr/bin/python3 /usr/local/bin/python

# open /work directory permissions
RUN chmod -R 777 /work
