FROM debian:stretch-slim

# all main work in work
WORKDIR /work

# set locale
RUN apt-get update && \
	apt-get -y install locales && \
	locale-gen en_US.UTF-8

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# install standard libraries and tools
RUN apt-get update && \
	apt-get -y install apt-utils apt-transport-https libjpeg62-turbo-dev libfreetype6-dev libpng-dev libagg-dev

# install python 2.7
RUN apt-get update && \
	apt-get -y install python2.7 python-setuptools && \
	ln -s /usr/bin/python /usr/local/bin/python

# install pip
RUN easy_install pip

# open /root directory permissions
RUN chmod -R 777 /root