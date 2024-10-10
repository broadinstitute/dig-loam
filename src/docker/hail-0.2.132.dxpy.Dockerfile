FROM debian:bullseye-slim

# all main work in work
WORKDIR /work

# install Java JDK
RUN apt-get update && \
	apt-get -y install software-properties-common && \
	mkdir -p /usr/share/man/man1 && \
	apt-add-repository 'deb http://deb.debian.org/debian bullseye main' && \
	apt-get update && \
	apt-get -y install \
		openjdk-11-jre-headless \
		g++ \
		python3.9 python3-pip \
		libopenblas-base liblapack3 \
		procps && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# set JAVA_HOME and add it to PATH
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME
ENV PYTHONPATH=$PYTHONPATH:/usr/local/lib/python3.9/dist-packages/hail

# install hail v0.2.61
# RUN pip3 install hail==0.2.132
RUN python3.9 -m pip install hail==0.2.132
RUN pip3 install dxpy

RUN ln -s /usr/bin/python3.9 /usr/local/bin/python

# open /work directory permissions
RUN chmod -R 777 /work
