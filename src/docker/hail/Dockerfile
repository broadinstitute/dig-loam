FROM broadinstitute/dig-loam:python36_base

# all main work in work
WORKDIR /work

# install Java JDK
RUN apt-get update && \
	apt-get -y install software-properties-common && \
	add-apt-repository -y ppa:openjdk-r/ppa && \
	apt-get update && \
	apt-get -y install openjdk-8-jdk

# set JAVA_HOME and add it to PATH
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME

# install hail v0.2
RUN pip3 install hail==0.2.13

# open /work directory permissions
RUN chmod -R 777 /work
