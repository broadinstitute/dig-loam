FROM python:3.6.9-slim-stretch

# all main work in work
WORKDIR /work

#RUN apt-get update && \
#	apt-get -y install software-properties-common && \
#	mkdir -p /usr/share/man/man1 && \
#	add-apt-repository -y ppa:openjdk-r/ppa && \
#	apt-get update && \
#	apt-get -y install openjdk-8-jre-headless \ \
#		g++ \
#		libopenblas-base \
#		liblapack3 && \
#	apt-get clean && \
#	rm -rf /var/lib/apt/lists/*

# install Java JDK
RUN mkdir -p /usr/share/man/man1 && \
	apt-get update && \
	apt-get -y install \
		openjdk-8-jre-headless \
		g++ \
		libopenblas-base \
		liblapack3 && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# set JAVA_HOME and add it to PATH
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME

# install hail v0.2.52
RUN pip3 install hail==0.2.52

# open /work directory permissions
RUN chmod -R 777 /work
