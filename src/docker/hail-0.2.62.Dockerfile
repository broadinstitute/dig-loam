FROM python:3.7.9-slim-stretch

# all main work in work
WORKDIR /work

# install Java JDK
RUN apt-get update && \
	apt-get -y install software-properties-common && \
	mkdir -p /usr/share/man/man1 && \
	apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' && \
	apt-get update && \
	apt-get -y install \
		g++ \
		libopenblas-base \
		liblapack3 \
		openjdk-8-jdk && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# set JAVA_HOME and add it to PATH
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME

# install hail v0.2.61
RUN pip3 install hail==0.2.62

# open /work directory permissions
RUN chmod -R 777 /work
