FROM ubuntu:16.04

RUN set -x && \
    apt-get update && \
	apt-get install -y \
        build-essential \
		r-mathlib \
        cmake \
        curl \
        ghostscript \
        git \
        gnuplot \
        groff \
        help2man \
        lsb-release \
		locales \
        python \
        python-pip \
        r-base \
        rpm \
		wget \
		zlib1g-dev && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/* && \
    pip install cget

WORKDIR /work

# Set the locale
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8     

# modify imagemagick policy
RUN sed -i 's/<policy domain="coder" rights="none" pattern="PDF" \/>/<policy domain="coder" rights="read|write" pattern="PDF" \/>/g' /etc/ImageMagick-6/policy.xml && \
	chmod +x /usr/bin/convert

# open /work directory permissions
RUN chmod -R 777 /work
