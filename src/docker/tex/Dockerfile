FROM debian:jessie-slim

# all main work in work
WORKDIR /work

RUN apt-get update && \
	apt-get -y install texlive-latex-base ghostscript && \
	apt-get -y install texlive-fonts-recommended texlive-fonts-extra texlive-latex-extra && \
	ln -s /usr/bin/gs /usr/local/bin/gs && \
	ln -s /usr/bin/pdflatex /usr/local/bin/pdflatex

# open /work directory permissions
RUN chmod -R 777 /work
