FROM ubuntu:22.04
USER root
# INSTALLING SYSTEM DEPENDENCIES
RUN apt-get update -y && apt-get install wget gdal-bin git vim \
    gcc libsasl2-dev python-dev-is-python3 libldap2-dev \
    libssl-dev ffmpeg libsm6 libxext6 exiv2 -y
# coping the code in the container
COPY . /usr/src/dbiait/
# installing miniconda
RUN mkdir -p ~/miniconda3
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
RUN bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3

COPY deploy/dramatiq.service /etc/systemd/system/dbiaitd.service
COPY deploy/dbiait.ini /etc/uwsgi/apps-available/dbiait.ini