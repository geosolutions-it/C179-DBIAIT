# Use the official Ubuntu 22.04 base image
FROM ubuntu:22.04

# Set environment variables for non-interactive installation
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary packages: wget for downloading, bzip2 for decompressing
# and ca-certificates for secure connections.
# Clean up apt lists to reduce image size.
RUN apt-get update -y && apt-get install wget gdal-bin git vim \
    gcc libsasl2-dev python-dev-is-python3 libldap2-dev curl \
    libssl-dev ffmpeg libsm6 libxext6 exiv2 -y && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory to /tmp for downloading the installer
WORKDIR /tmp

# Download the Miniconda installer
# -O miniconda.sh saves the downloaded file as miniconda.sh
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh -q

# Install Miniconda silently to /opt/conda
# -b for batch mode (no user interaction)
# -p for prefix path (installation directory)
RUN bash miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh

# Add Miniconda to the PATH environment variable
# This makes 'conda' command available globally in the container
ENV PATH="/opt/conda/bin:$PATH"
COPY . /usr/src/dbiait/
COPY environment.yml /usr/src/dbiait/environment.yml

RUN conda env create -f /usr/src/dbiait/environment.yml
WORKDIR /usr/src/dbiait/

RUN mkdir /var/log/gunicorn/ -p