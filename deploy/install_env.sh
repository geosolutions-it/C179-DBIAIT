#!/bin/bash

# Exit script in case of error
set -e
# this sh file will setup the environment for minicornda and all the requirements needed to run the application
. ~/miniconda3/bin/activate

if conda info --envs | grep -q dbiait; then
    echo "env found"
    conda activate dbiait
else 
    echo "env not found, installing qgis"
    conda init --all
    conda create -n dbiait -c conda-forge python=3.6 -y && conda activate dbiait
    conda install -c conda-forge qgis=3.16 -y
fi
# installing application requirements file
. ~/miniconda3/bin/activate
conda activate dbiait
cd /usr/src
pip install -r requirements.txt
conda install -c conda-forge uwsgi
