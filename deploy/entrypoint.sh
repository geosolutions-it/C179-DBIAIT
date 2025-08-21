#!/bin/bash

# Exit script in case of error
set -e

echo $"\n\n\n"
echo "-----------------------------------------------------"
echo "START ENTRYPOINT"
echo "-----------------------------------------------------"

cmd="$@"

source /opt/conda/etc/profile.d/conda.sh && conda activate dbiait && python /usr/src/dbiait/manage.py collectstatic --no-input
source /opt/conda/etc/profile.d/conda.sh && conda activate dbiait && python /usr/src/dbiait/manage.py migrate

echo $"\n\n\n"
echo "-----------------------------------------------------"
echo "END ENTRYPOINT"
echo "-----------------------------------------------------"

# Run the CMD 
echo "got command $cmd"
exec bash -c "$cmd"