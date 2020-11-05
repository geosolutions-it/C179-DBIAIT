FROM  qgis/qgis:final-3_12_3_focal
  
ENV DATABASE_USER="postgres"
ENV DATABASE_NAME="dbiait"
ENV DATABASE_PASSWORD="j05hhome"
ENV DATABASE_HOST="64.225.103.232"
ENV QT_QPA_PLATFORM="offscreen"

RUN apt update && \
    apt install -y vim libsasl2-dev python-dev libldap2-dev libssl-dev

# RUN apt-get install net-tools

# WORKDIR /

# RUN git clone https://github.com/geosolutions-it/C179-DBIAIT.git

WORKDIR /C179-DBIAIT

ADD . /C179-DBIAIT

# RUN git checkout uwsgi_test

RUN pip3 install -r requirements.txt
# RUN pip3 install python-opencv-headless
# RUN pip3 install opencv-python==4.1.2.30  

EXPOSE 9191 8000

CMD ["sh", "run.sh"]