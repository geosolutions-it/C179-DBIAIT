FROM  qgis/qgis:final-3_12_3_focal

RUN apt update && \
    apt install -y vim libsasl2-dev python-dev libldap2-dev libssl-dev

WORKDIR /

RUN git clone https://github.com/geosolutions-it/C179-DBIAIT.git

WORKDIR /C179-DBIAIT

RUN git checkout uwsgi_test

RUN pip3 install -r requirements.txt && \
    pip3 install -U uwsgi

CMD  uwsgi --ini app/dbiait.ini
