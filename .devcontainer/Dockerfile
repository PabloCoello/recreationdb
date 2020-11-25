FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
                git \
                cmake \
                software-properties-common \
                python3.8 \
                python3-pip \
                libpq-dev \
                libspatialindex-dev

RUN python3 -m pip install pymongo \
                        Rtree \
                        psycopg2 \
                        flickrapi \
                        pandas \
                        geopandas \
                        tqdm \
                        datetime \
                        paramiko \
                        shapely

RUN mkdir -p /local/lib/python3.8/dist-packages/

ENV PYTHONPATH=/local/lib/python3.8/dist-packages/

WORKDIR /opt/recreationdb/