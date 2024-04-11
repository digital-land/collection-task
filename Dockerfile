# syntax=docker/dockerfile:1

FROM python:3.8-slim-bookworm
WORKDIR /
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y curl git make sqlite3 sudo gdal-bin time libsqlite3-mod-spatialite

RUN useradd --shell /bin/bash --home-dir /collector --create-home collector
RUN adduser collector sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

COPY collector /collector
RUN chown collector:collector -R /collector
USER collector
WORKDIR /collector
ENV PATH="${PATH}:/collector/.local/bin"
RUN pip install pyproj
RUN pip install csvkit
RUN pip install awscli
RUN pip install --upgrade pip
RUN pip3 install --upgrade -r requirements.txt

