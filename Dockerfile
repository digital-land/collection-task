# syntax=docker/dockerfile:1

FROM python:3.10-slim-bookworm
WORKDIR /
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y curl git make sqlite3 sudo gdal-bin time libsqlite3-mod-spatialite build-essential

RUN useradd --shell /bin/bash --home-dir /task --create-home task
RUN adduser task sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

COPY . /task
RUN chown task:task -R /task
USER task
WORKDIR /task
ENV PATH="${PATH}:/task/.local/bin"
RUN pip install pyproj
RUN pip install csvkit
RUN pip install awscli
RUN pip install --upgrade pip
RUN pip3 install --upgrade -r requirements.txt

CMD ["./bin/run.sh"]

