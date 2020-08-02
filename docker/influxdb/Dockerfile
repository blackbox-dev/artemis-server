FROM ubuntu:18.04

RUN apt-get update && apt-get upgrade --yes && apt-get --yes install \
    apt-utils \
    curl \
    gnupg \
    wget

RUN apt update
RUN apt install influxdb
