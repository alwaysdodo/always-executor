FROM python:3.9-slim-buster

MAINTAINER lastone9182@gmail.com

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive

RUN set -ex \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends apt-utils \
    && apt-get install -yqq \
        build-essential \
        curl \
        locales \
        libffi-dev \
    && pip install --upgrade setuptools wheel \
    && apt-get autoremove -yqq --purge \
    && apt-get clean

COPY . /tmp
RUN pip install -r /tmp/requirements.txt
