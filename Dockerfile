FROM python:3.8.6-slim-buster AS base

RUN apt-get update -qy
RUN apt-get install -qy build-essential
RUN apt-get install -qy cmake
RUN apt-get install -qy git

WORKDIR inbulk
COPY . .

RUN pip install -e .
