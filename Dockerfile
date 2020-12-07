FROM python:3.8.6-slim-buster AS base

RUN apt-get update -qy
RUN apt-get install -qy build-essential
RUN apt-get install -qy cmake
RUN apt-get install -qy git

WORKDIR inbulk
COPY . .

FROM base AS test

RUN pip install -e .[test]

FROM base

RUN pip install -e .
