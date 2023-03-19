# Launch pyspark notebook

FROM        jupyter/pyspark-notebook:latest

USER root

ENV JUPYTER_ENABLE_LAB=yes

EXPOSE 8888

COPY ./seek_interview.ipynb ./
