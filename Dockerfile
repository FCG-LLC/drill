FROM ubuntu:16.04
ARG destEnv
# config
ENV GITHUB_TOKEN=263f9ede48d798f8d1f92bbefe34f34064ec6f3f

# system dependencies
RUN apt-get update
RUN apt-get install -y \
    openjdk-8-jdk \
    maven \
    git \
    curl \
    wget

# Apache Kudu
RUN cd /etc/apt/sources.list.d && \
    wget -qO - http://archive.cloudera.com/beta/kudu/ubuntu/trusty/amd64/kudu/archive.key | apt-key add - && \
    wget http://archive.cloudera.com/beta/kudu/ubuntu/trusty/amd64/kudu/cloudera.list && \
    echo "deb http://10.12.1.225/public xenial $destEnv" >> /etc/apt/sources.list && \
    printf "Package: * \nPin: release a=xenial, o=10.12.1.225 \nPin-Priority: 1600 \n" > /etc/apt/preferences && \
    apt-get update && \
    apt-get -y dist-upgrade && \
    apt-get -y install kudu kudu-master kudu-tserver libkuduclient0 libkuduclient-dev
EXPOSE 8050 8051 7050 7051

# Project
WORKDIR drill

# Install dependencies
ADD **/**/pom.xml ./
RUN mvn dependency:go-offline

# Build and cache large modules
ADD exec/vector/pom.xml exec/vector/pom.xml
RUN cd exec/vector && mvn dependency:go-offline
ADD exec/java-exec/pom.xml exec/java-exec/pom.xml
RUN cd exec/java-exec && mvn dependency:go-offline
ADD contrib/storage-kudu/pom.xml contrib/storage-kudu/pom.xml
RUN cd contrib/storage-kudu && mvn dependency:go-offline
ADD contrib/storage-hbase/pom.xml contrib/storage-hbase/pom.xml
RUN cd contrib/storage-hbase && mvn dependency:go-offline
ADD contrib/storage-jdbc/pom.xml contrib/storage-jdbc/pom.xml
RUN cd contrib/storage-jdbc && mvn dependency:go-offline
ADD contrib/format-maprdb/pom.xml contrib/format-maprdb/pom.xml
RUN cd contrib/format-maprdb && mvn dependency:go-offline
