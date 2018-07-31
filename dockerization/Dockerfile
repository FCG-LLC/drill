FROM docker-registry.cs.int/prod/cs-base-java
ARG destEnv
ARG debVer
MAINTAINER team@cs.com

RUN locale-gen en_US.UTF-8
RUN update-locale LANG=en_US.UTF-8

ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8 

RUN echo "deb http://aptly.cs.int/public xenial $destEnv" >> /etc/apt/sources.list
RUN printf "Package: * \nPin: release a=xenial, o=aptly.cs.int \nPin-Priority: 1600 \n" > /etc/apt/preferences

RUN apt-get update -y
RUN apt-get upgrade -y

RUN apt-get install curl paxctl pax-utils -y

RUN groupadd -g 10007 drill
RUN adduser --system --shell /bin/false --gecos 'drill dedicated user' --uid 10007 --gid 10007 --disabled-password drill

RUN mkdir /data
RUN mkdir -p /drill-scripts
RUN mkdir -p /tmp/drill

RUN chown drill:drill /drill-scripts
RUN apt-get install apache-drill=$debVer
RUN chmod +x /opt/drill/bin/*

RUN chown drill:drill /tmp/drill
RUN chmod 700 /tmp/drill

RUN paxctl -c /usr/lib/jvm/java-8-oracle/jre/bin/java && paxctl -m /usr/lib/jvm/java-8-oracle/jre/bin/java 

COPY files/drill_ddl.sql /
COPY files/test.sql /tmp/
COPY files/docker-entrypoint.sh /

COPY files/version.sh /version.sh
RUN ln -s /version.sh /usr/bin/version

USER drill
CMD ["/docker-entrypoint.sh"]

EXPOSE 8047
