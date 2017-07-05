FROM ubuntu:14.04
ARG destEnv
MAINTAINER team@cs.com

RUN locale-gen en_US.UTF-8
RUN update-locale LANG=en_US.UTF-8

ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8 

RUN apt-get update -y
RUN apt-get upgrade -y
RUN apt-get install wget -y
RUN echo "deb http://10.12.1.225/public trusty main" >> /etc/apt/sources.list
RUN wget http://10.12.1.225/public/cs-repo.key -O /tmp/cs-repo.key && apt-key add /tmp/cs-repo.key && rm -f /tmp/cs-repo.key
RUN echo "deb http://10.12.1.225/public xenial $destEnv" >> /etc/apt/sources.list
RUN printf "Package: * \nPin: release a=xenial, o=10.12.1.225 \nPin-Priority: 1600 \n" > /etc/apt/preferences


RUN apt-get install curl software-properties-common paxctl pax-utils -y
RUN apt-get install supervisor -y
RUN add-apt-repository ppa:webupd8team/java -y
RUN apt-get update -y
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN apt-get install -y oracle-java8-installer vim -y

RUN groupadd -g 10007 drill
RUN adduser --system --shell /bin/false --gecos 'drill dedicated user' --uid 10007 --gid 10007 --disabled-password drill



RUN mkdir /data
RUN mkdir -p /drill-scripts
RUN mkdir -p /tmp/drill

RUN chown drill:drill /drill-scripts
RUN apt-get install apache-drill
RUN chmod +x /opt/drill/bin/*

# installing postgresql-jdbc driver into Apache Drill
RUN wget https://jdbc.postgresql.org/download/postgresql-9.4.1212.jre6.jar -P /opt/drill/jars/3rdparty/

# install drill-geoip
RUN apt-get install apache-drill-geoip

# install drill-ip-enrichment
RUN apt-get install apache-drill-ip-enrichment

RUN chown drill:drill /tmp/drill
RUN chmod 700 /tmp/drill

RUN paxctl -c /usr/lib/jvm/java-8-oracle/jre/bin/java && paxctl -m /usr/lib/jvm/java-8-oracle/jre/bin/java 

COPY files/drill_ddl.sql /
COPY files/test.sql /tmp/
COPY files/docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

ADD files/conf/supervisor_drill.conf /etc/supervisor/conf.d/

CMD /usr/bin/supervisord -n

EXPOSE 8047
