FROM portus.cs.int:5000/dev/cs-base-java

# config
ENV GITHUB_TOKEN=263f9ede48d798f8d1f92bbefe34f34064ec6f3f

# system dependencies
RUN apt-get update
RUN apt-get install -y \
    maven \
    git \
    curl \
    unzip

# checkstyle
RUN curl http://downloads.sourceforge.net/project/checkstyle/checkstyle/7.5.1/checkstyle-7.5.1-all.jar \
    --output checkstyle.jar \
    --location \
    --silent \
    --show-error

# code analyzer
RUN curl https://github.com/pmd/pmd/releases/download/pmd_releases%2F5.5.3/pmd-bin-5.5.3.zip \
    --output pmd.zip \
    --location \
    --silent \
    --show-error
RUN unzip pmd.zip -d pmd

# Project files
ADD pom.xml pom.xml
RUN mvn dependency:resolve
