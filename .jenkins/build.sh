#!/bin/bash

cd $WORKSPACE/source

if test "${branch#*tags/}" != "$branch"; then
	VERSION="target\/apache-drill-${branch#tags/}"
    VERSION_CONTROL="Version: ${branch#tags/}"
else
	SHORT_COMMIT=`expr substr $GIT_COMMIT 1 7`
	VERSION="target\/apache-drill-\${project.version\}-\${maven.build.timestamp\}-$SHORT_COMMIT-$destEnv"
    VERSION_CONTROL="Version: [[project.version]]-[[buildTimestamp]]-$SHORT_COMMIT-$destEnv"   
fi

sed -i "s/Version.*/$VERSION_CONTROL/" distribution/src/deb/control/control
sed -i "s/<deb.*deb>/<deb>$VERSION.deb<\/deb>/" distribution/pom.xml
mvn -e -DskipTests -P deb install
cd target
DRILL_DEB=`ls | grep drill | grep deb`
APTLY_SERVER=http://10.12.1.225:8080
curl -X POST -F file=@$DRILL_DEB http://10.12.1.225:8080/api/files/$DRILL_DEB
curl -X POST http://10.12.1.225:8080/api/repos/main/file/$DRILL_DEB
ssh -tt -i ~/.ssh/aptly_rsa aptly@10.12.1.225

echo version="$VERSION" > env.properties

cd $WORKSPACE/source
cd dockerization
docker build --build-arg destEnv=$destEnv --no-cache -t cs/$app .
docker tag cs/$app portus.cs.int:5000/$destEnv/cs-$app
docker push portus.cs.int:5000/$destEnv/cs-$app
