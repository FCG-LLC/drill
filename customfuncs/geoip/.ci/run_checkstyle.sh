SHA=$(git rev-parse HEAD)
BUILD_URL="${BUILD_URL}violations/#checkstyle"

push_status () {
    sh .ci/post_github_status.sh ${SHA} "checkstyle" $1 ${BUILD_URL} "${2}"
}

push_status pending
if MSG="$(java \
    -jar /checkstyle.jar \
    -c .checkstyle.xml \
    -f xml \
    -o out/checkstyle.xml \
    src/*/java/cs/)
"; then
    push_status success
else
    MSG=`echo ${MSG} | sed -n 's/.*with \([0-9]* errors\).*/\1/p'`
    push_status failure "${MSG}"
fi
