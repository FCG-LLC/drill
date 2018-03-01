SHA=$(git rev-parse HEAD)
BUILD_URL="${BUILD_URL}testReport"

push_status () {
  sh .ci/post_github_status.sh ${SHA} "custom functions' tests" $1 ${BUILD_URL}
}

push_status pending
(
  (cd customfuncs && mvn test) && push_status success
) || push_status failure
