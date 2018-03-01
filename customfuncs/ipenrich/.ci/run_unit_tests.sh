SHA=$(git rev-parse HEAD)
BUILD_URL="${BUILD_URL}testReport"

push_status () {
  sh .ci/post_github_status.sh ${SHA} "unit tests" $1 ${BUILD_URL}
}

push_status pending
(
  mvn test -Dtest=*UnitTest* && push_status success
) || push_status failure
