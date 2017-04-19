SHA=$(git rev-parse HEAD)
BUILD_URL="${BUILD_URL}testReport"

push_status () {
  sh .jenkins/post_github_status.sh ${SHA} "kudu storage tests" $1 ${BUILD_URL}
}

push_status pending
(
  (cd contrib/storage-kudu/ && mvn test) && push_status success
) || push_status failure
