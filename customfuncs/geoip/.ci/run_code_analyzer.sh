SHA=$(git rev-parse HEAD)
BUILD_URL="${BUILD_URL}violations/#pmd"

push_status () {
  sh .ci/post_github_status.sh ${SHA} "code analyzer" $1 ${BUILD_URL}
}

run_pmd () {
  /pmd/pmd-bin-5.5.3/bin/run.sh pmd \
    -dir src/${1}/java/cs/ \
    -f xml \
    -rulesets .pmd.xml \
    > out/pmd_${1}.xml
}

push_status pending
PMD_MAIN=`run_pmd main; echo $?`
PMD_TEST=`run_pmd test; echo $?`
if expr "${PMD_MAIN}" + "${PMD_TEST}" = 0 > /dev/null; then
  push_status success
else
  push_status failure
fi
