sh .ci/start_kudu.sh
mvn clean install -DskipTests
sh .ci/run_kudu_storage_tests.sh

