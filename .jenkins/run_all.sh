sh .jenkins/start_kudu.sh
mvn clean install -DskipTests
sh .jenkins/run_kudu_storage_tests.sh

