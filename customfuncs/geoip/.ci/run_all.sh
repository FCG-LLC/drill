# code checks
mkdir -p out
sh .ci/run_checkstyle.sh
sh .ci/run_code_analyzer.sh

# unit tests
mvn clean
sh .ci/run_unit_tests.sh
