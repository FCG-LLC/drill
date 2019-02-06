#!/bin/bash

readonly DRILL_CONFIG_URL=$(hostname):8047

chown drill:drill /tmp/drill/ -R

echo "INFO:  configuring DFS storage inside Apache Drill container."
curl -X POST \
	-H "Content-Type: application/json" \
	-d "{\"name\":\"dfs\",\"config\":{\"type\":\"file\",\"enabled\":true,\"connection\":\"file:///\",\"workspaces\":{\"root\":{\"location\":\"/\",\"writable\":false,\"defaultInputFormat\":null},\"tmp\":{\"location\":\"/tmp\",\"writable\":true,\"defaultIn putFormat\":null},\"views\":{\"location\":\"/data\",\"writable\":true,\"defaultInputFormat\":null},\"parquet\":{\"location\":\"/mnt/backup\",\"writable\":true,\"defaultInputFormat\":null}},\"formats\":{\"psv\":{\"type\":\"text\",\"extensions\":[\"tbl\"],\"delimiter\":\"|\"},\"csv\":{\"type\":\"text\",\"extensions\":[\"csv\"],\"delimiter\":\",\"},\"tsv\":{\"type\":\"text\",\"extensions\":[\"tsv\"],\"delimiter\":\"\\t\"},\"parquet\":{\"type\":\"parquet\"},\"json\":{\"type\":\"json\",\"extensions\":[\"json\"]},\"avro\":{\"type\":\"avro\"},\"sequencefile\":{\"type\":\"sequencefile\",\"extensions\":[\"seq\"]},\"csvh\":{\"type\":\"text\",\"extensions\":[\"csvh\"],\"extractHeader\":\"true\",\"delimiter\":\",\"}}}}" \
	${DRILL_CONFIG_URL}/storage/dfs.json

echo "INFO: configuring Kudu storage inside Apache Drill container."
curl -X POST \
	-H "Content-Type: application/json" \
	-d "{\"name\":\"kudu\", \"config\": {\"type\": \"kudu\", \"masterAddresses\": \"${KUDU_MASTER_INT_IP}\", \"operationTimeoutMs\": \"600000\", \"optimizerMaxNonPrimaryKeyAlternatives\": \"20\", \"allUnsignedINT8\": true, \"allUnsignedINT16\": true, \"enabled\": \"true\"}}" \
	${DRILL_CONFIG_URL}/storage/kudu.json

echo "INFO: configuring Netdisco storage inside Apache Drill container."
curl -X POST \
	-H "Content-Type: application/json" \
	-d "{\"name\":\"netdisco\", \"config\": {\"type\": \"jdbc\", \"url\": \"jdbc:postgresql://${NETDISCO_DB_HOST}:${NETDISCO_DB_PORT}/${NETDISCO_DB_NAME}\", \"driver\": \"org.postgresql.Driver\", \"username\": \"${NETDISCO_DB_USER}\", \"password\": \"${NETDISCO_DB_PASS}\", \"enabled\": \"true\"}}" \
	${DRILL_CONFIG_URL}/storage/netdisco.json

echo "INFO: enabling multithreading in Drill queries."
curl -X POST \
	-H "Content-Type: application/json" \
	-d "{\"queryType\":\"SQL\",\"query\":\"ALTER SYSTEM SET planner.slice_target = 1000\"}" \
	${DRILL_CONFIG_URL}/query.json

echo "INFO: enabling fallback for hash aggregations."
curl -X POST \
	-H "Content-Type: application/json" \
	-d "{\"queryType\":\"SQL\",\"query\":\"ALERT SYSTEM SET \`drill.exec.hashagg.fallback.enabled\` = true\"}" \
	${DRILL_CONFIG_URL}/query.json

echo "INFO: Drill container started."
