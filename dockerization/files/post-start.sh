#!/bin/bash

function post_start(){
	local drill_config_url=${DRILL_CONFIG_URL}
	local kudu_master_int_ip=${KUDU_MASTER_INT_IP}

	docker exec drill chown drill:drill /tmp/drill/ -R
	echo "INFO:  configuring DFS storage inside Apache Drill container."
	docker exec drill \
		curl -X POST \
			-H "Content-Type: application/json" \
			-d "{\"name\":\"dfs\",\"config\":{\"type\":\"file\",\"enabled\":true,\"connection\":\"file:///\",\"workspaces\":{\"root\":{\"location\":\"/\",\"writable\":false,\"defaultInputFormat\":null},\"tmp\":{\"location\":\"/tmp\",\"writable\":true,\"defaultIn putFormat\":null},\"views\":{\"location\":\"/data\",\"writable\":true,\"defaultInputFormat\":null},\"parquet\":{\"location\":\"/mnt/backup\",\"writable\":true,\"defaultInputFormat\":null}},\"formats\":{\"psv\":{\"type\":\"text\",\"extensions\":[\"tbl\"],\"delimiter\":\"|\"},\"csv\":{\"type\":\"text\",\"extensions\":[\"csv\"],\"delimiter\":\",\"},\"tsv\":{\"type\":\"text\",\"extensions\":[\"tsv\"],\"delimiter\":\"\\t\"},\"parquet\":{\"type\":\"parquet\"},\"json\":{\"type\":\"json\",\"extensions\":[\"json\"]},\"avro\":{\"type\":\"avro\"},\"sequencefile\":{\"type\":\"sequencefile\",\"extensions\":[\"seq\"]},\"csvh\":{\"type\":\"text\",\"extensions\":[\"csvh\"],\"extractHeader\":\"true\",\"delimiter\":\",\"}}}}" \
			${drill_config_url}/storage/dfs.json

	echo "INFO: configuring Kudu storage inside Apache Drill container."
	docker exec drill \
		curl -X POST \
			-H "Content-Type: application/json" \
			-d "{\"name\":\"kudu\", \"config\": {\"type\": \"kudu\", \"masterAddresses\": \"${kudu_master_int_ip}\", \"operationTimeoutMs\": \"600000\", \"optimizerMaxNonPrimaryKeyAlternatives\": \"20\", \"allUnsignedINT8\": true, \"allUnsignedINT16\": true, \"enabled\": \"true\"}}" \
			${drill_config_url}/storage/kudu.json

	source /etc/collective-sense/csprov.drill.conf
	echo "INFO: configuring Netdisco storage inside Apache Drill container."
	docker exec drill \
		curl -X POST \
			-H "Content-Type: application/json" \
			-d "{\"name\":\"netdisco\", \"config\": {\"type\": \"jdbc\", \"url\": \"jdbc:postgresql://${NETDISCO_DB_HOST}:${NETDISCO_DB_PORT}/${NETDISCO_DB_NAME}\", \"driver\": \"org.postgresql.Driver\", \"username\": \"${NETDISCO_DB_USER}\", \"password\": \"${NETDISCO_DB_PASS}\", \"enabled\": \"true\"}}" \
			${drill_config_url}/storage/netdisco.json

	echo "INFO: enabling multithreading in Drill queries."
	docker exec drill \
		curl -X POST \
			-H "Content-Type: application/json" \
			-d "{\"queryType\":\"SQL\",\"query\":\"ALTER SYSTEM SET planner.slice_target = 1000\"}" \
			${drill_config_url}/query.json

	echo "INFO: enabling fallback for hash aggregations."
	docker exec drill \
		curl -X POST \
		-H "Content-Type: application/json" \
		-d "{\"queryType\":\"SQL\",\"query\":\"ALERT SYSTEM SET \`drill.exec.hashagg.fallback.enabled\` = true\"}" \
		${drill_config_url}/query.json

	echo "INFO: Drill container started."
}

post_start
