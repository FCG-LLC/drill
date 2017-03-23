#!/bin/bash -e
/opt/drill/bin/drillbit.sh start 
#curl -X POST -H "Content-Type: application/json" -d '{"name":"kudu", "config": {"type": "kudu", "masterAddresses": "10.12.1.143", "eabled": "true"}}' http://10.12.1.90:8047/storage/kudu.json

#/opt/drill/apache-drill-1.5.0-SNAPSHOT/bin/drill-localhost -f /drill_ddl.sql
exec "$@"

