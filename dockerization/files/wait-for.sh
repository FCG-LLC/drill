#!/bin/bash

HOST=$1
PORT=$2
STARTUP_TIMEOUT=$3

TIMEOUT=${STARTUP_TIMEOUT:-30}
SLEEP_TIME=1

printf "Waiting for ${HOST}:${PORT} to be accessible..."

counter=0
result="1"
while [ "$result" != 200 ]; do
    result=$(curl -s -o /dev/null -w "%{http_code}" ${DRILL_CONFIG_URL}/storage/dfs.json)

    counter=$[$counter + $SLEEP_TIME]
    if [ $counter -gt $TIMEOUT ]; then
           printf " FAIL\n"
           echo "The service ${HOST}:${PORT} is not accessible after ${TIMEOUT} sec timeout. Exiting."
           exit 1
    fi
    [[ "$result" != 200 ]] && sleep $SLEEP_TIME;
    printf "%s" "." ;
done
printf " OK\n"

exit 0
