SHA=$1
CONTEXT=$2
STATE=$3
TARGET_URL=$4
DESCRIPTION=$5

curl -XPOST -H "Authorization: token ${GITHUB_TOKEN}" \
    https://api.github.com/repos/FCG-LLC/drill-geoip/statuses/${SHA} -d "{
      \"state\": \"${STATE}\",
      \"target_url\": \"${TARGET_URL}\",
      \"description\": \"${DESCRIPTION}\",
      \"context\": \"${CONTEXT}\"
    }" \
    --silent \
    --show-error
