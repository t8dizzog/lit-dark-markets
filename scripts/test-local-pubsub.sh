#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${DIR}/.env.local"

RAW_MESSAGE=$1
MESSAGE_BASE64=$(echo -n ${RAW_MESSAGE} | base64)

echo "Setting EVENT_PAYLOAD var to: "
EVENT_PAYLOAD=$(
  sed -e \
    "s|__DATA_BASE64_PLACEHOLDER__|${MESSAGE_BASE64}|g" \
    ./payloads/test-local-pubsub-payload.json
)
echo $EVENT_PAYLOAD

curl -X POST \
  -H'Content-type: application/json' \
  -d "${EVENT_PAYLOAD}" \
  "http://localhost:${FUNCTION_PORT_PUBSUB}"

echo
