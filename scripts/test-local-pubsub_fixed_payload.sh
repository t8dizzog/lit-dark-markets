#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${DIR}/.env.local"

echo "Setting EVENT_PAYLOAD var to: "
EVENT_PAYLOAD=$(
  cat ./payloads/test-local-pubsub-fixed-payload.json
)
echo $EVENT_PAYLOAD

curl -X POST \
  -H'Content-type: application/json' \
  -d "${EVENT_PAYLOAD}" \
  "http://localhost:${FUNCTION_PORT_PUBSUB}"

echo
