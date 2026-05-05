#!/usr/bin/env bash
set -euo pipefail

clojure -Tmcp start \
  :start-nrepl-cmd '["lein" "start-dev"]' \
  :config-profile :cli-assist
