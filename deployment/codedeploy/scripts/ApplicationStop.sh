#!/bin/bash

# Stop Naksha service (only if it was configured)
CODE=0
sudo systemctl list-unit-files --all | grep -Fq "naksha-hub.service" || CODE=1
if [[ $CODE -eq 0 ]]; then
  echo ">> Stopping service [naksha-hub]..."
  sudo systemctl stop naksha-hub
else
  echo ">> Service [naksha-hub] not found. Nothing to stop"
fi
