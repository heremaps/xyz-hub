#!/bin/bash

# Stop Naksha service (only if it was configured)
CODE=0
sudo systemctl list-unit-files --all | grep -Fq "xyz-hub.service" || CODE=1
if [[ $CODE -eq 0 ]]; then
  echo ">> Stopping service [xyz-hub]..."
  sudo systemctl stop xyz-hub
else
  echo ">> Service [xyz-hub] not found. Nothing to stop"
fi
