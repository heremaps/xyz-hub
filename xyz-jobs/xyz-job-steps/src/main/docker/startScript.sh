#!/bin/bash
#set -x 
##
# Usage:
#   ./startScript.sh <InputSpaceListFile>
##

STARTED=$(date -u +"%Y%m%d-%H%M%S")
printf "### Start %s\n" $STARTED

#Get command-line args
inputFile="${1}"

FileOK="$(basename $inputFile).${STARTED}.ok.log"
FileERR="$(basename $inputFile).${STARTED}.err.log"


RED='\e[0;31m'
NC='\e[0m' # No Color

log () {
  text="$(date -u) $1"
  color="$2"
  if [ ! -z "$color" ]; then
    echo -e "${color}${text}${NC}"
  else
    echo "$text"
  fi
}

err () {
  log "$1" "$RED" >&2
}

echo "$(printf "%s\n" "Some Ok Data")" >> $FileOK
echo "$(printf "%s\n" "Some Err Data")" >> $FileERR

log "Some Log Messages"
err "Some Err Messages"

printf "### End   %s\n" $(date -u +"%Y%m%d-%H%M%S")
