#!/bin/bash
set -e
set -x

NUM_MAPPERS=${1:-20}
NUM_REDUCERS=${2:-10}

export _JAVA_OPTIONS="-Djava.awt.headless=true -Xmx8192m"

jmeter -JmaxRandomMapId=${NUM_MAPPERS} -JmaxRandomReduceId=${NUM_REDUCERS} -n -t shuffleHandler_v1.jmx
