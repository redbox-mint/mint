#!/bin/bash

usage() {
    echo "This script requires the first parameter to be in the input file to ingest."
    echo "NOTE: This input file is expected to be a tab delimited geonames data dump."
    echo "Usage: `basename $0` inputFile.txt"
	exit 1
}

# check script arguments
[ $# -gt 0 ] || usage

# get absolute path of where the script is run from
PROG_DIR=`cd \`dirname $0\`; pwd`
# setup environment
. $PROG_DIR/tf_env.sh

if [ -f $1 ]; then
	INPUT_FILE=$1
    shift
    LOG_FILE=$TF_HOME/logs/geo_harvest.out

    echo "Geonames Solr Index building."
    echo "Logging to: '$LOG_FILE' ..."

    CLASSPATH="$PROJECT_HOME/home/geonames/solr/conf;$CLASSPATH"
    java $JAVA_OPTS -cp $CLASSPATH com.googlecode.solrgeonames.harvester.Harvester $INPUT_FILE > $LOG_FILE 2>&1
fi
