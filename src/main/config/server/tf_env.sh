#!/bin/bash
#
# this script sets the environment for other fascinator scripts
#

# set fascinator home directory
export SERVER_URL="${server.url.base}"
export PROJECT_HOME="${project.home}"
export TF_HOME="$PROJECT_HOME/home"

# java class path
export CLASSPATH="plugins/*:lib/*"

# jvm memory settings
JVM_OPTS="-XX:MaxPermSize=256m -Xmx512m"

# logging directories
export SOLR_LOGS=$TF_HOME/logs/solr
export JETTY_LOGS=$TF_HOME/logs/jetty
if [ ! -d $JETTY_LOGS ]
then
    mkdir -p $JETTY_LOGS
fi
if [ ! -d $SOLR_LOGS ]
then
    mkdir -p $SOLR_LOGS
fi

# use http_proxy if defined
if [ -n "$http_proxy" ]; then
	_TMP=${http_proxy#*//}
	PROXY_HOST=${_TMP%:*}
	_TMP=${http_proxy##*:}
	PROXY_PORT=${_TMP%/}
	echo " * Detected HTTP proxy host:'$PROXY_HOST' port:'$PROXY_PORT'"
	PROXY_OPTS="-Dhttp.proxyHost=$PROXY_HOST -Dhttp.proxyPort=$PROXY_PORT -Dhttp.nonProxyHosts=localhost"
else
	echo " * No HTTP proxy detected"
fi

# jetty settings
JETTY_OPTS="-Djetty.port=${server.port} -Djetty.logs=$JETTY_LOGS -Djetty.home=$PROJECT_HOME/server/jetty"

# solr settings
SOLR_OPTS="-Dsolr.solr.home=$PROJECT_HOME/solr -Djava.util.logging.config.file=$PROJECT_HOME/solr/logging.properties"

# Geonames
GEONAMES="-Dgeonames.solr.home=$PROJECT_HOME/home/geonames/solr"

# directories
CONFIG_DIRS="-Dfascinator.home=$TF_HOME -Dportal.home=$PROJECT_HOME/home/portal -Dstorage.home=$PROJECT_HOME/storage"

# additional settings
EXTRA_OPTS="-Dserver.url.base=${server.url.base} -Damq.port=${amq.port} -Damq.stomp.port=${amq.stomp.port} -Dsmtp.host=${smtp.host} -Dadmin.email=${admin.email}"

# set options for maven to use
export JAVA_OPTS="$JVM_OPTS $JETTY_OPTS $SOLR_OPTS $PROXY_OPTS $CONFIG_DIRS $EXTRA_OPTS $GEONAMES"
