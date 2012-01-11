#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
# 
########################################################################################################################

STARTSTOP=$1

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/nephele-config.sh

if [ "$NEPHELE_PID_DIR" = "" ]; then
	NEPHELE_PID_DIR=/tmp
fi

if [ "$NEPHELE_IDENT_STRING" = "" ]; then
	NEPHELE_IDENT_STRING="$USER"
fi

# auxilliary function to construct a lightweight classpath for the
# Nephele TaskManager
constructTaskManagerClassPath() {

	for jarfile in $NEPHELE_LIB_DIR/*.jar ; do

		add=0

		if [[ "$jarfile" =~ 'nephele-server' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-management' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-hdfs' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-s3' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-profiling' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-runtime' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jackson' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-cli' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-logging' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'log4j' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'hadoop-core' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'httpcore' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'httpclient' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-codec' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'aws-java-sdk' ]]; then
			add=1
		fi

		if [[ "$add" = "1" ]]; then
			if [[ $NEPHELE_TM_CLASSPATH = "" ]]; then
				NEPHELE_TM_CLASSPATH=$jarfile;
			else
				NEPHELE_TM_CLASSPATH=$NEPHELE_TM_CLASSPATH:$jarfile
			fi
		fi
	done

	echo $NEPHELE_TM_CLASSPATH
}

NEPHELE_TM_CLASSPATH=$(constructTaskManagerClassPath)

log=$NEPHELE_LOG_DIR/nephele-$NEPHELE_IDENT_STRING-taskmanager-$HOSTNAME.log
out=$NEPHELE_LOG_DIR/nephele-$NEPHELE_IDENT_STRING-taskmanager-$HOSTNAME.out
pid=$NEPHELE_PID_DIR/nephele-$NEPHELE_IDENT_STRING-taskmanager.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file://"$NEPHELE_CONF_DIR"/log4j.properties"

JVM_ARGS="$JVM_ARGS -XX:+UseParNewGC -XX:NewRatio=8 -XX:PretenureSizeThreshold=64m -Xms"$NEPHELE_TM_HEAP"m -Xmx"$NEPHELE_TM_HEAP"m"

case $STARTSTOP in

	(start)
		mkdir -p "$NEPHELE_PID_DIR"
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo Nephele task manager running as process `cat $pid` on host $HOSTNAME.  Stop it first.
				exit 1
     			fi
		fi
		echo starting Nephele task manager on host $HOSTNAME
		$JAVA_HOME/bin/java $JVM_ARGS $NEPHELE_OPTS $log_setting -classpath $NEPHELE_TM_CLASSPATH eu.stratosphere.nephele.taskmanager.TaskManager -configDir $NEPHELE_CONF_DIR > "$out" 2>&1 < /dev/null &
		echo $! > $pid
	;;

	(stop)
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo stopping Nephele task manager on host $HOSTNAME
				kill `cat $pid`
			else
				echo no Nephele task manager to stop on host $HOSTNAME
			fi
		else
			echo no Nephele task manager to stop on host $HOSTNAME
		fi
	;;

	(*)
		echo Please specify start or stop
	;;

esac
