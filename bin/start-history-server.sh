#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -e

export THS_HOME="$(cd "`dirname "$0"`"/..; pwd)" # root of the project

. "$THS_HOME"/bin/ths-env.sh

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ `command -v java` ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

THS_LIB_DIR=$THS_HOME/tony-history-server/out/libs

num_jars="$(ls -1 "$THS_LIB_DIR" | grep "^tony-history-server.*.jar$" | wc -l)"
if [ "$num_jars" -eq "0" ]; then
  echo "Failed to find THS jar in $THS_LIB_DIR." 1>&2
  exit 1
fi
THS_JARS="$(ls -1 "$THS_LIB_DIR" | grep "^tony-history-server.*.jar$" || true)"
if [ "$num_jars" -gt "1" ]; then
  echo "Found multiple THS jars in $THS_LIB_DIR:" 1>&2
  echo "$THS_LIB_DIR" 1>&2
  echo "Please remove all but one jar." 1>&2
  exit 1
fi

THS_JAR="${THS_LIB_DIR}/${THS_JARS}"
LAUNCH_CLASSPATH=$THS_CLASSPATH

if [ ! -d "$THS_HOME/log" ]; then
  mkdir $THS_HOME/log
fi

nohup "$RUNNER" -cp "$LAUNCH_CLASSPATH" "com.linkedin.tony.jobhistory.JobHistoryServer" "$@" >"$THS_HOME/log/THS-history-$USER-$HOSTNAME.out" 2>&1 &
sleep 1
