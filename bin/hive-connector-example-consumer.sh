#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# BookKeeper CLI (experimental)

BINDIR=`dirname "$0"`
PSG_HOME=`cd ${BINDIR}/..;pwd`

source ${PSG_HOME}/bin/common.sh
source ${PSG_HOME}/conf/psgenv.sh

CLI_MODULE_PATH=connectors/hive
CLI_MODULE_NAME="(org.apache.psegment-)?psegment-connectors-hive"
CLI_MODULE_HOME=${PSG_HOME}/${CLI_MODULE_PATH}

# find the module jar
CLI_JAR=$(find_module_jar ${CLI_MODULE_PATH} ${CLI_MODULE_NAME})

# set up the classpath
CLI_CLASSPATH=$(set_module_classpath ${CLI_MODULE_PATH})

DEFAULT_LOG_CONF=${PSG_HOME}/conf/log4j.properties
if [ -z "${CLI_LOG_CONF}" ]; then
  CLI_LOG_CONF=${DEFAULT_LOG_CONF}
fi
CLI_LOG_DIR=${CLI_LOG_DIR:-"$PSG_HOME/logs"}
CLI_LOG_FILE=${CLI_LOG_FILE:-"hive-connector-example-consumer.log"}
CLI_ROOT_LOGGER=${CLI_ROOT_LOGGER:-"INFO,ROLLINGFILE"}

# Configure the classpath
CLI_CLASSPATH="$CLI_JAR:$CLI_CLASSPATH:$CLI_EXTRA_CLASSPATH"
CLI_CLASSPATH="`dirname $CLI_LOG_CONF`:$CLI_CLASSPATH"

# Build the OPTs
PSEGMENT_OPTS=$(build_psegment_opts)
GC_OPTS=$(build_cli_jvm_opts ${CLI_LOG_DIR} "hive-connector-example-consumer.log")
NETTY_OPTS=$(build_netty_opts)
LOGGING_OPTS=$(build_cli_logging_opts ${CLI_LOG_CONF} ${CLI_LOG_DIR} ${CLI_LOG_FILE} ${CLI_ROOT_LOGGER})

OPTS="${OPTS} -cp ${CLI_CLASSPATH} ${PSEGMENT_OPTS} ${GC_OPTS} ${NETTY_OPTS} ${LOGGING_OPTS} ${CLI_EXTRA_OPTS}"

#Change to PSG_HOME to support relative paths
cd "$PSG_HOME"
exec ${JAVA} ${OPTS} org.apache.hive.pulsar.examples.ConsumerMain $@
