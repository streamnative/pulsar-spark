/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pulsar.tests.pulsar.service;

import org.apache.pulsar.tests.common.framework.ExecResult;

/**
 * Operator to operate a {@link PulsarService}.
 */
public interface PulsarServiceOperator {

    /**
     * Start the node <tt>nodeName</tt> of pulsar <tt>component</tt>.
     *
     * @param component component of the pulsar service
     * @param nodeName node name
     */
    void startNode(PulsarComponent component, String nodeName);

    /**
     * Stop the node <tt>nodeName</tt> of pulsar <tt>component</tt>.
     *
     * @param component component of the pulsar service
     * @param nodeName node name
     */
    void stopNode(PulsarComponent component, String nodeName);

    /**
     * Kill the node <tt>nodeName</tt> of pulsar <tt>component</tt>.
     *
     * @param component component of the pulsar service
     * @param nodeName node name
     */
    void killNode(PulsarComponent component, String nodeName);

    /**
     * Restart the node <tt>nodeName</tt> of pulsar <tt>component</tt>.
     *
     * @param component component of the pulsar service
     * @param nodeName node name
     */
    void restartNode(PulsarComponent component, String nodeName);

    /**
     * Run commandline <tt>commands</tt> at any nodes of <tt>component</tt>.
     *
     * @param component component of the pulsar service
     * @param commands commands to run
     * @return the commandline execution result
     * @throws Exception when fail to execute the commands
     */
    ExecResult execCmd(PulsarComponent component, String... commands) throws Exception;

    /**
     * Run commandline <tt>commands</tt> at a specific node of <tt>component</tt>.
     *
     * @param component component of the pulsar service
     * @param nodeName a specific node of the pulsar component
     * @param commands commands to run
     * @return the commandline execution result
     * @throws Exception when fail to execute the commands
     */
    ExecResult execCmd(PulsarComponent component, String nodeName, String... commands) throws Exception;

}
