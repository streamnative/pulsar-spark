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
package org.apache.pulsar.tests.pulsar.service.testcontainers.containers;

/**
 * A Pulsar Container that runs standalone.
 */
public class PulsarStandaloneContainer extends PulsarContainer<PulsarStandaloneContainer> {

    public static final String NAME = "standalone";

    /**
     * Construct the pulsar container for cluster <tt>clusterName</tt> with the provided <tt>image</tt>.
     *
     * @param clusterName       cluster name
     */
    public PulsarStandaloneContainer(String clusterName) {
        super(
            clusterName,
            NAME,
            NAME + "-" + clusterName
        );
    }

    @Override
    protected void configure() {
        super.configure();
        setCommand("standalone --no-stream-storage");
    }

    public String getPlainTextServiceUrl() {
        return "pulsar://" + getContainerIpAddress() + ":" + getMappedPort(BROKER_PORT);
    }

    public String getHttpServiceUrl() {
        return "http://" + getContainerIpAddress() + ":" + getMappedPort(BROKER_HTTP_PORT);
    }
}
