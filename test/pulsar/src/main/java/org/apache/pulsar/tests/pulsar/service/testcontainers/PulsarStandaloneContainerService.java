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
package org.apache.pulsar.tests.pulsar.service.testcontainers;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.pulsar.service.PulsarComponent;
import org.apache.pulsar.tests.pulsar.service.PulsarDeployment;
import org.apache.pulsar.tests.pulsar.service.PulsarServiceSpec;
import org.apache.pulsar.tests.pulsar.service.testcontainers.containers.PulsarContainer;
import org.apache.pulsar.tests.pulsar.service.testcontainers.containers.PulsarStandaloneContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/**
 * A pulsar service that runs pulsar standalone in testcontainers.
 */
@Slf4j
public class PulsarStandaloneContainerService extends PulsarContainerServiceBase {

    private final Network network;
    private final PulsarStandaloneContainer container;

    public PulsarStandaloneContainerService(PulsarServiceSpec spec) {
        super(spec);
        this.network = Network.newNetwork();
        this.container = new PulsarStandaloneContainer(spec.clusterName())
            .withNetwork(network)
            .withNetworkAliases(PulsarStandaloneContainer.NAME + "-" + spec.clusterName());
        if (this.spec.enableContainerLogging()) {
            this.container.withLogConsumer(new Slf4jLogConsumer(log));
        }
    }

    @Override
    protected PulsarContainer getAnyContainer(PulsarComponent component) {
        return container;
    }

    @Override
    protected PulsarContainer getAnyContainer(PulsarComponent component, String containerName) {
        return container;
    }

    @Override
    protected void doStart() throws Exception {
        if (running) {
            throw new IllegalStateException("Standalone container service has already started!");
        }
        // start the standalone container
        container.start();
        running = true;
    }

    @Override
    protected void doStop() {
        if (!running) {
            return;
        }

        // stop the standalone container
        container.stop();
        running = false;
    }

    @Override
    public String getAllowedClusterList() {
        return "standalone";
    }

    @Override
    public String getPlainTextServiceUrl() {
        return container.getPlainTextServiceUrl();
    }

    @Override
    public String getSslServiceUrl() {
        throw new UnsupportedOperationException("Tls is not configured yet");
    }

    @Override
    public String getHttpServiceUrl() {
        return container.getHttpServiceUrl();
    }

    @Override
    public String getHttpsServiceUrl() {
        throw new UnsupportedOperationException("Tls is not configured yet");
    }

    @Override
    public PulsarDeployment deploymentType() {
        return PulsarDeployment.TESTCONTAINER_STANDALONE;
    }

    @Override
    public List<URI> getServiceUris() {
        List<String> serviceUris = new ArrayList<>(2);
        serviceUris.add(getPlainTextServiceUrl());
        serviceUris.add(getHttpServiceUrl());
        return serviceUris
            .stream()
            .map(url -> URI.create(url))
            .collect(Collectors.toList());
    }

    @Override
    public void startNode(PulsarComponent component, String nodeName) {
        throw new UnsupportedOperationException("Standalone doesn't support cluster operator");
    }

    @Override
    public void stopNode(PulsarComponent component, String nodeName) {
        throw new UnsupportedOperationException("Standalone doesn't support cluster operator");
    }

    @Override
    public void killNode(PulsarComponent component, String nodeName) {
        throw new UnsupportedOperationException("Standalone doesn't support cluster operator");
    }

    @Override
    public void restartNode(PulsarComponent component, String nodeName) {
        throw new UnsupportedOperationException("Standalone doesn't support cluster operator");
    }
}
