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

import com.github.dockerjava.api.DockerClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.common.container.DockerUtils;
import org.apache.pulsar.tests.common.framework.ExecResult;
import org.apache.pulsar.tests.pulsar.service.PulsarComponent;
import org.apache.pulsar.tests.pulsar.service.PulsarService;
import org.apache.pulsar.tests.pulsar.service.PulsarServiceSpec;
import org.apache.pulsar.tests.pulsar.service.testcontainers.containers.PulsarContainer;

/**
 * The abstract implementation for pulsar container service using testcontainers.
 */
@Slf4j
public abstract class PulsarContainerServiceBase implements PulsarService {

    protected final PulsarServiceSpec spec;
    protected volatile boolean running = false;

    protected PulsarContainerServiceBase(PulsarServiceSpec spec) {
        this.spec = spec;
    }

    protected abstract PulsarContainer getAnyContainer(PulsarComponent component);

    protected abstract PulsarContainer getAnyContainer(PulsarComponent component, String containerName);

    @Override
    public String getClusterName() {
        return spec.clusterName();
    }

    @Override
    public ExecResult execCmd(PulsarComponent component, String... commands) throws Exception {
        PulsarContainer container = getAnyContainer(component);
        DockerClient client = container.getDockerClient();
        String dockerId = container.getContainerId();
        return DockerUtils.runCommand(
            client, dockerId, true, commands);
    }

    @Override
    public ExecResult execCmd(PulsarComponent component, String nodeName, String... commands) throws Exception {
        PulsarContainer container = getAnyContainer(component, nodeName);
        DockerClient client = container.getDockerClient();
        String dockerId = container.getContainerId();
        return DockerUtils.runCommand(
                client, dockerId, true, commands);
    }

    @Override
    public void start() throws Exception {
        if (running) {
            return;
        }
        doStart();
    }

    protected abstract void doStart() throws Exception;

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        doStop();
    }

    protected abstract void doStop();

    @Override
    public void cleanup() {
        // no-op for container service. testcontainers manages it.
    }

    @Override
    public String getId() {
        return spec.clusterName();
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
