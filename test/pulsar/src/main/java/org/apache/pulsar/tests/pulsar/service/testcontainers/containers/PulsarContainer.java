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

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.common.container.ChaosContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

/**
 * A base container for pulsar components.
 */
@Slf4j
public abstract class PulsarContainer<SelfT extends PulsarContainer<SelfT>> extends ChaosContainer<SelfT> {

    private static final String IMAGE = "apachepulsar/pulsar-all:2.2.1";

    public static final int INVALID_PORT = -1;
    public static final int ZK_PORT = 2181;
    public static final int CS_PORT = 2184;
    public static final int BOOKIE_PORT = 3181;
    public static final int BROKER_PORT = 6650;
    public static final int BROKER_HTTP_PORT = 8080;
    public static final int BROKER_HTTPS_PORT = 8443;
    public static final int BROKER_TLS_PORT = 6651;

    private final String hostname;
    private final String serviceName;
    private final String serviceEntryPoint;
    private final int servicePort;
    private final int tlsServicePort;
    private final String httpPath;
    private final int httpPort;
    private final int httpsPort;

    protected PulsarContainer(String clusterName,
                              String hostname,
                              String serviceName) {
        this(
            clusterName,
            hostname,
            serviceName,
            "bin/pulsar",
            BROKER_PORT,
            INVALID_PORT,
            BROKER_HTTP_PORT,
            INVALID_PORT,
            "/metrics/"
        );
    }

    /**
     * Construct the pulsar container for cluster <tt>clusterName</tt> with the provided <tt>image</tt>.
     *
     * @param clusterName cluster name
     * @param hostname hostname
     * @param serviceName service name
     * @param serviceEntryPoint entry point to run the service
     * @param servicePort pulsar service port
     * @param tlsServicePort pulsar tls service port
     * @param httpPort pulsar http port
     * @param httpsPort pulsar https port
     * @param httpPath http path for health checking
     */
    protected PulsarContainer(String clusterName,
                              String hostname,
                              String serviceName,
                              String serviceEntryPoint,
                              int servicePort,
                              int tlsServicePort,
                              int httpPort,
                              int httpsPort,
                              String httpPath) {
        super(clusterName, IMAGE);
        this.hostname = hostname;
        this.serviceName = serviceName;
        this.serviceEntryPoint = serviceEntryPoint;
        this.servicePort = servicePort;
        this.httpPort = httpPort;
        this.tlsServicePort = tlsServicePort;
        this.httpsPort = httpsPort;
        this.httpPath = httpPath;
    }

    @Override
    public String getContainerName() {
        return clusterName + "-" + hostname;
    }

    @Override
    protected void configure() {
        if (httpPort > 0) {
            addExposedPort(httpPort);
        }
        if (httpsPort > 0) {
            addExposedPort(httpsPort);
        }
        if (servicePort > 0) {
            addExposedPort(servicePort);
        }
        if (tlsServicePort > 0) {
            addExposedPort(tlsServicePort);
        }
    }

    @Override
    public void start() {
        if (httpPort > 0 && servicePort < 0) {
            this.waitStrategy = new HttpWaitStrategy()
                .forPort(httpPort)
                .forStatusCode(200)
                .forPath(httpPath)
                .withStartupTimeout(Duration.of(300, SECONDS));
        } else if (httpPort > 0 || servicePort > 0) {
            this.waitStrategy = new HostPortWaitStrategy()
                .withStartupTimeout(Duration.of(300, SECONDS));
        }
        this.withCreateContainerCmdModifier(createContainerCmd -> {
            createContainerCmd.withHostName(hostname);
            createContainerCmd.withName(getContainerName());
            createContainerCmd.withEntrypoint(serviceEntryPoint);
        });

        super.start();
        log.info("Start pulsar service {} at container {}", serviceName, containerName);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PulsarContainer)) {
            return false;
        }

        PulsarContainer another = (PulsarContainer) o;
        return containerName.equals(another.containerName)
            && super.equals(another);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(containerName);
    }

}
