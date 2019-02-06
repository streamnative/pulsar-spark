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

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Specification of a pulsar service cluster.
 */
@Builder
@Accessors(fluent = true)
@Getter
@Setter
public class PulsarServiceSpec {

    /**
     * Returns the cluster name.
     *
     * @return the cluster name.
     */
    String clusterName;

    /**
     * Returns number of bookies.
     *
     * @return number of bookies.
     */
    @Default
    int numBookies = 3;

    /**
     * Returns number of brokers.
     *
     * @return number of brokers.
     */
    @Default
    int numBrokers = 2;

    /**
     * Returns number of proxies.
     *
     * @return number of proxies.
     */
    @Default
    int numProxies = 1;

    /**
     * Returns number of workers.
     *
     * @return number of workers.
     */
    @Default
    int numWorkers = 0;

    /**
     * Returns is container logging enabled.
     *
     * @return is container logging enabled.
     */
    @Default
    boolean enableContainerLogging = true;

    /**
     * Returns is tls enabled for brokers.
     *
     * @return is container tls enabled.
     */
    @Default
    boolean enableTLS = false;

}
