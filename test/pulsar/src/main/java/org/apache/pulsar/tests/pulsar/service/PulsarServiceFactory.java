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

import org.apache.pulsar.tests.pulsar.service.testcontainers.PulsarStandaloneContainerService;

/**
 * A factory to create {@link PulsarService} based on the provided {@link PulsarDeployment} method.
 */
public class PulsarServiceFactory {

    /**
     * Create a pulsar service with the given service spec.
     *
     * @param deployment pulsar deployment method
     * @param spec service spec
     * @return a pulsar service instance.
     */
    public static PulsarService createPulsarService(PulsarDeployment deployment,
                                                    PulsarServiceSpec spec) {
        switch (deployment) {
            case TESTCONTAINER_STANDALONE:
                return new PulsarStandaloneContainerService(spec);
            default:
                throw new IllegalArgumentException("Unsupported deployment method: " + deployment);
        }
    }

}
