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

import org.apache.pulsar.tests.common.framework.Service;

/**
 * A Pulsar Service.
 */
public interface PulsarService extends Service, PulsarServiceOperator {

    /**
     * Get cluster name of the pulsar service.
     *
     * @return cluster name of the pulsar service.
     */
    String getClusterName();

    /**
     * Get the list of allowed clusters.
     *
     * @return the list of allowed clusters.
     */
    String getAllowedClusterList();

    /**
     * Return the plain text service url.
     *
     * @return the plain text service url.
     */
    String getPlainTextServiceUrl();

    /**
     * Return the ssl service url.
     *
     * @return the ssl service url.
     */
    String getSslServiceUrl();

    /**
     * Return the http service url.
     *
     * @return the http service url.
     */
    String getHttpServiceUrl();

    /**
     * Return the https service url.
     *
     * @return the https service url.
     */
    String getHttpsServiceUrl();

    /**
     * The deployment type of this pulsar service.
     *
     * @return deployment type.
     */
    PulsarDeployment deploymentType();

}
