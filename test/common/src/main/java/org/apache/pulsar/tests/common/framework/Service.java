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
package org.apache.pulsar.tests.common.framework;

import java.net.URI;
import java.util.List;

/**
 * Service abstraction for the system test framework.
 *
 * <p>System tests interact with Services. All the configuration, deployment details of a given service are hidden
 * in the actual implementation.
 */
public interface Service {

    /**
     * Start the given service and wait unit the service is up.
     *
     * @throws Exception when fail to start the service.
     */
    void start() throws Exception;

    /**
     * Stop the given service.
     */
    void stop();

    /**
     * Cleanup the resources for a given service.
     */
    void cleanup();

    /**
     * Returns the identifier of the service.
     *
     * @return the identifier of the service.
     */
    String getId();

    /**
     * Check if the service is up and running.
     *
     * @return true if the service is running.
     */
    boolean isRunning();

    /**
     * Return the list of service uris for the clients using this service to interact with.
     *
     * @return the list of service uris exposed by this service.
     */
    List<URI> getServiceUris();

}