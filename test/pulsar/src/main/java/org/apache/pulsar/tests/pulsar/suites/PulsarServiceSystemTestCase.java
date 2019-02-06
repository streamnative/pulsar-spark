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
package org.apache.pulsar.tests.pulsar.suites;

import org.apache.pulsar.tests.common.framework.SystemTest;
import org.apache.pulsar.tests.pulsar.service.PulsarService;

/**
 * A {@link PulsarService} system test case that runs as part of {@link PulsarServiceTestSuite}.
 */
public abstract class PulsarServiceSystemTestCase extends SystemTest<PulsarService> {

    protected final PulsarService pulsarService;

    public PulsarServiceSystemTestCase(PulsarService service) {
        this.pulsarService = service;
    }

    protected PulsarService pulsarService() {
        return pulsarService;
    }

}
