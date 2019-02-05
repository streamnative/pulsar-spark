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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.common.framework.SystemTestSuite.ServiceClass;
import org.apache.pulsar.tests.common.framework.SystemTestSuite.ServiceProvider;
import org.apache.pulsar.tests.common.framework.SystemTestSuite.SuiteClasses;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

/**
 * A test suite for mock service.
 */
@RunWith(SystemTestSuite.class)
@SuiteClasses({
    MockServiceTestA.class,
    MockServiceTestB.class
})
@ServiceClass(MockService.class)
@Slf4j
public class MockServiceTestSuite {

    private static MockService service;

    @BeforeClass
    public static void setupService() throws Exception {
        log.info("Setting up service");
        service = new MockService();
        service.start();
    }

    @ServiceProvider
    public static MockService mockService() {
        return service;
    }

    @AfterClass
    public static void teardownService() {
        if (null != service) {
            log.info("Tearing down service");
            service.stop();
        }
    }

}
