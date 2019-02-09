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

import static org.junit.Assert.assertTrue;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.common.framework.SystemTestRunner.Invokers;
import org.apache.pulsar.tests.common.framework.SystemTestRunner.TestSuiteClass;
import org.apache.pulsar.tests.common.framework.TestInvokers.InvokerType;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * A mock test to run as part of {@link MockServiceTestSuite}.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(MockServiceTestSuite.class)
@Invokers({
    InvokerType.DOCKER
})
@Slf4j
public class MockServiceTestA extends SystemTest<MockService> {

    private final MockService mockService;

    public MockServiceTestA(MockService mockService) {
        this.mockService = mockService;
    }

    @Test
    public void testMockService() {
        log.info("Checking whether mock service is running or not");
        assertTrue(mockService.isRunning());
    }

}
