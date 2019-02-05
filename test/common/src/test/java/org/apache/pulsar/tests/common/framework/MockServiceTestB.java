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

import static org.junit.Assert.assertEquals;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.common.framework.SystemTestRunner.TestSuiteClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * A mock test to run as part of {@link MockServiceTestSuite}.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(MockServiceTestSuite.class)
@Slf4j
public class MockServiceTestB extends SystemTest<MockService> {

    private final MockService mockService;

    public MockServiceTestB(MockService mockService) {
        this.mockService = mockService;
    }

    @Test
    public void testMockService() {
        log.info("getting service id");
        assertEquals("mock", mockService.getId());
    }

}
