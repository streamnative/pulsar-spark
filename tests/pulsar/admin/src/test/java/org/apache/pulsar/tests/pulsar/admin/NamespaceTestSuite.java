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
package org.apache.pulsar.tests.pulsar.admin;

import org.apache.pulsar.tests.common.framework.SystemTestSuite;
import org.apache.pulsar.tests.common.framework.SystemTestSuite.SuiteClasses;
import org.apache.pulsar.tests.pulsar.suites.PulsarServiceTestSuite;
import org.junit.runner.RunWith;

/**
 * A system test suite that tests all namespace related operations.
 */
@RunWith(SystemTestSuite.class)
@SuiteClasses(
    NamespaceOperationsTest.class
)
public class NamespaceTestSuite extends PulsarServiceTestSuite {
}