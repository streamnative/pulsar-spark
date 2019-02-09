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

import org.apache.pulsar.tests.common.framework.invokers.docker.DockerInvoker;

/**
 * A central place to manage all available test invokers.
 */
public class TestInvokers {

    /**
     * Invoker type.
     */
    public enum InvokerType {
        LOCAL,
        DOCKER
    }

    /**
     * Retrieve a given test invoker to invoke system test.
     *
     * @param invokerType invoker type.
     * @return test invoker
     */
    public TestInvoker getTestInvoker(InvokerType invokerType) {
        switch (invokerType) {
            case DOCKER:
                return new DockerInvoker();
            default:
                throw new IllegalArgumentException("Unsupported invoker type : " + invokerType);
        }
    }

}
