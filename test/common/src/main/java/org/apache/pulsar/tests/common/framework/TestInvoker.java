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

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

/**
 * A invoker interface to invoke a system test.
 */
public interface TestInvoker {

    /**
     * Start invoking a test method.
     *
     * @param testMethod test method to be invoked.
     * @return a <tt>CompletableFuture</tt> which is completed once the test method invocation is completed.
     */
    CompletableFuture<Void> invokeAsync(Method testMethod);

    /**
     * Stop the test.
     */
    void stop();

}
