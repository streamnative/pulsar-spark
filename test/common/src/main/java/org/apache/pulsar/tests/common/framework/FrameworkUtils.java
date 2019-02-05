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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.common.framework.TestInvokers.InvokerType;
import org.testcontainers.shaded.com.google.common.base.Stopwatch;

/**
 * Utils used in the system test framework.
 */
@Slf4j
public final class FrameworkUtils {

    static final String SKIP_SERVICE_INITIALIZATION = "skipServiceInitialization";
    static final String SYSTEM_TEST_INVOKER = "systemTestInvoker";

    private FrameworkUtils() {}

    /**
     * Retrieve a configuration setting either from system env or system properties.
     *
     * @param key configuration key
     * @param defaultValue default value
     * @return the configuration value
     */
    public static String getConfig(String key, String defaultValue) {
        return System.getenv().getOrDefault(key, System.getProperty(key, defaultValue));
    }

    /**
     * Helper method to check if <tt>skipServiceInitialization</tt> enabled or not.
     *
     * <p>The flag indicates if the system test framework should skip initializing services before running
     * system tests. If it is set to true, all the system tests will skip initializing services and reuse
     * already deployed services. If it is set to false, the system test runner will deploy the service
     * using the annotated static method <tt>@Environment</tt> before running system tests.
     *
     * @return true if skip initializing service, otherwise false.
     */
    public static boolean isSkipServiceInitializationEnabled() {
        return Boolean.parseBoolean(getConfig(SKIP_SERVICE_INITIALIZATION, "false").trim());
    }

    /**
     * Helper method to retrieve the system test invoker.
     *
     * @return the system test invoker.
     */
    public static InvokerType getSystemTestInvoker() {
        return InvokerType.valueOf(getConfig(SYSTEM_TEST_INVOKER, InvokerType.LOCAL.name()));
    }

    /**
     * Generate a randomized string.
     *
     * @param numChars number characters to generate
     * @return a randomized string
     */
    public static String randomName(int numChars) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numChars; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }

    public static void waitUntil(Predicate<Void> predicate,
                                 String waitMessage,
                                 long waitTime,
                                 TimeUnit timeUnit)
            throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        while (!predicate.test(null)) {
            timeUnit.sleep(waitTime);
            log.info("{} ms elapsed, still waiting : {}", sw.elapsed(TimeUnit.MILLISECONDS), waitMessage);
        }
    }

}
