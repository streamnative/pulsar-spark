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
import java.util.Collections;
import java.util.List;

/**
 * A mock implementation of {@link Service}.
 */
public class MockService implements Service {
    @Override
    public void start() throws Exception {
        // no-op
    }

    @Override
    public void stop() {
        // no-op
    }

    @Override
    public void cleanup() {
        // no-op
    }

    @Override
    public String getId() {
        return "mock";
    }

    @Override
    public boolean isRunning() {
        return true;
    }

    @Override
    public List<URI> getServiceUris() {
        return Collections.emptyList();
    }
}
