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
package org.apache.bookkeeper.api.segment;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;

@Builder
@Accessors(fluent = true)
@Getter
public class ReadAheadConfig {

    /**
     * Number of entries to prefetch for each call.
     */
    private final int numPrefetchEntries = 3;

    /**
     * Max number of entries to prefetch.
     */
    private final int maxPrefetchEntries = 1000;

    /**
     * Wait time when the read ahead cache is full.
     */
    private final int readAheadWaitTime = 200;

    /**
     * The long-poll timeout milliseconds of reading lac.
     */
    private final long readLacTimeoutMs = 10000;

}
