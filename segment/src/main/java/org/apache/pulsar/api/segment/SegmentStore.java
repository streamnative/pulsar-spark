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
package org.apache.pulsar.api.segment;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.ReadHandle;

/**
 * Segment provider to access segment.
 */
public interface SegmentStore extends AutoCloseable {

    /**
     * Open the random access entry reader for a given <i>segment</i>.
     *
     * @param segment segment to open to read
     * @return an instance of random access entry reader to read entries
     */
    CompletableFuture<ReadHandle> openRandomAccessEntryReader(Segment segment);

    /**
     * Open a sequential entry reader to read entries from a given <i>segment</i> sequentially.
     *
     * @param segment segment to open to read
     * @param startEntryId the entry id to start reading
     * @param endEntryId the entry id to stop reading
     * @param config read ahead config
     * @return
     */
    CompletableFuture<SegmentEntryReader> openSequentialEntryReader(Segment segment,
                                                                    long startEntryId,
                                                                    Optional<Long> endEntryId,
                                                                    ReadAheadConfig config);

    @Override
    void close();

}
