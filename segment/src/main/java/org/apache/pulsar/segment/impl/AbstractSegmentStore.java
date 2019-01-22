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
package org.apache.pulsar.segment.impl;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.api.segment.ReadAheadConfig;
import org.apache.pulsar.api.segment.Segment;
import org.apache.pulsar.api.segment.SegmentEntryReader;
import org.apache.pulsar.api.segment.SegmentStore;

/**
 * The basic implementation of {@link SegmentStore}.
 */
public abstract class AbstractSegmentStore implements SegmentStore {

    /**
     * Return the ordered scheduler for opening sequential entry reader.
     *
     * @return the ordered scheduler used for opening sequential entry reader
     */
    protected abstract OrderedScheduler scheduler();

    @Override
    public CompletableFuture<SegmentEntryReader> openSequentialEntryReader(Segment segment,
                                                                           long startEntryId,
                                                                           Optional<Long> endEntryId,
                                                                           ReadAheadConfig config) {
        return openRandomAccessEntryReader(segment)
            .thenApply(lh -> new SegmentEntryReaderImpl(
                segment,
                lh,
                startEntryId,
                this,
                scheduler(),
                config.numPrefetchEntries(),
                config.maxPrefetchEntries(),
                config.readAheadWaitTime(),
                config.readLacTimeoutMs()
            ))
            .thenApply(reader -> new SegmentEntryReaderImplWithEndEntryId(
                reader,
                Math.min(endEntryId.orElse(Long.MAX_VALUE), reader.getLh().getLastAddConfirmed())
            ));
    }

}
