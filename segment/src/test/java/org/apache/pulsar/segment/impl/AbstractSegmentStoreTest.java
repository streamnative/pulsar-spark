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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.api.segment.ReadAheadConfig;
import org.apache.pulsar.api.segment.Segment;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link AbstractSegmentStore}.
 */
public class AbstractSegmentStoreTest {

    private AbstractSegmentStore store;

    @Before
    public void setup() {
        store = mock(AbstractSegmentStore.class, CALLS_REAL_METHODS);
    }

    @Test
    public void testOpenSequentialEntryReaderWithoutEndEntryId() throws Exception {
        long lastEntryId = 123456L;
        ReadHandle mockRh = mock(ReadHandle.class);
        when(mockRh.getLastAddConfirmed()).thenReturn(lastEntryId);
        OrderedScheduler scheduler = mock(OrderedScheduler.class);
        when(store.openRandomAccessEntryReader(any(Segment.class)))
            .thenReturn(FutureUtils.value(mockRh));
        when(store.scheduler()).thenReturn(scheduler);

        Segment segment = mock(Segment.class);
        ReadAheadConfig config = ReadAheadConfig.builder().build();
        SegmentEntryReaderImplWithEndEntryId reader =
            (SegmentEntryReaderImplWithEndEntryId) result(store.openSequentialEntryReader(
                segment,
                0L,
                Optional.empty(),
                config
            ));

        assertEquals(lastEntryId, reader.getEndEntryId());
    }

    @Test
    public void testOpenSequentialEntryReaderWithSmallerEndEntryId() throws Exception {
        long lastEntryId = 123456L;
        ReadHandle mockRh = mock(ReadHandle.class);
        when(mockRh.getLastAddConfirmed()).thenReturn(lastEntryId);
        OrderedScheduler scheduler = mock(OrderedScheduler.class);
        when(store.openRandomAccessEntryReader(any(Segment.class)))
            .thenReturn(FutureUtils.value(mockRh));
        when(store.scheduler()).thenReturn(scheduler);

        Segment segment = mock(Segment.class);
        ReadAheadConfig config = ReadAheadConfig.builder().build();
        SegmentEntryReaderImplWithEndEntryId reader =
            (SegmentEntryReaderImplWithEndEntryId) result(store.openSequentialEntryReader(
                segment,
                0L,
                Optional.of(lastEntryId / 2),
                config
            ));

        assertEquals(lastEntryId / 2, reader.getEndEntryId());
    }

    @Test
    public void testOpenSequentialEntryReaderWithLargerEndEntryId() throws Exception {
        long lastEntryId = 123456L;
        ReadHandle mockRh = mock(ReadHandle.class);
        when(mockRh.getLastAddConfirmed()).thenReturn(lastEntryId);
        OrderedScheduler scheduler = mock(OrderedScheduler.class);
        when(store.openRandomAccessEntryReader(any(Segment.class)))
            .thenReturn(FutureUtils.value(mockRh));
        when(store.scheduler()).thenReturn(scheduler);

        Segment segment = mock(Segment.class);
        ReadAheadConfig config = ReadAheadConfig.builder().build();
        SegmentEntryReaderImplWithEndEntryId reader =
            (SegmentEntryReaderImplWithEndEntryId) result(store.openSequentialEntryReader(
                segment,
                0L,
                Optional.of(lastEntryId * 2),
                config
            ));

        assertEquals(lastEntryId, reader.getEndEntryId());
    }

}
