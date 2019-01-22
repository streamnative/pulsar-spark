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
package org.apache.pulsar.segment.pulsar;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.pulsar.api.segment.Segment;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link PulsarSegmentStore}.
 */
public class PulsarSegmentStoreTest {

    private static final DigestType digestType = DigestType.CRC32C;
    private static final byte[] password = "test-password".getBytes(UTF_8);

    private BookKeeper mockBk;
    private PulsarSegmentStore store;

    @Before
    public void setup() {
        this.mockBk = mock(BookKeeper.class);
        this.store = new PulsarSegmentStore(mockBk, digestType, password);
    }

    @After
    public void teardown() throws Exception {
        this.store.close();
        verify(mockBk, times(1)).close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOpenInvalidSegment() throws Exception {
        Segment invalidSegment = mock(Segment.class);
        result(store.openRandomAccessEntryReader(invalidSegment));
    }

    @Test
    public void testOpenValidSegment() throws Exception {
        final long ledgerId = System.currentTimeMillis();

        ReadHandle mockRh = mock(ReadHandle.class);
        OpenBuilder mockOpenBuilder = mock(OpenBuilder.class);
        when(mockOpenBuilder.withLedgerId(eq(ledgerId))).thenReturn(mockOpenBuilder);
        when(mockOpenBuilder.withDigestType(eq(digestType))).thenReturn(mockOpenBuilder);
        when(mockOpenBuilder.withPassword(same(password))).thenReturn(mockOpenBuilder);
        when(mockOpenBuilder.withRecovery(anyBoolean())).thenReturn(mockOpenBuilder);
        when(mockOpenBuilder.execute()).thenReturn(FutureUtils.value(mockRh));
        when(mockBk.newOpenLedgerOp()).thenReturn(mockOpenBuilder);

        PulsarSegment segment = mock(PulsarSegment.class);
        when(segment.getLedgerId()).thenReturn(ledgerId);
        assertSame(mockRh, result(store.openRandomAccessEntryReader(segment)));

        verify(mockBk, times(1)).newOpenLedgerOp();
    }

}
