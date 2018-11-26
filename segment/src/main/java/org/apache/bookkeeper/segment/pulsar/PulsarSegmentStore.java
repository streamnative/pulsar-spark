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
package org.apache.bookkeeper.segment.pulsar;

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.segment.Segment;
import org.apache.bookkeeper.api.segment.SegmentStore;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Pulsar segment store.
 */
@Slf4j
public class PulsarSegmentStore implements SegmentStore {

    private final BookKeeper bk;
    private final DigestType digestType;
    private final byte[] password;

    public PulsarSegmentStore(BookKeeper bk,
                              DigestType digestType,
                              byte[] password) {
        this.bk = bk;
        this.digestType = digestType;
        this.password = password;
    }

    @Override
    public CompletableFuture<ReadHandle> openSegmentEntryReader(Segment segment) {
        if (!(segment instanceof PulsarSegment)) {
            return FutureUtils.exception(new IllegalArgumentException("Expected to open a pulsar segment"));
        }

        PulsarSegment ps = (PulsarSegment) segment;
        return bk.newOpenLedgerOp()
            .withLedgerId(ps.getLedgerId())
            .withDigestType(digestType)
            .withPassword(password)
            .withRecovery(false)
            .execute();
    }

    @Override
    public void close() {
        try {
            bk.close();
        } catch (BKException e) {
            log.warn("Failed to close pulsar segment store", e);
        } catch (InterruptedException e) {
            log.warn("Interrupted at closing pulsar segment store", e);
        }
    }
}
