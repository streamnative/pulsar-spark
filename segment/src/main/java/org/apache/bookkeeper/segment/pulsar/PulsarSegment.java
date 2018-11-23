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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.bookkeeper.api.segment.EntryConverter;
import org.apache.bookkeeper.api.segment.Segment;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Version.Occurred;

/**
 * A pulsar segment.
 */
public class PulsarSegment implements Segment {

    private final String topicName;
    private final int partitionId;
    private final long ledgerId;
    private final byte[] ledgerMetadataBytes;

    private volatile String fullyQualifiedSegmentName = null;
    private volatile LedgerMetadata ledgerMetadata = null;

    @JsonCreator
    public PulsarSegment(
        @JsonProperty("topicName") String topicName,
        @JsonProperty("partitionId") int partitionId,
        @JsonProperty("ledgerId") long ledgerId,
        @JsonProperty("ledgerMetadata") byte[] ledgerMetadata) {
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.ledgerId = ledgerId;
        this.ledgerMetadataBytes = ledgerMetadata;
    }

    void deserializeLedgerMetadataIfNeeded() {
        if (null == ledgerMetadata) {
            try {
                ledgerMetadata = org.apache.bookkeeper.client.LedgerMetadata.parseConfig(
                    ledgerMetadataBytes,
                    Version.ANY,
                    com.google.common.base.Optional.absent()
                );
            } catch (IOException ioe) {
                throw new RuntimeException("Invalid ledger metadata for topic ["
                    + topicName + "] - ledger [" + ledgerId + "]", ioe);
            }
        }
    }

    @Override
    public String name() {
        if (null == fullyQualifiedSegmentName) {
            fullyQualifiedSegmentName = topicName + "-ledger-" + ledgerId;
        }
        return fullyQualifiedSegmentName;
    }

    @Override
    public Long id() {
        return ledgerId;
    }

    @Override
    public boolean isSealed() {
        deserializeLedgerMetadataIfNeeded();
        return ledgerMetadata.isClosed();
    }

    @Override
    public long size() {
        deserializeLedgerMetadataIfNeeded();
        return ledgerMetadata.getLength();
    }

    @Override
    public long entries() {
        deserializeLedgerMetadataIfNeeded();
        return ledgerMetadata.getLastEntryId() >= 0 ? ledgerMetadata.getLastEntryId() + 1 : -1;
    }

    @Override
    public Occurred compare(Segment segment) {
        if (!(segment instanceof PulsarSegment)) {
            return Occurred.CONCURRENTLY;
        }

        PulsarSegment other = (PulsarSegment) segment;

        if (Objects.equals(topicName, other.topicName)
            && partitionId == other.partitionId) {
            if (ledgerId > other.ledgerId) {
                return Occurred.AFTER;
            } else if (ledgerId < other.ledgerId) {
                return Occurred.BEFORE;
            } else {
                return Occurred.CONCURRENTLY;
            }
        } else {
            return Occurred.CONCURRENTLY;
        }
    }

    @Override
    public String[] getLocations() {
        deserializeLedgerMetadataIfNeeded();

        Set<BookieSocketAddress> allBookies = new HashSet<>();
        ledgerMetadata.getAllEnsembles().values().stream()
            .forEach(ensemble -> allBookies.addAll(ensemble));

        return allBookies.stream()
            .map(addr -> addr.toString())
            .collect(Collectors.toList())
            .toArray(new String[allBookies.size()]);
    }

    @Override
    public EntryConverter entryConverter() {
        return new PulsarEntryConverter(topicName, ledgerId);
    }
}
