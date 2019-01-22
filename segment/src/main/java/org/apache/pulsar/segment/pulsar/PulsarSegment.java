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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Version.Occurred;
import org.apache.pulsar.api.segment.EntryConverter;
import org.apache.pulsar.api.segment.Segment;

/**
 * A pulsar segment.
 */
@Getter(AccessLevel.PACKAGE)
public class PulsarSegment implements Segment {

    @JsonProperty
    private final String topicName;
    @JsonProperty
    private final int partitionId;
    @JsonProperty
    private final long ledgerId;
    @JsonProperty
    private final byte[] ledgerMetadataBytes;

    private transient String fullyQualifiedSegmentName = null;
    private transient LedgerMetadata ledgerMetadata = null;

    @JsonCreator
    public PulsarSegment(
        @JsonProperty("topicName") String topicName,
        @JsonProperty("partitionId") int partitionId,
        @JsonProperty("ledgerId") long ledgerId,
        @JsonProperty("ledgerMetadataBytes") byte[] ledgerMetadata) {
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

    @JsonIgnore
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

    @JsonIgnore
    @Override
    public String[] getLocations() {
        deserializeLedgerMetadataIfNeeded();

        Set<BookieSocketAddress> allBookies = new HashSet<>();
        ledgerMetadata.getAllEnsembles().values().stream()
            .forEach(ensemble -> allBookies.addAll(ensemble));

        return allBookies.stream()
            .map(addr -> addr.toString())
            .collect(Collectors.toList())
            .toArray(new String[0]);
    }

    @Override
    public EntryConverter entryConverter() {
        return new PulsarEntryConverter(topicName, ledgerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName, ledgerId, partitionId) * 13 + Arrays.hashCode(ledgerMetadataBytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PulsarSegment)) {
            return false;
        }
        PulsarSegment other = (PulsarSegment) obj;
        return Objects.equals(topicName, other.topicName)
            && Objects.equals(ledgerId, other.ledgerId)
            && Objects.equals(partitionId, other.partitionId)
            && Arrays.equals(ledgerMetadataBytes, other.ledgerMetadataBytes);
    }

    @Override
    public String toString() {
        ToStringHelper helper = MoreObjects.toStringHelper("PulsarSegment");
        helper.add("TopicName", topicName);
        helper.add("PartitionId", partitionId);
        helper.add("LedgerId", ledgerId);
        helper.add("LedgerMetadata", new String(ledgerMetadataBytes, UTF_8));
        return helper.toString();
    }
}
