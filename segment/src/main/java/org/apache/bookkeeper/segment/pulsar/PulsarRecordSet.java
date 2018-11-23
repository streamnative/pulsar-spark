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

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.io.IOException;
import java.util.NoSuchElementException;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.segment.Record;
import org.apache.bookkeeper.api.segment.RecordSet;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.pulsar.client.impl.MessageParser;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.SingleMessageMetadata;

/**
 * A pulsar {@link RecordSet}.
 */
@Slf4j
@NotThreadSafe
@Getter(AccessLevel.PACKAGE)
class PulsarRecordSet implements RecordSet {

    static PulsarRecordSet create(String topicName,
                                  long ledgerId,
                                  LedgerEntry entry) {
        PulsarRecordSet recordSet = RECYCLER.get();
        return recordSet.reset(
            topicName, ledgerId, entry.getEntryId(), entry.getEntryBuffer().retain());
    }

    private static final Recycler<PulsarRecordSet> RECYCLER = new Recycler<PulsarRecordSet>() {
        @Override
        protected PulsarRecordSet newObject(Handle<PulsarRecordSet> handle) {
            return new PulsarRecordSet(handle);
        }
    };

    private final Handle<PulsarRecordSet> handle;

    private String topicName;
    private MessageIdData messageId;
    private MessageIdData.Builder messageIdBuilder;
    private ByteBuf payload;

    // initialize via {@link #initialize()}
    private MessageMetadata msgMetadata;
    private ByteBuf uncompressPayload;
    private int numMessages = 0;
    private int slotId = 0;

    private PulsarRecordSet(Handle<PulsarRecordSet> handle) {
        this.handle = handle;
    }

    PulsarRecordSet reset(String topicName,
                          long ledgerId,
                          long entryId,
                          ByteBuf headersAndPayload) {
        this.topicName = topicName;
        this.messageIdBuilder = MessageIdData.newBuilder();
        this.messageIdBuilder.setLedgerId(ledgerId);
        this.messageIdBuilder.setEntryId(entryId);
        this.messageId = this.messageIdBuilder.build();
        this.payload = headersAndPayload;
        return this;
    }

    /**
     * Initialize should be called before any other methods.
     *
     * @return true if the record set is initialized, otherwise false
     */
    boolean initialize() {
        if (!MessageParser.verifyChecksum(payload, messageId, topicName, "reader")) {
            // this is a corrupted entry
            log.error("[{}] entry (lid={}, eid={}) is corrupted",
                topicName, messageId.getLedgerId(), messageId.getEntryId());
            return false;
        }

        try {
            msgMetadata = Commands.parseMessageMetadata(payload);
        } catch (RuntimeException re) {
            log.error("[{}] failed to deserialize message metadata from entry (lid={}, eid={})",
                topicName, messageId.getLedgerId(), messageId.getEntryId(), re);
            return false;
        }

        if (msgMetadata.getEncryptionKeysCount() > 0) {
            log.error("[{}] entry (lid={}, eid={}) is encrypted, cannot parse it.");
            return false;
        }

        uncompressPayload = MessageParser.uncompressPayloadIfNeeded(
            messageId, msgMetadata, payload, topicName, "reader");
        if (null == uncompressPayload) {
            log.error("[{}] failed to decompress entry (lid={}, eid={})",
                topicName, messageId.getLedgerId(), messageId.getEntryId());
            return false;
        }

        numMessages = msgMetadata.getNumMessagesInBatch();
        return true;
    }

    @Override
    public boolean hasNext() {
        return slotId < numMessages;
    }

    @Override
    public Record next() {
        if (slotId >= numMessages) {
            throw new NoSuchElementException(
                "[" + topicName + "] No more record in entry (lid=" + messageId.getLedgerId()
                    + ", eid=" + messageId.getEntryId() + ")");
        }

        // single-message entry
        if (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch()) {
            try {
                return PulsarRecord.newRecord(
                    messageId.getEntryId(),
                    0,
                    msgMetadata,
                    uncompressPayload);
            } finally {
                // the ownership of metadata and payload is handed over to the record.
                msgMetadata = null;
                uncompressPayload = null;
                ++slotId;
            }
        } else {
            return nextRecordInBatch();
        }
    }

    private Record nextRecordInBatch() {
        SingleMessageMetadata.Builder singleMsgMetadataBuilder =
            SingleMessageMetadata.newBuilder();
        ByteBuf singleMsgPayload;
        try {
            singleMsgPayload = Commands.deSerializeSingleMessageInBatch(
                uncompressPayload, singleMsgMetadataBuilder, slotId, numMessages);
        } catch (IOException ioe) {
            singleMsgMetadataBuilder.recycle();
            log.error("[{}] failed to retrieve record {} from entry (lid={}, eid={})",
                topicName, slotId, messageId.getLedgerId(), messageId.getEntryId(), ioe);
            throw new RuntimeException("[" + topicName + "] failed to retrieve record " + slotId
                + " from entry (lid=" + messageId.getLedgerId() + ", eid=" + messageId.getEntryId() + ")", ioe);
        }

        if (singleMsgMetadataBuilder.getCompactedOut()) {
            singleMsgPayload.release();
            singleMsgMetadataBuilder.recycle();
            // TODO: compacted entry doesn't fit well into `hasNext-next` iteration model.
            //       so return `null` for now. need to revisit how to handle this better.
            return null;
        } else {
            MessageMetadata.Builder metadataBuilder = MessageMetadata.newBuilder(msgMetadata);
            singleMsgMetadataBuilder.getPropertiesList()
                .forEach(kv -> metadataBuilder.addProperties(kv));
            if (singleMsgMetadataBuilder.hasPartitionKey()) {
                metadataBuilder.setPartitionKeyB64Encoded(singleMsgMetadataBuilder.getPartitionKeyB64Encoded());
                metadataBuilder.setPartitionKey(singleMsgMetadataBuilder.getPartitionKey());
            }
            if (singleMsgMetadataBuilder.hasEventTime()) {
                metadataBuilder.setEventTime(singleMsgMetadataBuilder.getEventTime());
            }
            try {
                return PulsarRecord.newRecord(
                    messageId.getEntryId(), slotId++,
                    metadataBuilder.build(),
                    singleMsgPayload);
            } finally {
                singleMsgMetadataBuilder.recycle();
                metadataBuilder.recycle();
            }
        }
    }

    @Override
    public void close() {
        if (payload != null) {
            payload.release();
            payload = null;
        }

        if (uncompressPayload != null) {
            uncompressPayload.release();
            uncompressPayload = null;
        }

        if (messageIdBuilder != null) {
            messageIdBuilder.recycle();
            messageIdBuilder = null;
        }

        if (messageId != null) {
            messageId.recycle();
            messageId = null;
        }

        if (msgMetadata != null) {
            msgMetadata.recycle();
            msgMetadata = null;
        }

        topicName = null;
        numMessages = 0;
        slotId = 0;

        handle.recycle(this);
    }
}
