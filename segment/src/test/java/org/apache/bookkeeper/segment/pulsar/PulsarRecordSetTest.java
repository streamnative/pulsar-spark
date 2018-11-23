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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.segment.Record;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.pulsar.client.impl.PulsarClientTestUtils;
import org.apache.pulsar.common.api.proto.PulsarApi.CompressionType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.junit.Test;

/**
 * Unit test {@link PulsarRecordSet}.
 */
@Slf4j
public class PulsarRecordSetTest {

    @Test
    public void testSingleMessageRecordNone() {
        testSingleMessageRecord(CompressionType.NONE);
    }

    @Test
    public void testSingleMessageRecordLZ4() {
        testSingleMessageRecord(CompressionType.LZ4);
    }

    private void testSingleMessageRecord(CompressionType compressionType) {
        final byte[] data = "test-single-message-record".getBytes(UTF_8);

        MessageMetadata.Builder metadataBuilder = MessageMetadata.newBuilder();
        metadataBuilder.setPublishTime(System.currentTimeMillis());
        metadataBuilder.setSequenceId(0L);
        metadataBuilder.setCompression(compressionType);
        metadataBuilder.setProducerName("test-producer-name");
        metadataBuilder.setUncompressedSize(data.length);

        ByteBuf payload = Unpooled.wrappedBuffer(data);
        ByteBuf serializedMessage = PulsarClientTestUtils.serializeSingleMessage(
            metadataBuilder, payload, CompressionCodecProvider.getCompressionCodec(compressionType));
        byte[] serializedMessageBytes = ByteBufUtil.getBytes(serializedMessage);

        final long ledgerId = 12345L;
        final long entryId = 356L;
        final long length = 1024 * 1024L;
        final String topicName = "test-single-message-record";
        final ByteBuf entryBuf = PooledByteBufAllocator.DEFAULT.buffer(serializedMessageBytes.length);
        entryBuf.writeBytes(serializedMessageBytes);

        // create the ledger entry to test
        LedgerEntryImpl entry = LedgerEntryImpl.create(
            ledgerId, entryId, length, entryBuf
        );

        // create the record set
        PulsarRecordSet recordSet = PulsarRecordSet.create(topicName, ledgerId, entry);
        assertEquals(topicName, recordSet.getTopicName());
        assertNotNull(recordSet.getMessageIdBuilder());
        assertNotNull(recordSet.getMessageId());
        assertNull(recordSet.getMsgMetadata());
        assertNull(recordSet.getUncompressPayload());
        assertEquals(0, recordSet.getNumMessages());
        assertEquals(0, recordSet.getSlotId());
        assertSame(entryBuf, recordSet.getPayload());
        // after a new record set is set, the entry buffer is retained
        assertEquals(2, entryBuf.refCnt());

        int refcnt = entryBuf.refCnt();
        // initialize the record set
        assertTrue(recordSet.initialize());
        assertNotNull(recordSet.getMsgMetadata());
        if (CompressionType.NONE == compressionType) {
            assertSame(entryBuf, recordSet.getUncompressPayload());
            assertEquals(refcnt + 1, entryBuf.refCnt());
        } else {
            assertNotSame(entryBuf, recordSet.getUncompressPayload());
            assertEquals(refcnt, entryBuf.refCnt());
            assertEquals(1, recordSet.getUncompressPayload().refCnt());
        }

        // iterate over record set
        assertTrue(recordSet.hasNext());
        assertEquals(1, recordSet.getNumMessages());
        assertEquals(0, recordSet.getSlotId());

        // before returning the record, keep the refcnt
        ByteBuf uncompressPayload = recordSet.getUncompressPayload();
        refcnt = uncompressPayload.refCnt();
        // return the record, the ownership of entry buf is handed over to the returned record
        Record record = recordSet.next();
        assertNotNull(record);
        assertEquals(1, recordSet.getNumMessages());
        assertEquals(1, recordSet.getSlotId());
        assertNull(recordSet.getMsgMetadata());
        assertNull(recordSet.getUncompressPayload());
        // only ownership is handed over by the refcnt remains unchanged
        assertEquals(refcnt, uncompressPayload.refCnt());

        // no more record in the record set
        assertFalse(recordSet.hasNext());

        // release record set
        recordSet.close();
        assertNull(recordSet.getPayload());
        assertNull(recordSet.getUncompressPayload());
        assertNull(recordSet.getMessageIdBuilder());
        assertNull(recordSet.getMessageId());
        assertNull(recordSet.getMsgMetadata());
        assertNull(recordSet.getTopicName());
        assertEquals(0, recordSet.getNumMessages());
        assertEquals(0, recordSet.getSlotId());

        // the uncompress payload is not released
        if (CompressionType.NONE == compressionType) {
            assertEquals(refcnt - 1, uncompressPayload.refCnt());
        } else {
            assertEquals(refcnt, uncompressPayload.refCnt());
        }
        refcnt = uncompressPayload.refCnt();

        // check the record
        PulsarRecord pulsarRecord = (PulsarRecord) record;
        assertNotNull(pulsarRecord.getMetadata());
        assertSame(uncompressPayload, pulsarRecord.getPayload());
        assertEquals(entryId, pulsarRecord.getEntryId());
        assertEquals(0, pulsarRecord.getSlotId());

        byte[] recordData = ByteBufUtil.getBytes(uncompressPayload);
        assertArrayEquals(data, recordData);

        record.close();
        assertEquals(refcnt - 1, uncompressPayload.refCnt());

        // release resources
        entry.close();
        serializedMessage.release();
        assertEquals(0, entryBuf.refCnt());
    }

    @Test
    public void testBatchedMessageRecordNone() {
        testBatchedMessageRecord(10, CompressionType.NONE);
    }

    @Test
    public void testBatchedMessageRecordLZ4() {
        testBatchedMessageRecord(10, CompressionType.LZ4);
    }

    private void testBatchedMessageRecord(int numMessages, CompressionType compressionType) {
        final String dataPrefix = "test-batched-message-record";
        final String producerName = "test-producer-name";
        final String topicName = "test-single-message-record";

        MessageMetadata.Builder metadataBuilder = MessageMetadata.newBuilder();
        metadataBuilder.setPublishTime(System.currentTimeMillis());
        metadataBuilder.setSequenceId(0L);
        metadataBuilder.setCompression(compressionType);
        metadataBuilder.setProducerName(producerName);

        ByteBuf serializedMessage = PulsarClientTestUtils.serializeBatchedMessage(
            numMessages, compressionType, topicName, producerName, dataPrefix,
            metadataBuilder);
        byte[] serializedMessageBytes = ByteBufUtil.getBytes(serializedMessage);

        final long ledgerId = 12345L;
        final long entryId = 356L;
        final long length = 1024 * 1024L;
        final ByteBuf entryBuf = PooledByteBufAllocator.DEFAULT.buffer(serializedMessageBytes.length);
        entryBuf.writeBytes(serializedMessageBytes);

        // create the ledger entry to test
        LedgerEntryImpl entry = LedgerEntryImpl.create(
            ledgerId, entryId, length, entryBuf
        );

        // create the record set
        PulsarRecordSet recordSet = PulsarRecordSet.create(topicName, ledgerId, entry);
        assertEquals(topicName, recordSet.getTopicName());
        assertNotNull(recordSet.getMessageIdBuilder());
        assertNotNull(recordSet.getMessageId());
        assertNull(recordSet.getMsgMetadata());
        assertNull(recordSet.getUncompressPayload());
        assertEquals(0, recordSet.getNumMessages());
        assertEquals(0, recordSet.getSlotId());
        assertSame(entryBuf, recordSet.getPayload());
        // after a new record set is set, the entry buffer is retained
        assertEquals(2, entryBuf.refCnt());

        int refcnt = entryBuf.refCnt();
        // initialize the record set
        assertTrue(recordSet.initialize());
        assertNotNull(recordSet.getMsgMetadata());
        if (CompressionType.NONE == compressionType) {
            assertSame(entryBuf, recordSet.getUncompressPayload());
            assertEquals(refcnt + 1, entryBuf.refCnt());
        } else {
            assertNotSame(entryBuf, recordSet.getUncompressPayload());
            assertEquals(refcnt, entryBuf.refCnt());
            assertEquals(1, recordSet.getUncompressPayload().refCnt());
        }
        assertEquals(numMessages, recordSet.getNumMessages());

        ByteBuf uncompressPayload = recordSet.getUncompressPayload();
        for (int i = 0; i < numMessages; i++) {
            // iterate over record set
            assertTrue(recordSet.hasNext());
            assertEquals(i, recordSet.getSlotId());

            // before returning the record, keep the refcnt
            refcnt = uncompressPayload.refCnt();
            // return the record, the ownership of entry buf is handed over to the returned record
            Record record = recordSet.next();
            assertNotNull(record);
            assertEquals(numMessages, recordSet.getNumMessages());
            assertEquals(i + 1, recordSet.getSlotId());
            assertNotNull(recordSet.getMsgMetadata());
            assertNotNull(recordSet.getUncompressPayload());
            assertEquals(refcnt + 1, uncompressPayload.refCnt());

            // check the record
            PulsarRecord pulsarRecord = (PulsarRecord) record;
            assertNotNull(pulsarRecord.getMetadata());
            assertEquals(entryId, pulsarRecord.getEntryId());
            assertEquals(i, pulsarRecord.getSlotId());

            byte[] recordData = ByteBufUtil.getBytes(pulsarRecord.getPayload());
            assertEquals(dataPrefix + "-" + i, new String(recordData, UTF_8));

            record.close();
            assertEquals(refcnt, uncompressPayload.refCnt());
        }

        // no more record in the record set
        assertFalse(recordSet.hasNext());

        // release record set
        recordSet.close();
        assertNull(recordSet.getPayload());
        assertNull(recordSet.getUncompressPayload());
        assertNull(recordSet.getMessageIdBuilder());
        assertNull(recordSet.getMessageId());
        assertNull(recordSet.getMsgMetadata());
        assertNull(recordSet.getTopicName());
        assertEquals(0, recordSet.getNumMessages());
        assertEquals(0, recordSet.getSlotId());

        // the uncompress payload is not released
        if (CompressionType.NONE == compressionType) {
            assertEquals(refcnt - 2, uncompressPayload.refCnt());
        } else {
            assertEquals(refcnt - 1, uncompressPayload.refCnt());
        }

        // release resources
        entry.close();
        serializedMessage.release();
        assertEquals(0, entryBuf.refCnt());
    }

}
