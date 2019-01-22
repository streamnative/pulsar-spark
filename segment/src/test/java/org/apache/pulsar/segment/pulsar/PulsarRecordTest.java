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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.pulsar.common.api.proto.PulsarApi.CompressionType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.junit.Test;

/**
 * Unit test {@link PulsarRecord}.
 */
public class PulsarRecordTest {

    @Test
    public void testPulsarRecord() {
        long entryId = 12345L;
        int slotId = 3758;
        MessageMetadata.Builder metadataBuilder = MessageMetadata.newBuilder()
            .setPublishTime(System.currentTimeMillis())
            .setSequenceId(0L)
            .setProducerName("test-producer")
            .setCompression(CompressionType.LZ4);
        ByteBuf payload = Unpooled.wrappedBuffer("test-pulsar-record".getBytes(UTF_8));

        // create a record
        MessageMetadata metadata = metadataBuilder.build();
        PulsarRecord record = PulsarRecord.newRecord(
            entryId, slotId, metadata, payload
        );
        assertEquals(entryId, record.getEntryId());
        assertEquals(slotId, record.getSlotId());
        assertSame(payload, record.getPayload());
        assertEquals(1, payload.refCnt());

        // close record should release resources
        record.close();
        assertEquals(-1L, record.getEntryId());
        assertEquals(-1, record.getSlotId());
        assertNull(record.getMetadata());
        assertNull(record.getPayload());
        assertEquals(0, payload.refCnt());

        // the object should be recycled.
        ByteBuf newPayload = Unpooled.wrappedBuffer("test-pulsar-new-record".getBytes(UTF_8));
        PulsarRecord newRecord = PulsarRecord.newRecord(
            entryId + 1,
            slotId + 1,
            metadataBuilder.setEventTime(1234L).build(),
            newPayload
        );
        // it is the record recycled
        assertSame(record, newRecord);
        assertEquals(entryId + 1, newRecord.getEntryId());
        assertEquals(slotId + 1, newRecord.getSlotId());
        assertNotNull(newRecord.getMetadata());
        assertSame(newPayload, newRecord.getPayload());
        assertEquals(1, newPayload.refCnt());

        newRecord.close();
    }

}
