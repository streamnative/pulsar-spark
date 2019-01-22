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

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import org.apache.pulsar.api.segment.Record;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;

/**
 * A pulsar {@link Record}.
 */
class PulsarRecord implements Record {

    /**
     * Create a record of provided <tt>msg</tt>.
     *
     * @param entryId entry id
     * @param slotId slot id
     * @param metadata message metadata
     * @param payload message payload
     * @return a pulsar message record
     */
    static PulsarRecord newRecord(long entryId, int slotId,
                                  MessageMetadata metadata,
                                  ByteBuf payload) {
        PulsarRecord record = RECYCLER.get();
        record.entryId = entryId;
        record.slotId = slotId;
        record.metadata = metadata;
        record.payload = payload;
        return record;
    }

    private static final Recycler<PulsarRecord> RECYCLER = new Recycler<PulsarRecord>() {
        @Override
        protected PulsarRecord newObject(Handle<PulsarRecord> handle) {
            return new PulsarRecord(handle);
        }
    };

    private final Handle<PulsarRecord> handle;
    private long entryId;
    private int slotId;
    private MessageMetadata metadata;
    private ByteBuf payload;

    private PulsarRecord(Handle<PulsarRecord> handle) {
        this.handle = handle;
    }

    public long getEntryId() {
        return entryId;
    }

    public int getSlotId() {
        return slotId;
    }

    public ByteBuf getPayload() {
        return payload;
    }

    MessageMetadata getMetadata() {
        return metadata;
    }

    @Override
    public void close() {
        if (null != payload) {
            payload.release();
            payload = null;
        }
        if (null != metadata) {
            metadata.recycle();
            metadata = null;
        }
        entryId = -1L;
        slotId = -1;
        handle.recycle(this);
    }
}
