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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.zookeeper.ExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.pulsar.api.segment.EndOfSegmentException;
import org.apache.pulsar.api.segment.EntryConverter;
import org.apache.pulsar.api.segment.ReadAheadConfig;
import org.apache.pulsar.api.segment.Record;
import org.apache.pulsar.api.segment.RecordSet;
import org.apache.pulsar.api.segment.Segment;
import org.apache.pulsar.api.segment.SegmentEntryReader;
import org.apache.pulsar.api.segment.SegmentSource;
import org.apache.pulsar.api.segment.SegmentSource.SegmentBatch;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.segment.pulsar.PulsarSegmentSourceBuilder;
import org.apache.pulsar.segment.pulsar.PulsarSegmentStore;
import org.apache.pulsar.segment.test.common.PulsarServiceTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test {@link SegmentEntryReaderImpl}.
 */
@Slf4j
public class SegmentEntryReaderImplTest extends PulsarServiceTestCase {

    private static final DigestType digestType = DigestType.CRC32C;
    private static final byte[] password = "".getBytes(UTF_8);

    @Rule
    public final TestName runtime = new TestName();
    private OrderedScheduler scheduler;
    private ClientConfiguration conf;
    private BookKeeper bkc;
    private ZooKeeperClient zkc;
    private PulsarSegmentStore segmentStore;

    @Override
    public void doSetup() throws Exception {
        zkc = ZooKeeperClient.newBuilder()
            .connectString(getZkServers())
            .sessionTimeoutMs(30000)
            .operationRetryPolicy(new ExponentialBackoffRetryPolicy(2000, 30))
            .build();
        conf = new ClientConfiguration()
            .setMetadataServiceUri("zk://" + getZkServers() + "/ledgers");
        bkc = BookKeeper.newBuilder(conf)
            .build();
        scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-segment-entry-reader")
            .numThreads(1)
            .build();
        segmentStore = new PulsarSegmentStore(
            bkc, digestType, password
        );
    }

    @Override
    public void doTeardown() throws Exception {
        if (null != bkc) {
            bkc.close();
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
        if (null != zkc) {
            zkc.close();
        }
    }


    private void createNonPartitionedTopic(String topicName, int numMessages) throws Exception {
        try (Producer<String> producer = client.newProducer(Schema.STRING)
            .enableBatching(false)
            .topic(topicName)
            .create()
        ) {
            for (int i = 0; i < numMessages; i++) {
                producer.send("message-" + i);
            }
            producer.flush();
        }
    }

    private List<Segment> getNonPartitionedTopicSegments(String topicName) throws Exception {
        SegmentSource source = PulsarSegmentSourceBuilder.newBuilder()
            .withAdminUrl(getWebServiceUrl())
            .withTopic(topicName)
            .build();
        SegmentBatch batch = result(source.getNextSegmentBatch(Integer.MAX_VALUE));
        return batch.getSegments();
    }


    @Test
    public void testReadEntriesFromCompleteLogSegment() throws Exception {
        final String topicName = runtime.getMethodName();
        final int numMessages = 20;

        createNonPartitionedTopic(topicName, numMessages);
        admin.topics().unload(topicName);

        List<Segment> segments = getNonPartitionedTopicSegments(topicName);

        assertEquals(segments.size() + " segments found, expected to be only one",
            1, segments.size());

        EntryConverter converter = segments.get(0).entryConverter();
        SegmentEntryReader reader = result(segmentStore.openSequentialEntryReader(
            segments.get(0),
            0L,
            Optional.empty(),
            ReadAheadConfig.builder().build()
        ));

        reader.start();
        boolean done = false;
        int index = 0;
        while (!done) {
            List<LedgerEntry> entries;
            try {
                entries = reader.readNext();
            } catch (EndOfSegmentException eos) {
                done = true;
                continue;
            }

            for (LedgerEntry entry : entries) {
                try (RecordSet rs = converter.convertEntry(entry)) {
                    while (rs.hasNext()) {
                        try (Record record = rs.next()) {
                            ++index;
                        }
                    }
                } finally {
                    entry.close();
                }
            }

        }
        assertEquals(numMessages, index);
        reader.close();
    }

}
