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
import static org.junit.Assert.assertEquals;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.api.segment.Segment;
import org.apache.pulsar.api.segment.SegmentSource;
import org.apache.pulsar.api.segment.SegmentSource.SegmentBatch;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.segment.test.common.PulsarServiceTestCase;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test {@link PulsarSegmentSourceBuilder}.
 */
@Slf4j
public class PulsarSegmentSourceTest extends PulsarServiceTestCase {

    @Rule
    public final TestName testName = new TestName();

    private void createNonPartitionedTopic(String topicName, int numSegments) throws Exception {
        try (Producer<String> producer = client.newProducer(Schema.STRING)
             .topic(topicName)
             .create()
        ) {
            for (int i = 0; i < numSegments; i++) {
                log.info("Creating segment '{}' ...", i);
                producer.send("segment-" + i);
                log.info("Sent message '{}'", i);
                admin.topics().unload(topicName);
                log.info("Created segment '{}'.", i);
            }

        }
    }

    private void createPartitionedTopic(String topicName, int numPartitions, int numSegmentsPerPartition)
            throws Exception {
        admin.topics().createPartitionedTopic(topicName, numPartitions);
        try (Producer<byte[]> producer = client.newProducer(Schema.BYTES)
             .topic(topicName)
             .messageRoutingMode(MessageRoutingMode.CustomPartition)
             .messageRouter(new MessageRouter() {
                 @Override
                 public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                     return Integer.parseInt(msg.getKey()) % metadata.numPartitions();
                 }
             })
             .create()
        ) {
            TopicName tn = TopicName.get(topicName);
            for (int i = 0; i < numSegmentsPerPartition; i++) {
                log.info("Creating segment '{}' ...", i);
                for (int j = 0; j < numPartitions; j++) {
                    producer.newMessage()
                        .value(("partition-" + j + "-segment-" + i).getBytes(UTF_8))
                        .key(String.valueOf(j))
                        .send();
                    admin.topics().unload(tn.getPartition(j).toString());
                }
            }
        }
    }

    @Test
    public void testNonPartitionedTopicSegmentSource() throws Exception {
        String topicName = testName.getMethodName();
        createNonPartitionedTopic(topicName, 10);

        SegmentSource source = PulsarSegmentSourceBuilder.newBuilder()
            .withAdminUrl(getWebServiceUrl())
            .withTopic(topicName)
            .build();

        SegmentBatch batch = result(source.getNextSegmentBatch(10));
        List<Segment> segments = batch.getSegments();
        log.info("Received {} segments : {}", segments.size(), segments);
        assertEquals(10, segments.size());
    }

    @Ignore
    public void testPartitionedTopicSegmentSource() throws Exception {
        String topicName = testName.getMethodName();
        createPartitionedTopic(topicName, 5, 4);

        SegmentSource source = PulsarSegmentSourceBuilder.newBuilder()
            .withAdminUrl(getWebServiceUrl())
            .withTopic(topicName)
            .build();

        SegmentBatch batch = result(source.getNextSegmentBatch(100));
        List<Segment> segments = batch.getSegments();
        log.info("Received {} segments : {}", segments.size(), segments);
        assertEquals(20, segments.size());
    }

}
