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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.segment.Segment;
import org.apache.bookkeeper.api.segment.SegmentSource;
import org.apache.bookkeeper.api.segment.SegmentSource.SegmentBatch;
import org.apache.pulsar.PulsarServiceTestCase;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
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

}
