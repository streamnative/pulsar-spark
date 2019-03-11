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
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.pulsar.api.segment.Segment;
import org.junit.Test;

/**
 * Unit test {@link PulsarSegment}.
 */
public class PulsarSegmentTest {

    private static final byte[] PASSWD = "test-password".getBytes(UTF_8);

    private static LedgerMetadata newLedgerMetadata(int numBookies) {
        LedgerMetadata lm = new LedgerMetadata(
            numBookies, numBookies, numBookies - 1,
            DigestType.CRC32C,
            PASSWD
        );
        ArrayList<BookieSocketAddress> ensemble = new ArrayList<>(numBookies);
        for (int i = 0; i < numBookies; i++) {
            ensemble.add(new BookieSocketAddress("127.0.0.1", 3181 + i));
        }
        lm.addEnsemble(0, ensemble);
        return lm;
    }

    /**
     * Test Json Serialization.
     */
    @Test
    public void testJsonSerialization() throws Exception {
        final String topicName = "test-json-serialization";
        final int partitionId = 100;
        final long ledgerId = 23456L;
        final LedgerMetadata lm = newLedgerMetadata(3);

        PulsarSegment segment = new PulsarSegment(
            topicName,
            partitionId,
            ledgerId,
            lm.serialize()
        );

        verifySegment(segment, topicName, ledgerId, lm);

        ObjectMapper mapper = new ObjectMapper();
        String segmentJsonStr = mapper.writeValueAsString(segment);

        mapper = new ObjectMapper();
        PulsarSegment returnSegment = mapper.readValue(segmentJsonStr, PulsarSegment.class);

        verifySegment(returnSegment, topicName, ledgerId, lm);
    }

    /**
     * Test Java Object Serialization.
     */
    @Test
    public void testJavaObjectSerialization() throws Exception {
        final String topicName = "test-java-object-serialization";
        final int partitionId = 100;
        final long ledgerId = 23456L;
        final LedgerMetadata lm = newLedgerMetadata(3);

        PulsarSegment segment = new PulsarSegment(
            topicName,
            partitionId,
            ledgerId,
            lm.serialize()
        );

        verifySegment(segment, topicName, ledgerId, lm);

        byte[] data;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream os = new ObjectOutputStream(baos)) {
            os.writeObject(segment);
            data = baos.toByteArray();
        }

        PulsarSegment returnSegment;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             ObjectInputStream is = new ObjectInputStream(bais)) {
             returnSegment = (PulsarSegment) is.readObject();
        }

        verifySegment(returnSegment, topicName, ledgerId, lm);
    }

    private static void verifySegment(PulsarSegment segment,
                                      String topicName,
                                      long ledgerId,
                                      LedgerMetadata lm) {

        assertEquals(topicName + "-ledger-" + ledgerId, segment.name());
        assertEquals(ledgerId, segment.id().longValue());
        assertEquals(lm.isClosed(), segment.isSealed());
        assertEquals(lm.getLength(), segment.size());
        if (lm.isClosed()) {
            assertEquals(lm.getLastEntryId() + 1, segment.entries());
        } else {
            assertEquals(-1, segment.entries());
        }
        String[] locations = segment.getLocations();

        Set<BookieSocketAddress> expectedBookies = new HashSet<>();
        lm.getAllEnsembles().values().stream()
            .forEach(ensemble -> expectedBookies.addAll(ensemble));

        Set<BookieSocketAddress> segmentBookies = Lists.newArrayList(locations)
            .stream()
            .map(addrStr -> {
                try {
                    return new BookieSocketAddress(addrStr);
                } catch (UnknownHostException uhe) {
                    throw new RuntimeException(uhe);
                }
            })
            .collect(Collectors.toSet());
        assertEquals(expectedBookies.size(), segmentBookies.size());
        assertTrue(Sets.difference(expectedBookies, segmentBookies).isEmpty());

    }

}
