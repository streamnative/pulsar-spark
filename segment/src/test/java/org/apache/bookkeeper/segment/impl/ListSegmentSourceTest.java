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
package org.apache.bookkeeper.segment.impl;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.segment.Segment;
import org.apache.bookkeeper.api.segment.SegmentSource.SegmentBatch;
import org.junit.Test;

/**
 * Unit test {@link ListSegmentSource}.
 */
@Slf4j
public class ListSegmentSourceTest {

    @Test(expected = NullPointerException.class)
    public void testCreateListSegmentSourceWithNullList() {
        ListSegmentSource.of(null);
    }

    private static List<Segment> createNumSegments(int numSegments) {
        return IntStream.range(0, numSegments)
            .mapToObj(idx -> mock(Segment.class))
            .collect(Collectors.toList());
    }

    private static void iterateListSegmentSource(ListSegmentSource source, int numSegmentsPerIteration,
                                                 List<Segment> expectedSegments) throws Exception {

        int idx = 0;
        while (true) {
            SegmentBatch batch = result(source.getNextSegmentBatch(numSegmentsPerIteration));
            List<Segment> subSegments =  batch.getSegments();
            for (Segment segment : subSegments) {
                Segment expectedSegment = expectedSegments.get(idx++);
                assertSame(expectedSegment, segment);
            }
            if (!batch.hasMoreSegments()) {
                break;
            }
        }
    }

    private void testGetNextSegmentBatch(int numSegments, int numSegmentsPerIteration) throws Exception {
        List<Segment> segments = createNumSegments(numSegments);
        ListSegmentSource source = ListSegmentSource.of(segments);
        iterateListSegmentSource(source, numSegmentsPerIteration, segments);
    }

    @Test
    public void testGetNextSegmentBatch() throws Exception {
        for (int i = 1; i <= 11; i++) {
            testGetNextSegmentBatch(10, i);
        }
    }

}
