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

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.segment.Segment;
import org.apache.bookkeeper.api.segment.SegmentSource;

/**
 * The abstract implementation of Pulsar topics based {@link SegmentSource}.
 */
public class ListSegmentSource implements SegmentSource {

    /**
     * Create a {@link SegmentSource} based on a list of {@link Segment}.
     *
     * @param segments list of segments
     * @return a list based segment source
     */
    public static ListSegmentSource of(List<Segment> segments) {
        return new ListSegmentSource(segments);
    }

    private final List<Segment> segments;
    private int index;

    private ListSegmentSource(List<Segment> segments) {
        this.segments = Collections.unmodifiableList(
            requireNonNull(segments, "Null segment list is provided"));
        this.index = 0;
    }

    @Override
    public CompletableFuture<SegmentBatch> getNextSegmentBatch(int maxNumSegments) {
        int remainingSegments = segments.size() - index;
        int numSegments = Math.min(remainingSegments, maxNumSegments);
        List<Segment> subSegments = segments.subList(index, index + numSegments);
        index += numSegments;

        return CompletableFuture.completedFuture(
            new SegmentBatch(subSegments, index < segments.size()));
    }

    @Override
    public void close() {
        // no-op
    }
}
