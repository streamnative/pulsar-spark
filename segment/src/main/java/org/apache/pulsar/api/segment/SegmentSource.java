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
package org.apache.pulsar.api.segment;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The source that offers segments.
 */
public interface SegmentSource extends AutoCloseable {

    /**
     * A batch of segments.
     */
    class SegmentBatch {

        private final List<Segment> segments;
        private final boolean hasMoreSegments;

        public SegmentBatch(List<Segment> segments, boolean hasMoreSegments) {
            this.segments = requireNonNull(segments, "segments is null");
            this.hasMoreSegments = hasMoreSegments;
        }

        public List<Segment> getSegments() {
            return segments;
        }

        public boolean hasMoreSegments() {
            return hasMoreSegments;
        }

    }

    /**
     * Retrieve the next batch of segments.
     *
     * <p>If the returned {@link SegmentBatch} returns <tt>false</tt> at
     * {@link SegmentBatch#hasMoreSegments()}, it means there is no more segments
     * to retrieve from this segment source.
     *
     * @param maxNumSegments max number of segments to return
     * @return a future represents the next batch of segments.
     */
    CompletableFuture<SegmentBatch> getNextSegmentBatch(int maxNumSegments);

    @Override
    void close();

}
