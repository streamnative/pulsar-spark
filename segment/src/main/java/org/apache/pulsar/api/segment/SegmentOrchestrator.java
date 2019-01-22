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

import java.util.function.Supplier;

/**
 * Orchestrator orchestrates segments.
 */
public interface SegmentOrchestrator<T> extends AutoCloseable {

    /**
     * Returns the segment source.
     *
     * <p>The segment source can be a pulsar topic or a set of pulsar topics.
     *
     * @return segment source
     */
    SegmentSource source();

    /**
     * Returns the segment store.
     *
     * <p>The segment store instance is used for reading data from segments
     * offered by {@link SegmentSource} via {@link #source()}.
     *
     * @return segment store to access data in segments
     */
    SegmentStore store();

    /**
     * Returns the schema provider.
     *
     * <p>The schema provider returns a {@link Schema} instance for
     * deserializing data stored in segments.
     *
     * @return schema provider
     */
    Supplier<Schema<T>> schemaProvider();

    /**
     * {@inheritDoc}
     */
    @Override
    void close();

}
