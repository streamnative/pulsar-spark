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
package org.apache.bookkeeper.api.segment;

import org.apache.bookkeeper.client.api.LedgerEntry;

/**
 * Entry Reader that reads entries within a segment.
 */
public interface SegmentEntryReader extends AutoCloseable {

    /**
     * Return next available entry in the segment.
     *
     * @return next available entry in the segment
     * @throws EndOfSegmentException when reaching end of segment.
     */
    LedgerEntry readNext();

    /**
     * {@inheritDoc}
     */
    @Override
    void close();

}
