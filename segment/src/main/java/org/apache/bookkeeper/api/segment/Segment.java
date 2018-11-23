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

import java.io.Serializable;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.versioning.Version.Occurred;

/**
 * Class represents the metadata of a segment.
 */
public interface Segment extends Serializable {

    /**
     * Name of the segment.
     *
     * @return name of the segment.
     */
    String name();

    /**
     * Id of the segment.
     *
     * @return id of the segment.
     */
    Long id();

    /**
     * Whether the segment is sealed or not.
     *
     * @return true if the segment is sealed, otherwise false.
     */
    boolean isSealed();

    /**
     * Return the bytes of this segment.
     *
     * @return the bytes of this segment.
     */
    long size();

    /**
     * Return the number of entries in this segment.
     *
     * @return the number of entries in this segment.
     */
    long entries();

    /**
     * Compare the orders between segments.
     *
     * @param segment segment to compare.
     * @return the ordering between segments.
     */
    Occurred compare(Segment segment);

    /**
     * Get the locations storing the segment.
     *
     * @return the locations storing the segment.
     */
    String[] getLocations();

    /**
     * Returns an entry converter that can be used for converting {@link org.apache.bookkeeper.client.api.LedgerEntry}
     * to {@link RecordSet}.
     *
     * @return entry convert.
     */
    EntryConverter entryConverter();

}
