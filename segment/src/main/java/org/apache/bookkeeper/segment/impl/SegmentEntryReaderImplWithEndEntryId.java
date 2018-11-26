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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.api.segment.EndOfSegmentException;
import org.apache.bookkeeper.api.segment.SegmentEntryReader;
import org.apache.bookkeeper.client.api.LedgerEntry;

/**
 * A segment entry reader wraps an existing reader with end entry id.
 */
class SegmentEntryReaderImplWithEndEntryId implements SegmentEntryReader {

    private final SegmentEntryReader reader;
    private final long endEntryId;
    private boolean reachEndEntry = false;

    SegmentEntryReaderImplWithEndEntryId(SegmentEntryReader reader,
                                         long endEntryId) {
        this.reader = reader;
        this.endEntryId = endEntryId;
    }

    @VisibleForTesting
    long getEndEntryId() {
        return endEntryId;
    }

    @VisibleForTesting
    boolean reachEndEntry() {
        return reachEndEntry;
    }

    @Override
    public List<LedgerEntry> readNext() throws EndOfSegmentException, IOException {
        if (reachEndEntry) {
            return Collections.emptyList();
        } else {
            return doReadNext();
        }
    }

    private List<LedgerEntry> doReadNext() throws EndOfSegmentException, IOException {
        List<LedgerEntry> entries = reader.readNext();
        if (entries.isEmpty()) {
            reachEndEntry = true;
            return entries;
        } else {
            if (entries.get(entries.size() - 1).getEntryId() <= endEntryId) {
                return entries;
            } else {
                List<LedgerEntry> subEntries = new ArrayList<>(entries.size());
                for (int i = 0; i < entries.size(); i++) {
                    LedgerEntry entry = entries.get(i);
                    if (reachEndEntry) {
                        entry.close();
                    } else {
                        if (entry.getEntryId() <= endEntryId) {
                            subEntries.add(entry);
                        } else {
                            reachEndEntry = true;
                            entry.close();
                        }
                    }
                }
                return subEntries;
            }
        }
    }

    @Override
    public void close() {
        reader.close();
    }
}
