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

import org.apache.bookkeeper.api.segment.EntryConverter;
import org.apache.bookkeeper.api.segment.RecordSet;
import org.apache.bookkeeper.client.api.LedgerEntry;

/**
 * An {@link EntryConverter} converts ledger entries to pulsar messages.
 */
class PulsarEntryConverter implements EntryConverter {

    private final String topicName;
    private final long ledgerId;

    PulsarEntryConverter(String topicName, long ledgerId) {
        this.topicName = topicName;
        this.ledgerId = ledgerId;
    }

    @Override
    public RecordSet convertEntry(LedgerEntry entry) {
        PulsarRecordSet rs = PulsarRecordSet.create(
            topicName,
            ledgerId,
            entry
        );
        if (!rs.initialize()) {
            throw new RuntimeException("entry " + entry.getEntryId() + " @ ledger " + ledgerId + " is corrupted");
        }
        return rs;
    }
}
