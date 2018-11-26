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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.api.segment.EndOfSegmentException;
import org.apache.bookkeeper.api.segment.SegmentEntryReader;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link SegmentEntryReaderImplWithEndEntryId}.
 */
public class SegmentEntryReaderImplWithEndEntryIdTest {

    private SegmentEntryReader mockedUnderlyingReader;

    @Before
    public void setup() {
        this.mockedUnderlyingReader = mock(SegmentEntryReader.class);
    }

    @Test
    public void testEndOfSegment() throws Exception {
        EndOfSegmentException exception = new EndOfSegmentException("test-exception");
        doThrow(exception).when(mockedUnderlyingReader).readNext();

        SegmentEntryReaderImplWithEndEntryId reader =
            new SegmentEntryReaderImplWithEndEntryId(mockedUnderlyingReader, 100L);
        try {
            reader.readNext();
            fail("Should fail with EndOfSegmentException");
        } catch (EndOfSegmentException e) {
            assertSame(exception, e);
        }
    }

    @Test
    public void testReachEndEntry() throws Exception {
        when(mockedUnderlyingReader.readNext()).thenReturn(Collections.emptyList());

        SegmentEntryReaderImplWithEndEntryId reader =
            new SegmentEntryReaderImplWithEndEntryId(mockedUnderlyingReader, 100L);
        List<LedgerEntry> entries = reader.readNext();
        assertTrue(entries.isEmpty());
        assertTrue(reader.reachEndEntry());
        verify(mockedUnderlyingReader, times(1)).readNext();

        entries = reader.readNext();
        assertTrue(entries.isEmpty());
        assertTrue(reader.reachEndEntry());
        verify(mockedUnderlyingReader, times(1)).readNext();
    }

    @Test
    public void testClose() throws Exception {
        SegmentEntryReaderImplWithEndEntryId reader =
            new SegmentEntryReaderImplWithEndEntryId(mockedUnderlyingReader, 100L);
        reader.close();
        verify(mockedUnderlyingReader, times(1)).close();
    }

    private static List<LedgerEntry> newEntries(long startEntryId, long endEntryId) {
        List<LedgerEntry> entries = new ArrayList<>((int) (endEntryId - startEntryId + 1));
        for (long entryId = startEntryId; entryId <= endEntryId; ++entryId) {
            LedgerEntry entry = mock(LedgerEntry.class);
            when(entry.getEntryId()).thenReturn(entryId);
            entries.add(entry);
        }
        return entries;
    }

    @Test
    public void testReadNext() throws Exception {
        List<LedgerEntry> firstBatch = newEntries(0L, 9L);
        List<LedgerEntry> secondBatch = newEntries(10L, 19L);

        when(mockedUnderlyingReader.readNext())
            .thenReturn(firstBatch)
            .thenReturn(secondBatch);

        SegmentEntryReaderImplWithEndEntryId reader =
            new SegmentEntryReaderImplWithEndEntryId(mockedUnderlyingReader, 15L);

        // first read
        List<LedgerEntry> entries = reader.readNext();
        assertSame(firstBatch, entries);
        assertFalse(reader.reachEndEntry());

        // second read
        entries = reader.readNext();
        assertEquals(6, entries.size());
        for (int i = 0; i < 6; i++) {
            LedgerEntry le = entries.get(i);
            assertEquals(10L + i, le.getEntryId());
            verify(le, times(0)).close();
        }
        assertTrue(reader.reachEndEntry());
        // the remaining 4 entries will be released
        for (int i = 6; i < 10; i++) {
            LedgerEntry le = secondBatch.get(i);
            assertEquals(10L + i, le.getEntryId());
            verify(le, times(1)).close();
        }

        // third read
        entries = reader.readNext();
        assertEquals(0, entries.size());
    }

}
