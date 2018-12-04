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

import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.api.segment.RecordSet;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test {@link PulsarEntryConverter}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ PulsarRecordSet.class, PulsarEntryConverter.class })
public class PulsarEntryConverterTest {

    @Rule
    public final TestName testName = new TestName();

    @Test
    public void testConvertEntry() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String topicName = "topic-" + testName.getMethodName();
        PulsarEntryConverter converter = new PulsarEntryConverter(
            topicName, ledgerId
        );
        PulsarRecordSet mockSet = mock(PulsarRecordSet.class);
        when(mockSet.initialize()).thenReturn(true);
        PowerMockito.mockStatic(PulsarRecordSet.class);
        PowerMockito.doAnswer(invocation -> mockSet).when(
            PulsarRecordSet.class,
            "create",
            any(String.class),
            anyLong(),
            any(LedgerEntry.class)
        );

        LedgerEntry mockEntry = mock(LedgerEntry.class);
        RecordSet convertedSet = converter.convertEntry(mockEntry);
        assertSame(mockSet, convertedSet);
        PowerMockito.verifyStatic(PulsarRecordSet.class, times(1));
        PulsarRecordSet.create(
            eq(topicName),
            eq(ledgerId),
            same(mockEntry)
        );
    }



}
