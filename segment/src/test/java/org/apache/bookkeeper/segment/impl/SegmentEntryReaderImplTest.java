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
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.bookkeeper.api.segment.Segment;
import org.apache.bookkeeper.api.segment.SegmentSource;
import org.apache.bookkeeper.api.segment.SegmentSource.SegmentBatch;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.segment.pulsar.PulsarSegmentSourceBuilder;
import org.apache.bookkeeper.zookeeper.ExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.pulsar.PulsarServiceTestCase;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test {@link SegmentEntryReaderImpl}.
 */
public class SegmentEntryReaderImplTest extends PulsarServiceTestCase {

    @Rule
    public final TestName runtime = new TestName();
    private OrderedScheduler scheduler;
    private ClientConfiguration conf;
    private BookKeeper bkc;
    private ZooKeeperClient zkc;

    @Override
    public void doSetup() throws Exception {
        zkc = ZooKeeperClient.newBuilder()
            .connectString(getZkServers())
            .sessionTimeoutMs(30000)
            .operationRetryPolicy(new ExponentialBackoffRetryPolicy(2000, 30))
            .build();
        conf = new ClientConfiguration()
            .setMetadataServiceUri("zk://" + getZkServers() + "/ledgers");
        bkc = BookKeeper.newBuilder(new ClientConfiguration())
            .build();
        scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-segment-entry-reader")
            .numThreads(1)
            .build();
    }

    @Override
    public void doTeardown() throws Exception {
        if (null != bkc) {
            bkc.close();
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
        if (null != zkc) {
            zkc.close();
        }
    }


    /**

    SegmentEntryReaderImpl createEntryReader(Segment segment, long startEntryId) throws Exception {



        LogSegmentEntryStore store = new BKLogSegmentEntryStore(
            conf,
            ConfUtils.getConstDynConf(conf),
            zkc,
            bkc,
            scheduler,
            null,
            NullStatsLogger.INSTANCE,
            AsyncFailureInjector.NULL);
        return (BKLogSegmentEntryReader) Utils.ioResult(store.openReader(segment, startEntryId));
    }



    void generateCompletedLogSegments(DistributedLogManager dlm,
                                      DistributedLogConfiguration conf,
                                      long numCompletedSegments,
                                      long segmentSize) throws Exception {
        long txid = 1L;
        for (long i = 0; i < numCompletedSegments; i++) {
            AsyncLogWriter writer = Utils.ioResult(dlm.openAsyncLogWriter());
            for (long j = 1; j <= segmentSize; j++) {
                Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(txid++)));
                LogRecord ctrlRecord = DLMTestUtil.getLogRecordInstance(txid);
                ctrlRecord.setControl();
                Utils.ioResult(writer.write(ctrlRecord));
            }
            Utils.close(writer);
        }
    }

    AsyncLogWriter createInprogressLogSegment(DistributedLogManager dlm,
                                              DistributedLogConfiguration conf,
                                              long segmentSize) throws Exception {
        AsyncLogWriter writer = Utils.ioResult(dlm.openAsyncLogWriter());
        for (long i = 1L; i <= segmentSize; i++) {
            Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(i)));
            LogRecord ctrlRecord = DLMTestUtil.getLogRecordInstance(i);
            ctrlRecord.setControl();
            Utils.ioResult(writer.write(ctrlRecord));
        }
        return writer;
    }

    private void createNonPartitionedTopic(String topicName, int numMessages) throws Exception {
        try (Producer<String> producer = client.newProducer(Schema.STRING)
            .enableBatching(false)
            .topic(topicName)
            .create()
        ) {
            for (int i = 0; i < numMessages; i++) {
                producer.send("message-" + i);
            }
            producer.flush();
        }
    }

    private List<Segment> getNonPartitionedTopicSegments(String topicName) throws Exception {
        SegmentSource source = PulsarSegmentSourceBuilder.newBuilder()
            .withAdminUrl(getWebServiceUrl())
            .withTopic(topicName)
            .build();
        SegmentBatch batch = result(source.getNextSegmentBatch(Integer.MAX_VALUE));
        return batch.getSegments();
    }

    @Test
    public void testReadEntriesFromCompleteLogSegment() throws Exception {
        final String topicName = runtime.getMethodName();
        final int numMessages = 20;

        createNonPartitionedTopic(topicName, numMessages);
        admin.topics().unload(topicName);

        List<Segment> segments = getNonPartitionedTopicSegments(topicName);

        assertEquals(segments.size() + " segments found, expected to be only one",
            1, segments.size());

        SegmentEntryReaderImpl reader = createEntryReader(segments.get(0), 0, confLocal);
        reader.start();
        boolean done = false;
        long txId = 1L;
        long entryId = 0L;
        while (!done) {
            Entry.Reader entryReader;
            try {
                entryReader = Utils.ioResult(reader.readNext(1)).get(0);
            } catch (EndOfLogSegmentException eol) {
                done = true;
                continue;
            }
            LogRecordWithDLSN record = entryReader.nextRecord();
            while (null != record) {
                if (!record.isControl()) {
                    DLMTestUtil.verifyLogRecord(record);
                    assertEquals(txId, record.getTransactionId());
                    ++txId;
                }
                DLSN dlsn = record.getDlsn();
                assertEquals(1L, dlsn.getLogSegmentSequenceNo());
                assertEquals(entryId, dlsn.getEntryId());
                record = entryReader.nextRecord();
            }
            ++entryId;
        }
        assertEquals(21, txId);
        assertFalse(reader.hasCaughtUpOnInprogress());
        Utils.close(reader);
    }

    /**
    @Test(timeout = 60000)
    public void testCloseReaderToCancelPendingReads() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setNumPrefetchEntriesPerLogSegment(10);
        confLocal.setMaxPrefetchEntriesPerLogSegment(10);
        DistributedLogManager dlm = createNewDLM(confLocal, runtime.getMethodName());
        DLMTestUtil.generateCompletedLogSegments(dlm, confLocal, 1, 20);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(segments.size() + " log segments found, expected to be only one",
            1, segments.size());

        BKLogSegmentEntryReader reader = createEntryReader(segments.get(0), 0, confLocal);
        List<CompletableFuture<List<Entry.Reader>>> futures = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            futures.add(reader.readNext(1));
        }
        assertFalse("Reader should not be closed yet", reader.isClosed());
        Utils.close(reader);
        for (CompletableFuture<List<Entry.Reader>> future : futures) {
            try {
                Utils.ioResult(future);
                fail("The read request should be cancelled");
            } catch (ReadCancelledException rce) {
                // expected
            }
        }
        assertFalse(reader.hasCaughtUpOnInprogress());
        assertTrue("Reader should be closed yet", reader.isClosed());
    }

    @Test(timeout = 60000)
    public void testMaxPrefetchEntriesSmallBatch() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setNumPrefetchEntriesPerLogSegment(2);
        confLocal.setMaxPrefetchEntriesPerLogSegment(10);
        DistributedLogManager dlm = createNewDLM(confLocal, runtime.getMethodName());
        generateCompletedLogSegments(dlm, confLocal, 1, 20);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(segments.size() + " log segments found, expected to be only one",
            1, segments.size());

        BKLogSegmentEntryReader reader = createEntryReader(segments.get(0), 0, confLocal);
        reader.start();

        // wait for the read ahead entries to become available
        while (reader.readAheadEntries.size() < 10) {
            TimeUnit.MILLISECONDS.sleep(10);
        }

        long txId = 1L;
        long entryId = 0L;

        assertEquals(10, reader.readAheadEntries.size());
        assertEquals(10, reader.getNextEntryId());
        assertFalse(reader.hasCaughtUpOnInprogress());
        // read first entry
        Entry.Reader entryReader = Utils.ioResult(reader.readNext(1)).get(0);
        LogRecordWithDLSN record = entryReader.nextRecord();
        while (null != record) {
            if (!record.isControl()) {
                DLMTestUtil.verifyLogRecord(record);
                assertEquals(txId, record.getTransactionId());
                ++txId;
            }
            DLSN dlsn = record.getDlsn();
            assertEquals(1L, dlsn.getLogSegmentSequenceNo());
            assertEquals(entryId, dlsn.getEntryId());
            record = entryReader.nextRecord();
        }
        ++entryId;
        assertEquals(2L, txId);
        // wait for the read ahead entries to become 10 again
        while (reader.readAheadEntries.size() < 10) {
            TimeUnit.MILLISECONDS.sleep(10);
        }

        assertEquals(10, reader.readAheadEntries.size());
        assertEquals(11, reader.getNextEntryId());
        assertFalse(reader.hasCaughtUpOnInprogress());

        Utils.close(reader);
    }

    @Test(timeout = 60000)
    public void testMaxPrefetchEntriesLargeBatch() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setNumPrefetchEntriesPerLogSegment(10);
        confLocal.setMaxPrefetchEntriesPerLogSegment(5);
        DistributedLogManager dlm = createNewDLM(confLocal, runtime.getMethodName());
        generateCompletedLogSegments(dlm, confLocal, 1, 20);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(segments.size() + " log segments found, expected to be only one",
            1, segments.size());

        BKLogSegmentEntryReader reader = createEntryReader(segments.get(0), 0, confLocal);
        reader.start();

        // wait for the read ahead entries to become available
        while (reader.readAheadEntries.size() < 5) {
            TimeUnit.MILLISECONDS.sleep(10);
        }

        long txId = 1L;
        long entryId = 0L;

        assertEquals(5, reader.readAheadEntries.size());
        assertEquals(5, reader.getNextEntryId());
        // read first entry
        Entry.Reader entryReader = Utils.ioResult(reader.readNext(1)).get(0);
        LogRecordWithDLSN record = entryReader.nextRecord();
        while (null != record) {
            if (!record.isControl()) {
                DLMTestUtil.verifyLogRecord(record);
                assertEquals(txId, record.getTransactionId());
                ++txId;
            }
            DLSN dlsn = record.getDlsn();
            assertEquals(1L, dlsn.getLogSegmentSequenceNo());
            assertEquals(entryId, dlsn.getEntryId());
            record = entryReader.nextRecord();
        }
        ++entryId;
        assertEquals(2L, txId);
        // wait for the read ahead entries to become 10 again
        while (reader.readAheadEntries.size() < 5) {
            TimeUnit.MILLISECONDS.sleep(10);
        }

        assertEquals(5, reader.readAheadEntries.size());
        assertEquals(6, reader.getNextEntryId());
        assertFalse(reader.hasCaughtUpOnInprogress());

        Utils.close(reader);
    }

    @Test(timeout = 60000)
    public void testMaxPrefetchEntriesSmallSegment() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setNumPrefetchEntriesPerLogSegment(10);
        confLocal.setMaxPrefetchEntriesPerLogSegment(20);
        DistributedLogManager dlm = createNewDLM(confLocal, runtime.getMethodName());
        generateCompletedLogSegments(dlm, confLocal, 1, 5);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(segments.size() + " log segments found, expected to be only one",
            1, segments.size());

        BKLogSegmentEntryReader reader = createEntryReader(segments.get(0), 0, confLocal);
        reader.start();

        // wait for the read ahead entries to become available
        while (reader.readAheadEntries.size() < (reader.getLastAddConfirmed() + 1)) {
            TimeUnit.MILLISECONDS.sleep(10);
        }

        long txId = 1L;
        long entryId = 0L;

        assertEquals((reader.getLastAddConfirmed() + 1), reader.readAheadEntries.size());
        assertEquals((reader.getLastAddConfirmed() + 1), reader.getNextEntryId());
        // read first entry
        Entry.Reader entryReader = Utils.ioResult(reader.readNext(1)).get(0);
        LogRecordWithDLSN record = entryReader.nextRecord();
        while (null != record) {
            if (!record.isControl()) {
                DLMTestUtil.verifyLogRecord(record);
                assertEquals(txId, record.getTransactionId());
                ++txId;
            }
            DLSN dlsn = record.getDlsn();
            assertEquals(1L, dlsn.getLogSegmentSequenceNo());
            assertEquals(entryId, dlsn.getEntryId());
            record = entryReader.nextRecord();
        }
        ++entryId;
        assertEquals(2L, txId);
        assertEquals(reader.getLastAddConfirmed(), reader.readAheadEntries.size());
        assertEquals((reader.getLastAddConfirmed() + 1), reader.getNextEntryId());
        assertFalse(reader.hasCaughtUpOnInprogress());

        Utils.close(reader);
    }

    @Test(timeout = 60000)
    public void testReadEntriesFromInprogressSegment() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setNumPrefetchEntriesPerLogSegment(20);
        confLocal.setMaxPrefetchEntriesPerLogSegment(20);
        DistributedLogManager dlm = createNewDLM(confLocal, runtime.getMethodName());
        AsyncLogWriter writer = createInprogressLogSegment(dlm, confLocal, 5);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(segments.size() + " log segments found, expected to be only one",
            1, segments.size());

        BKLogSegmentEntryReader reader = createEntryReader(segments.get(0), 0, confLocal);
        reader.start();

        long expectedLastAddConfirmed = 8L;
        // wait until sending out all prefetch requests
        while (reader.readAheadEntries.size() < expectedLastAddConfirmed + 2) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals(expectedLastAddConfirmed + 2, reader.getNextEntryId());

        long txId = 1L;
        long entryId = 0L;
        while (true) {
            Entry.Reader entryReader = Utils.ioResult(reader.readNext(1)).get(0);
            LogRecordWithDLSN record = entryReader.nextRecord();
            while (null != record) {
                if (!record.isControl()) {
                    DLMTestUtil.verifyLogRecord(record);
                    assertEquals(txId, record.getTransactionId());
                    ++txId;
                }
                DLSN dlsn = record.getDlsn();
                assertEquals(1L, dlsn.getLogSegmentSequenceNo());
                assertEquals(entryId, dlsn.getEntryId());
                record = entryReader.nextRecord();
            }
            ++entryId;
            if (entryId == expectedLastAddConfirmed + 1) {
                break;
            }
        }
        assertEquals(6L, txId);

        CompletableFuture<List<Entry.Reader>> nextReadFuture = reader.readNext(1);
        // write another record to commit previous writes
        Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(txId)));
        // the long poll will be satisfied
        List<Entry.Reader> nextReadEntries = Utils.ioResult(nextReadFuture);
        assertEquals(1, nextReadEntries.size());
        assertTrue(reader.hasCaughtUpOnInprogress());
        Entry.Reader entryReader = nextReadEntries.get(0);
        LogRecordWithDLSN record = entryReader.nextRecord();
        assertNotNull(record);
        assertTrue(record.isControl());
        assertNull(entryReader.nextRecord());
        // once the read is advanced, we will prefetch next record
        while (reader.getNextEntryId() <= entryId) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals(entryId + 2, reader.getNextEntryId());
        assertEquals(1, reader.readAheadEntries.size());

        Utils.close(reader);
        Utils.close(writer);
    }

    @Test(timeout = 60000)
    public void testReadEntriesOnStateChange() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setNumPrefetchEntriesPerLogSegment(20);
        confLocal.setMaxPrefetchEntriesPerLogSegment(20);
        DistributedLogManager dlm = createNewDLM(confLocal, runtime.getMethodName());
        AsyncLogWriter writer = createInprogressLogSegment(dlm, confLocal, 5);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(segments.size() + " log segments found, expected to be only one",
            1, segments.size());

        BKLogSegmentEntryReader reader = createEntryReader(segments.get(0), 0, confLocal);
        reader.start();

        long expectedLastAddConfirmed = 8L;
        // wait until sending out all prefetch requests
        while (reader.readAheadEntries.size() < expectedLastAddConfirmed + 2) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals(expectedLastAddConfirmed + 2, reader.getNextEntryId());

        long txId = 1L;
        long entryId = 0L;
        while (true) {
            Entry.Reader entryReader = Utils.ioResult(reader.readNext(1)).get(0);
            LogRecordWithDLSN record = entryReader.nextRecord();
            while (null != record) {
                if (!record.isControl()) {
                    DLMTestUtil.verifyLogRecord(record);
                    assertEquals(txId, record.getTransactionId());
                    ++txId;
                }
                DLSN dlsn = record.getDlsn();
                assertEquals(1L, dlsn.getLogSegmentSequenceNo());
                assertEquals(entryId, dlsn.getEntryId());
                record = entryReader.nextRecord();
            }
            ++entryId;
            if (entryId == expectedLastAddConfirmed + 1) {
                break;
            }
        }
        assertEquals(6L, txId);

        CompletableFuture<List<Entry.Reader>> nextReadFuture = reader.readNext(1);
        // write another record to commit previous writes
        Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(txId)));
        // the long poll will be satisfied
        List<Entry.Reader> nextReadEntries = Utils.ioResult(nextReadFuture);
        assertEquals(1, nextReadEntries.size());
        Entry.Reader entryReader = nextReadEntries.get(0);
        LogRecordWithDLSN record = entryReader.nextRecord();
        assertNotNull(record);
        assertTrue(record.isControl());
        assertNull(entryReader.nextRecord());
        // once the read is advanced, we will prefetch next record
        while (reader.getNextEntryId() <= entryId) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals(entryId + 2, reader.getNextEntryId());
        assertEquals(1, reader.readAheadEntries.size());

        // advance the entry id
        ++entryId;
        // close the writer, the write will be committed
        Utils.close(writer);
        entryReader = Utils.ioResult(reader.readNext(1)).get(0);
        record = entryReader.nextRecord();
        assertNotNull(record);
        assertFalse(record.isControl());
        assertNull(entryReader.nextRecord());
        while (reader.getNextEntryId() <= entryId + 1) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals(entryId + 2, reader.getNextEntryId());
        assertEquals(1, reader.readAheadEntries.size());

        // get the new log segment
        List<LogSegmentMetadata> newSegments = dlm.getLogSegments();
        assertEquals(1, newSegments.size());
        assertFalse(newSegments.get(0).isInProgress());
        reader.onLogSegmentMetadataUpdated(newSegments.get(0));
        // when reader received the new log segments. the outstanding long poll
        // should be cancelled and end of log segment should be signaled correctly
        try {
            // when we closed the log segment, another control record will be
            // written, so we loop over the reader until we reach end of log segment.
            Utils.ioResult(reader.readNext(1));
            Utils.ioResult(reader.readNext(1));
            fail("Should reach end of log segment");
        } catch (EndOfLogSegmentException eol) {
            // expected
        }
        Utils.close(reader);
    }
    **/

}
