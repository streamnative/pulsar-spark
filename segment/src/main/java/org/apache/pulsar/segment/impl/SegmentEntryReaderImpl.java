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
package org.apache.pulsar.segment.impl;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BKException.Code;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.pulsar.api.segment.EndOfSegmentException;
import org.apache.pulsar.api.segment.ReadCancelledException;
import org.apache.pulsar.api.segment.Segment;
import org.apache.pulsar.api.segment.SegmentEntryReader;
import org.apache.pulsar.api.segment.SegmentStateListener;
import org.apache.pulsar.api.segment.SegmentStore;

/**
 * A default implementation of {@link SegmentEntryReaderImpl}.
 *
 * <p>The implementation is based on <a>
 * https://github.com/apache/bookkeeper/blob/master/stream/distributedlog/core/src/main/java/org/apache/distributedlog/impl/logsegment/BKLogSegmentEntryReader.java</a>.
 */
@Slf4j
class SegmentEntryReaderImpl implements SafeRunnable, SegmentEntryReader {

    private class CacheEntry implements SafeRunnable {

        private final long entryId;
        private boolean done;
        private LedgerEntry entry;
        private int rc;

        private CacheEntry(long entryId) {
            this.entryId = entryId;
            this.entry = null;
            this.rc = BKException.Code.UnexpectedConditionException;
            this.done = false;
        }

        long getEntryId() {
            return entryId;
        }

        synchronized boolean isDone() {
            return done;
        }

        synchronized void release() {
            if (null != this.entry) {
                this.entry.close();
                this.entry = null;
            }
        }

        void complete(LedgerEntry entry) {
            // the reader is already closed
            if (isClosed()) {
                entry.close();
                return;
            }
            synchronized (this) {
                if (done) {
                    return;
                }
                this.rc = BKException.Code.OK;
                this.entry = entry;
            }
            setDone(true);
        }

        void completeExceptionally(int rc) {
            synchronized (this) {
                if (done) {
                    return;
                }
                this.rc = rc;
            }
            setDone(false);
        }

        void setDone(boolean success) {
            synchronized (this) {
                this.done = true;
            }
            onReadEntryDone(success);
        }

        synchronized boolean isSuccess() {
            return BKException.Code.OK == rc;
        }

        synchronized LedgerEntry getEntry() {
            return this.entry.duplicate();
        }

        synchronized int getRc() {
            return rc;
        }

        void processReadEntries(LedgerEntries entries) {
            try {
                if (isDone()) {
                    return;
                }
                resetReadError();

                LedgerEntry entry = entries.getEntry(entryId);
                if (null == entry || entry.getEntryId() != entryId) {
                    completeExceptionally(Code.UnexpectedConditionException);
                } else {
                    complete(entry.duplicate());
                }
            } finally {
                entries.close();
            }
        }

        void processReadEntry(LastConfirmedAndEntry entry) {
            try {
                if (isDone()) {
                    return;
                }
                resetReadError();
                if (entry.hasEntry() && this.entryId == entry.getEntry().getEntryId()) {
                    complete(entry.getEntry());
                } else {
                    // the long poll is timeout or interrupted; we will retry it again.
                    issueRead(this);
                }
            } finally {
                entry.close();
            }
        }

        private void resetReadError() {
            numReadErrorsUpdater.set(SegmentEntryReaderImpl.this, 0);
        }

        /**
         * Check return code and retry if needed.
         *
         * @param rc the return code
         * @param isLongPoll is it a long poll request
         */
        void checkReturnCodeAndHandleFailure(int rc, boolean isLongPoll) {
            if (isDone()) {
                return;
            }

            if (BKException.Code.BookieHandleNotAvailableException == rc
                    || (isLongPoll && BKException.Code.NoSuchLedgerExistsException == rc)) {
                int numErrors = Math.max(1, numReadErrorsUpdater.incrementAndGet(SegmentEntryReaderImpl.this));
                int nextReadBackoffTime = Math.min(numErrors * readAheadWaitTime, maxReadBackoffTime);
                scheduler.scheduleOrdered(
                        getSegment().name(),
                        this,
                        nextReadBackoffTime,
                        TimeUnit.MILLISECONDS);
            } else {
                completeExceptionally(rc);
            }
        }

        @Override
        public void safeRun() {
            issueRead(this);
        }
    }

    /**
     * Represents a pending read request.
     */
    private class PendingReadRequest {
        private final int numEntries;
        private final List<LedgerEntry> entries;
        private final CompletableFuture<List<LedgerEntry>> promise;

        PendingReadRequest(int numEntries) {
            this.numEntries = numEntries;
            if (numEntries == 1) {
                this.entries = new ArrayList<>(1);
            } else {
                this.entries = new ArrayList<>();
            }
            this.promise = new CompletableFuture<>();
        }

        CompletableFuture<List<LedgerEntry>> getPromise() {
            return promise;
        }

        void completeExceptionally(Throwable throwable) {
            FutureUtils.completeExceptionally(promise, throwable);
        }

        void addEntry(LedgerEntry entry) {
            entries.add(entry);
        }

        void complete() {
            FutureUtils.complete(promise, entries);
            onEntriesConsumed(entries.size());
        }

        boolean hasReadEntries() {
            return entries.size() > 0;
        }

        boolean hasReadEnoughEntries() {
            return entries.size() >= numEntries;
        }
    }

    private final SegmentStore store;
    private final OrderedScheduler scheduler;
    // settings
    private final int numPrefetchEntries;
    private final int maxPrefetchEntries;
    private final long readLacTimeoutMs;
    // state
    private CompletableFuture<Void> closePromise = null;
    private Segment metadata;
    private ReadHandle lh;
    private final List<ReadHandle> openLedgerHandles;
    private CacheEntry outstandingLongPoll;
    private long nextEntryId;
    private static final AtomicReferenceFieldUpdater<SegmentEntryReaderImpl, Throwable> lastExceptionUpdater =
        AtomicReferenceFieldUpdater.newUpdater(SegmentEntryReaderImpl.class, Throwable.class, "lastException");
    private volatile Throwable lastException = null;
    private static final AtomicLongFieldUpdater<SegmentEntryReaderImpl> scheduleCountUpdater =
        AtomicLongFieldUpdater.newUpdater(SegmentEntryReaderImpl.class, "scheduleCount");
    private volatile long scheduleCount = 0L;
    private volatile boolean hasCaughtupOnInprogress = false;
    private final CopyOnWriteArraySet<SegmentStateListener> stateChangeListeners =
            new CopyOnWriteArraySet<>();
    // read retries
    private int readAheadWaitTime;
    private final int maxReadBackoffTime;
    private static final AtomicIntegerFieldUpdater<SegmentEntryReaderImpl> numReadErrorsUpdater =
        AtomicIntegerFieldUpdater.newUpdater(SegmentEntryReaderImpl.class, "numReadErrors");
    private volatile int numReadErrors = 0;
    // readahead cache
    private int cachedEntries = 0;
    private int numOutstandingEntries = 0;
    private final LinkedBlockingQueue<CacheEntry> readAheadEntries;
    // request queue
    private final LinkedList<PendingReadRequest> readQueue;

    SegmentEntryReaderImpl(Segment metadata,
                           ReadHandle lh,
                           long startEntryId,
                           SegmentStore store,
                           OrderedScheduler scheduler,
                           int numPrefetchEntries,
                           int maxPrefetchEntries,
                           int readAheadWaitTime,
                           long readLacTimeoutMs) {
        this.metadata = metadata;
        this.lh = lh;
        this.nextEntryId = Math.max(startEntryId, 0);
        this.store = store;
        this.numPrefetchEntries = numPrefetchEntries;
        this.maxPrefetchEntries = maxPrefetchEntries;
        this.scheduler = scheduler;
        this.openLedgerHandles = Lists.newArrayList();
        this.openLedgerHandles.add(lh);
        this.outstandingLongPoll = null;
        // create the readahead queue
        this.readAheadEntries = new LinkedBlockingQueue<>();
        // create the read request queue
        this.readQueue = new LinkedList<>();
        // read backoff settings
        this.readAheadWaitTime = readAheadWaitTime;
        this.maxReadBackoffTime = 4 * readAheadWaitTime;
        this.readLacTimeoutMs = readLacTimeoutMs;
    }

    public synchronized CacheEntry getOutstandingLongPoll() {
        return outstandingLongPoll;
    }

    LinkedBlockingQueue<CacheEntry> getReadAheadEntries() {
        return this.readAheadEntries;
    }

    synchronized ReadHandle getLh() {
        return lh;
    }

    public synchronized Segment getSegment() {
        return metadata;
    }

    synchronized long getNextEntryId() {
        return nextEntryId;
    }

    @Override
    public void start() {
        prefetchIfNecessary();
    }

    public boolean hasCaughtUpOnInprogress() {
        return hasCaughtupOnInprogress;
    }

    public SegmentEntryReaderImpl registerListener(SegmentStateListener listener) {
        stateChangeListeners.add(listener);
        return this;
    }

    public SegmentEntryReaderImpl unregisterListener(SegmentStateListener listener) {
        stateChangeListeners.remove(listener);
        return this;
    }

    private void notifyCaughtupOnInprogress() {
        for (SegmentStateListener listener : stateChangeListeners) {
            listener.onCaughtupOnInprogress();
        }
    }

    //
    // Process on Log Segment Metadata Updates
    //

    public synchronized void onLogSegmentMetadataUpdated(Segment segment) {
        if (metadata == segment || metadata.equals(segment)) {
            return;
        }
        // segment is changed, then re-open the log segment
        store.openRandomAccessEntryReader(segment)
            .whenComplete(new FutureEventListener<ReadHandle>() {
                @Override
                public void onSuccess(ReadHandle value) {
                    openComplete(segment, value);
                }

                @Override
                public void onFailure(Throwable cause) {
                    failOrRetryOpenLedger(segment);
                }
            });
    }

    private void openComplete(Segment segment, ReadHandle rh) {
        // switch to new ledger handle if the log segment is moved to completed.
        CacheEntry longPollRead = null;
        synchronized (this) {
            if (isClosed()) {
                rh.closeAsync().whenComplete((ignored, cause) -> {
                    log.debug("Close the open ledger {} since the log segment reader is already closed",
                        lh.getId());
                });
                return;
            }
            this.metadata = segment;
            this.lh = rh;
            this.openLedgerHandles.add(lh);
            longPollRead = outstandingLongPoll;
        }
        if (null != longPollRead) {
            // reissue the long poll read when the log segment state is changed
            issueRead(longPollRead);
        }
        // notify readers
        notifyReaders();
    }

    private void failOrRetryOpenLedger(final Segment segment) {
        if (isClosed()) {
            return;
        }
        if (isBeyondLastAddConfirmed()) {
            // if the reader is already caught up, let's fail the reader immediately
            // as we need to pull the latest metadata of this log segment.
            completeExceptionally(
                new IOException("Failed to open ledger for reading segment " + getSegment().name()),
                true);
            return;
        }
        // the reader is still catching up, retry opening the log segment later
        scheduler.scheduleOrdered(
            segment.name(),
            () -> onLogSegmentMetadataUpdated(segment),
            10000,
            TimeUnit.MILLISECONDS);
    }

    //
    // Change the state of this reader
    //

    private boolean checkClosedOrInError() {
        Throwable cause = lastExceptionUpdater.get(this);
        if (null != cause) {
            cancelAllPendingReads(cause);
            return true;
        }
        return false;
    }

    /**
     * Set the reader into error state with return code <i>rc</i>.
     *
     * @param throwable exception indicating the error
     * @param isBackground is the reader set exception by background reads or foreground reads
     */
    private void completeExceptionally(Throwable throwable, boolean isBackground) {
        lastExceptionUpdater.compareAndSet(this, null, throwable);
        if (isBackground) {
            notifyReaders();
        }
    }

    /**
     * Notify the readers with the state change.
     */
    private void notifyReaders() {
        processReadRequests();
    }

    private void cancelAllPendingReads(Throwable throwExc) {
        List<PendingReadRequest> requestsToCancel;
        synchronized (readQueue) {
            requestsToCancel = Lists.newArrayListWithExpectedSize(readQueue.size());
            requestsToCancel.addAll(readQueue);
            readQueue.clear();
        }
        for (PendingReadRequest request : requestsToCancel) {
            request.completeExceptionally(throwExc);
        }
    }

    private void releaseAllCachedEntries() {
        synchronized (this) {
            CacheEntry entry = readAheadEntries.poll();
            while (null != entry) {
                entry.release();
                entry = readAheadEntries.poll();
            }
        }
    }

    //
    // Background Read Operations
    //

    private void onReadEntryDone(boolean success) {
        // we successfully read an entry
        synchronized (this) {
            --numOutstandingEntries;
        }
        // notify reader that there is entry ready
        notifyReaders();
        // stop prefetch if we already encountered exceptions
        if (success) {
            prefetchIfNecessary();
        }
    }

    private void onEntriesConsumed(int numEntries) {
        synchronized (this) {
            cachedEntries -= numEntries;
        }
        prefetchIfNecessary();
    }

    private void prefetchIfNecessary() {
        List<CacheEntry> entriesToFetch;
        synchronized (this) {
            if (cachedEntries >= maxPrefetchEntries) {
                return;
            }
            // we don't have enough entries, do prefetch
            int numEntriesToFetch = numPrefetchEntries - numOutstandingEntries;
            if (numEntriesToFetch <= 0) {
                return;
            }
            entriesToFetch = new ArrayList<>(numEntriesToFetch);
            for (int i = 0; i < numEntriesToFetch; i++) {
                if (cachedEntries >= maxPrefetchEntries) {
                    break;
                }
                if ((isLedgerClosed() && nextEntryId > getLastAddConfirmed())
                        || (!isLedgerClosed() && nextEntryId > getLastAddConfirmed() + 1)) {
                    break;
                }
                CacheEntry entry = new CacheEntry(nextEntryId);
                entriesToFetch.add(entry);
                readAheadEntries.add(entry);
                ++numOutstandingEntries;
                ++cachedEntries;
                ++nextEntryId;
            }
        }
        for (CacheEntry entry : entriesToFetch) {
            issueRead(entry);
        }
    }


    private void issueRead(CacheEntry cacheEntry) {
        if (isClosed()) {
            return;
        }
        if (isLedgerClosed()) {
            if (isNotBeyondLastAddConfirmed(cacheEntry.getEntryId())) {
                issueSimpleRead(cacheEntry);
                return;
            } else {
                // Reach the end of stream
                notifyReaders();
            }
        } else { // the ledger is still in progress
            if (isNotBeyondLastAddConfirmed(cacheEntry.getEntryId())) {
                issueSimpleRead(cacheEntry);
            } else {
                issueLongPollRead(cacheEntry);
            }
        }
    }

    private void issueSimpleRead(CacheEntry cacheEntry) {
        getLh().readAsync(cacheEntry.entryId, cacheEntry.entryId)
            .whenComplete(new FutureEventListener<LedgerEntries>() {
                @Override
                public void onSuccess(LedgerEntries entries) {
                    cacheEntry.processReadEntries(entries);
                }

                @Override
                public void onFailure(Throwable cause) {
                    cacheEntry.checkReturnCodeAndHandleFailure(getExceptionCode(cause, Code.ReadException), false);
                }
            });
    }

    private void issueLongPollRead(CacheEntry cacheEntry) {
        // register the read as outstanding reads
        synchronized (this) {
            this.outstandingLongPoll = cacheEntry;
        }

        if (!hasCaughtupOnInprogress) {
            hasCaughtupOnInprogress = true;
            notifyCaughtupOnInprogress();
        }
        getLh().readLastAddConfirmedAndEntryAsync(
            cacheEntry.entryId,
            readLacTimeoutMs,
            false
        ).whenComplete(new FutureEventListener<LastConfirmedAndEntry>() {
            @Override
            public void onSuccess(LastConfirmedAndEntry value) {
                cacheEntry.processReadEntry(value);
            }

            @Override
            public void onFailure(Throwable cause) {
                cacheEntry.checkReturnCodeAndHandleFailure(getExceptionCode(cause, Code.ReadException), true);
            }
        });
    }

    //
    // Foreground Read Operations
    //

    private CompletableFuture<List<LedgerEntry>> readNextAsync(int numEntries) {
        final PendingReadRequest readRequest = new PendingReadRequest(numEntries);

        if (checkClosedOrInError()) {
            readRequest.completeExceptionally(lastExceptionUpdater.get(this));
        } else {
            boolean wasQueueEmpty;
            synchronized (readQueue) {
                wasQueueEmpty = readQueue.isEmpty();
                readQueue.add(readRequest);
            }
            if (wasQueueEmpty) {
                processReadRequests();
            }
        }
        return readRequest.getPromise();
    }

    @Override
    public List<LedgerEntry> readNext() throws EndOfSegmentException, IOException {
        // 1000 is an indicator
        try {
            return FutureUtils.result(readNextAsync(1000));
        } catch (Exception e) {
            if (e instanceof EndOfSegmentException) {
                throw (EndOfSegmentException) e;
            } else if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException("Exception on reading entries from segment '" + getSegment().name() + "'", e);
            }
        }
    }

    private void processReadRequests() {
        if (isClosed()) {
            // the reader is already closed.
            return;
        }

        long prevCount = scheduleCountUpdater.getAndIncrement(this);
        if (0 == prevCount) {
            scheduler.executeOrdered(getSegment().name(), this);
        }
    }

    /**
     * The core function to propagate fetched entries to read requests.
     */
    @Override
    public void safeRun() {
        long scheduleCountLocal = scheduleCountUpdater.get(this);
        while (true) {
            PendingReadRequest nextRequest;
            synchronized (readQueue) {
                nextRequest = readQueue.peek();
            }

            // if read queue is empty, nothing to read, return
            if (null == nextRequest) {
                scheduleCountUpdater.set(this, 0L);
                return;
            }

            // if the oldest pending promise is interrupted then we must
            // mark the reader in error and abort all pending reads since
            // we don't know the last consumed read
            if (null == lastExceptionUpdater.get(this)) {
                if (nextRequest.getPromise().isCancelled()) {
                    completeExceptionally(new ReadCancelledException("Interrupted on reading log segment "
                            + getSegment() + " : " + nextRequest.getPromise().isCancelled()), false);
                }
            }

            // if the reader is in error state, stop read
            if (checkClosedOrInError()) {
                return;
            }

            // read entries from readahead cache to satisfy next read request
            readEntriesFromReadAheadCache(nextRequest);

            // check if we can satisfy the read request
            if (nextRequest.hasReadEntries()) {
                PendingReadRequest request;
                synchronized (readQueue) {
                    request = readQueue.poll();
                }
                if (null != request && nextRequest == request) {
                    request.complete();
                } else {
                    IllegalStateException ise = new IllegalStateException("Unexpected condition at reading from "
                            + getSegment());
                    nextRequest.completeExceptionally(ise);
                    if (null != request) {
                        request.completeExceptionally(ise);
                    }
                    completeExceptionally(ise, false);
                }
            } else {
                if (0 == scheduleCountLocal) {
                    return;
                }
                scheduleCountLocal = scheduleCountUpdater.decrementAndGet(this);
            }
        }
    }

    private void readEntriesFromReadAheadCache(PendingReadRequest nextRequest) {
        while (!nextRequest.hasReadEnoughEntries()) {
            CacheEntry entry;
            boolean hitEndOfLogSegment;
            synchronized (this) {
                entry = readAheadEntries.peek();
                hitEndOfLogSegment = (null == entry) && isEndOfLogSegment();
            }
            // reach end of log segment
            if (hitEndOfLogSegment && !nextRequest.hasReadEntries()) {
                completeExceptionally(
                    new EndOfSegmentException("Reach end of segment '" + getSegment().name() + "'"), false);
                return;
            }
            if (null == entry) {
                return;
            }
            // entry is not complete yet.
            if (!entry.isDone()) {
                // we already reached end of the log segment
                if (isEndOfLogSegment(entry.getEntryId())) {
                    completeExceptionally(
                        new EndOfSegmentException("Reach end of segment '" + getSegment().name() + "'"), false);
                }
                return;
            }
            if (entry.isSuccess()) {
                CacheEntry removedEntry = readAheadEntries.poll();
                try {
                    if (entry != removedEntry) {
                        IllegalStateException ise =
                                new IllegalStateException("Unexpected condition at reading from " + getSegment());
                        completeExceptionally(ise, false);
                        return;
                    }
                    // the reference is retained on `entry.getEntry()`.
                    nextRequest.addEntry(entry.getEntry());
                } finally {
                    removedEntry.release();
                }
            } else {
                completeExceptionally(new IOException("Encountered issue on reading entry " + entry.getEntryId()
                    + " @ log segment " + getSegment() + ": rc = " + entry.getRc()
                    + ", reason = " + BKException.getMessage(entry.getRc())), false);
                return;
            }
        }
    }

    //
    // State Management
    //

    private synchronized boolean isEndOfLogSegment() {
        return isEndOfLogSegment(nextEntryId);
    }

    private boolean isEndOfLogSegment(long entryId) {
        return isLedgerClosed() && entryId > getLastAddConfirmed();
    }

    public synchronized boolean isBeyondLastAddConfirmed() {
        return isBeyondLastAddConfirmed(nextEntryId);
    }

    private boolean isBeyondLastAddConfirmed(long entryId) {
        return entryId > getLastAddConfirmed();
    }

    private boolean isNotBeyondLastAddConfirmed(long entryId) {
        return entryId <= getLastAddConfirmed();
    }

    private boolean isLedgerClosed() {
        return getLh().isClosed();
    }

    public long getLastAddConfirmed() {
        return getLh().getLastAddConfirmed();
    }

    synchronized boolean isClosed() {
        return null != closePromise;
    }

    public CompletableFuture<Void> closeAsync() {
        final CompletableFuture<Void> closeFuture;
        ReadCancelledException exception;
        List<ReadHandle> lhsToClose;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closeFuture = closePromise = new CompletableFuture<>();
            lhsToClose = openLedgerHandles;
            openLedgerHandles.clear();
            // set the exception to cancel pending and subsequent reads
            exception = new ReadCancelledException("Reader of segment '" + metadata.name() + "' was closed");
            completeExceptionally(exception, false);
        }

        // release the cached entries
        releaseAllCachedEntries();

        // cancel all pending reads
        cancelAllPendingReads(exception);

        // close all the open ledger
        FutureUtils.proxyTo(
            FutureUtils.collect(
                lhsToClose
                    .stream()
                    .map(ReadHandle::closeAsync)
                    .collect(Collectors.toList()))
                .thenApply(ignored -> (Void) null),
            closeFuture
        );
        return closeFuture;
    }

    @Override
    public void close() {
        try {
            FutureUtils.result(closeAsync(), 1, TimeUnit.MINUTES);
        } catch (Exception e) {
            log.warn("Exception on closing segment '{}'", metadata.name(), e);
        }
    }

    // TODO: replace with BKException util method when a newer version is available.

    /**
     * Extract an exception code from an BKException, or use a default if it's another type.
     */
    public static int getExceptionCode(Throwable t, int defaultCode) {
        if (t instanceof BKException) {
            return ((BKException) t).getCode();
        } else if (t.getCause() != null) {
            return getExceptionCode(t.getCause(), defaultCode);
        } else {
            return defaultCode;
        }
    }
}
