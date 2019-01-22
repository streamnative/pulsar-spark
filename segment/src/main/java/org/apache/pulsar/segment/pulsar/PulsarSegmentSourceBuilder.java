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
package org.apache.pulsar.segment.pulsar;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.api.BKException.Code;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ManagedLedgerInfoCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.api.segment.Segment;
import org.apache.pulsar.api.segment.SegmentSource;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.segment.impl.ListSegmentSource;

/**
 * Builder to build a pulsar {@link SegmentSource}.
 */
public class PulsarSegmentSourceBuilder {

    /**
     * Create a pulsar segment source builder.
     *
     * @return a pulsar segment source builder.
     */
    public static PulsarSegmentSourceBuilder newBuilder() {
        return new PulsarSegmentSourceBuilder();
    }

    private String adminUrl;
    private final Set<String> topics = new HashSet<>();

    private PulsarSegmentSourceBuilder() {}

    /**
     * Set the admin url.
     *
     * @param adminUrl admin url
     * @return pulsar segment source builder.
     */
    public PulsarSegmentSourceBuilder withAdminUrl(String adminUrl) {
        this.adminUrl = requireNonNull(adminUrl, "Admin Url is null");
        return this;
    }

    /**
     * Add the topic.
     *
     * @param topic topic name
     * @return pulsar segment source builder.
     */
    public PulsarSegmentSourceBuilder withTopic(String topic) {
        this.topics.add(requireNonNull(topic, "Topic is null"));
        return this;
    }

    /**
     * Add the topics to segment source builder.
     *
     * @param topics list of topics to add
     * @return pulsar segment source builder.
     */
    public PulsarSegmentSourceBuilder withTopics(String... topics) {
        for (String topic : topics) {
            this.topics.add(requireNonNull(topic, "Topic is null"));
        }
        return this;
    }

    /**
     * Build the segment source.
     *
     * @return segment source
     */
    @SuppressWarnings("deprecation")
    public SegmentSource build() throws Exception {
        requireNonNull(adminUrl, "Admin Url is not set");
        checkArgument(!topics.isEmpty(), "No topic is set");

        try (PulsarAdmin admin = PulsarAdmin.builder()
             .serviceHttpUrl(adminUrl)
             .build()) {
            InternalConfigurationData conf = admin.brokers().getInternalConfigurationData();

            ClientConfiguration bkClientConf = new ClientConfiguration()
                .setZkServers(conf.getZookeeperServers())
                .setAllowShadedLedgerManagerFactoryClass(true)
                .setShadedLedgerManagerFactoryClassPrefix("org.apache.pulsar.shade.");

            ManagedLedgerFactoryImpl mlFactory = new ManagedLedgerFactoryImpl(bkClientConf);
            try {
                BookKeeper bk = mlFactory.getBookKeeper();
                MetadataClientDriver clientDriver = bk.getMetadataClientDriver();
                LedgerManagerFactory lmFactory = clientDriver.getLedgerManagerFactory();
                try (LedgerManager lm = lmFactory.newLedgerManager()) {
                    List<Segment> segments = result(
                        getSegmentsFromPulsarTopics(
                            topics.stream().collect(Collectors.toList()),
                            admin,
                            mlFactory,
                            lm)
                    );
                    return ListSegmentSource.of(segments);
                }
            } finally {
                mlFactory.shutdown();
            }
        }
    }

    private static CompletableFuture<List<Segment>> getSegmentsFromPulsarTopics(List<String> topics,
                                                                                PulsarAdmin admin,
                                                                                ManagedLedgerFactory mlFactory,
                                                                                LedgerManager lm) throws Exception {
        List<CompletableFuture<List<Segment>>> getFutures = topics.stream()
            .map(topic -> {
                TopicName tn = TopicName.get(topic);
                try {
                    return getSegmentsFromPulsarTopic(tn, admin, mlFactory, lm);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to get partitioned metadata of topic " + topic, e);
                }
            })
            .collect(Collectors.toList());

        return FutureUtils.collect(getFutures)
            .thenApply(segmentsList -> segmentsList.stream().flatMap(List::stream).collect(Collectors.toList()));
    }



    private static CompletableFuture<List<Segment>> getSegmentsFromPulsarTopic(TopicName topicName,
                                                                               PulsarAdmin admin,
                                                                               ManagedLedgerFactory mlFactory,
                                                                               LedgerManager lm) throws Exception {
        PartitionedTopicMetadata ptMetadata = admin.topics().getPartitionedTopicMetadata(topicName.toString());
        if (ptMetadata.partitions > 0) {
            return getSegmentsPartitionedTopic(topicName, ptMetadata.partitions, mlFactory, lm);
        } else {
            return getSegmentsNonPartitionedTopic(topicName, mlFactory, lm);
        }
    }


    private static CompletableFuture<List<Segment>> getSegmentsNonPartitionedTopic(TopicName topicName,
                                                                                   ManagedLedgerFactory mlFactory,
                                                                                   LedgerManager lm) {
        return new ManagedLedgerSegmentProvider(
            topicName, -1, mlFactory, lm
        ).get();
    }

    private static CompletableFuture<List<Segment>> getSegmentsPartitionedTopic(TopicName topicName,
                                                                                int numPartitions,
                                                                                ManagedLedgerFactory mlFactory,
                                                                                LedgerManager lm) {
        List<CompletableFuture<List<Segment>>> getFutures = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            getFutures.add(new ManagedLedgerSegmentProvider(
                topicName, i, mlFactory, lm
            ).get());
        }
        return FutureUtils.collect(getFutures)
            .thenApply(segmentsList ->
                segmentsList.stream().flatMap(List::stream).collect(Collectors.toList()));
    }

    /**
     * A provider that provides a list of segments.
     */
    private interface SegmentProvider {

        /**
         * Returns the list of segments.
         *
         * @return the list of segments
         */
        CompletableFuture<List<Segment>> get();

    }

    static class ManagedLedgerSegmentProvider implements SegmentProvider {

        protected final TopicName topicName;
        protected final int partitionIdx;
        protected final String mlName;
        protected final ManagedLedgerFactory mlFactory;
        protected final LedgerManager lm;

        protected ManagedLedgerSegmentProvider(TopicName topicName,
                                               int partitionIdx,
                                               ManagedLedgerFactory mlFactory,
                                               LedgerManager lm) {
            this.topicName = topicName;
            this.partitionIdx = partitionIdx;
            if (partitionIdx < 0) {
                this.mlName = topicName.getPersistenceNamingEncoding();
            } else {
                this.mlName = topicName.getPartition(partitionIdx).getPersistenceNamingEncoding();
            }
            this.mlFactory = mlFactory;
            this.lm = lm;
        }

        @Override
        public CompletableFuture<List<Segment>> get() {
            CompletableFuture<List<Segment>> future = new CompletableFuture<>();
            mlFactory.asyncGetManagedLedgerInfo(mlName, new ManagedLedgerInfoCallback() {
                @Override
                public void getInfoComplete(ManagedLedgerInfo info, Object ctx) {
                    FutureUtils.proxyTo(
                        processManagedLedgerInfo(info),
                        future
                    );
                }

                @Override
                public void getInfoFailed(ManagedLedgerException exception, Object ctx) {
                    future.completeExceptionally(exception);
                }
            }, null);
            return future;
        }

        private CompletableFuture<List<Segment>> processManagedLedgerInfo(ManagedLedgerInfo mlInfo) {
            final List<LedgerInfo> ledgers = mlInfo.ledgers;
            List<CompletableFuture<LedgerMetadata>> getFutures = ledgers.stream()
                .map(li -> readLedgerMetadata(li))
                .collect(Collectors.toList());

            return FutureUtils.collect(getFutures)
                .thenApply(metadatas -> processLedgerMetadatas(ledgers, metadatas));
        }

        private List<Segment> processLedgerMetadatas(List<LedgerInfo> ledgers, List<LedgerMetadata> metadatas) {
            List<Segment> segments = new ArrayList<>(ledgers.size());
            for (int i = 0; i < ledgers.size(); i++) {
                LedgerInfo li = ledgers.get(i);
                LedgerMetadata metadata = metadatas.get(i);

                PulsarSegment segment = new PulsarSegment(
                    mlName,
                    partitionIdx,
                    li.ledgerId,
                    metadata.serialize()
                );
                segments.add(segment);
            }
            return segments;
        }

        private CompletableFuture<LedgerMetadata> readLedgerMetadata(LedgerInfo li) {
            CompletableFuture<LedgerMetadata> future = new CompletableFuture<>();
            lm.readLedgerMetadata(li.ledgerId, (rc, metadata) -> {
                if (Code.OK == rc) {
                    future.complete(metadata);
                } else {
                    future.completeExceptionally(BKException.create(rc));
                }
            });
            return future;
        }
    }

}
