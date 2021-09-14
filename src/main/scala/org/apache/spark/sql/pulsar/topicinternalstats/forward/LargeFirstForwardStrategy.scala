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
package org.apache.spark.sql.pulsar.topicinternalstats.forward

/**
 * Forward strategy which sorts the topics by their backlog size starting
 * with the largest, and forwards topics starting from the beginning of
 * this list as the maximum entries parameter allows (taking into account
 * the number entries that need to be added anyway if
 *
 * @param additionalEntriesPerTopic is set).
 *
 * If the maximum entries to forward is `100`, topics will be forwarded
 * like this (provided there is no minimum entry number specified:
 * | topic name | backlog size | forward amount |
 * |------------|--------------|----------------|
 * | topic-1    |           60 |             60 |
 * | topic-2    |           50 |             40 |
 * | topic-3    |           40 |              0 |
 *
 * If @param ensureEntriesPerTopic is specified, then every topic will be
 * forwarded by that value in addition to this (taking the backlog size of
 * the topic into account so that bandwidth is not wasted). Given maximum
 * entries is `100`, minimum entries is `10`, topics will be forwarded like
 * this:
 *
 * | topic name | backlog size | forward amount |
 * |------------|--------------|----------------|
 * | topic-1    |           60 |   10 + 50 = 60 |
 * | topic-2    |           50 |   10 + 30 = 30 |
 * | topic-3    |           40 |    10 + 0 = 10 |
 * @param maxEntriesAltogetherToForward Maximum entries in all topics to forward.
 *                                      Individual topics forward values will sum
 *                                      up to this value.
 * @param ensureEntriesPerTopic All topics will be forwarded by this value. The goal
 *                              of this parameter is to ensure that topics with a very
 *                              small backlog are also forwarded with a given minimal
 *                              value. Has a higher precedence than
 *                              @param maxEntriesAltogetherToForward.
 */
class LargeFirstForwardStrategy(maxEntriesAltogetherToForward: Long,
                                ensureEntriesPerTopic: Long) extends ForwardStrategy {
  override def forward(topics: Map[String, TopicState]): Map[String, Long] = {

    // calculate all remaining entries per topic, ordering them by remaining entry count
    // in a reverse order
    val topicBacklogs = topics
      .map{
        case(topicName, topicStat) =>
          val internalStat = topicStat.internalStat
          val ledgerId = topicStat.actualLedgerId
          val entryId = topicStat.actualEntryId
          (topicName, TopicInternalStatsUtils.numOfEntriesAfter(internalStat, ledgerId, entryId))
      }
      .toList
      .sortBy{ case(_, numOfEntriesAfterPosition) => numOfEntriesAfterPosition }
      .reverse

    // calculate quota based on the ensured entry count
    // this will be distributed between individual topics
    var quota = Math.max(maxEntriesAltogetherToForward - ensureEntriesPerTopic * topics.size, 0)

    val result = for ((topic, topicBacklogSize) <- topicBacklogs) yield {
      // try to increase topic by this number
      // - if we have already ran out of quota, do not move topic
      // - if we do not have enough quota, proceed with the quota (exhaust it completely)
      // - if we have enough quota, exhaust all topic content (and decrease it later)
      // - take the number of ensured entries into account when calculating quota
      val forwardTopicBy = if (quota > 0) {
        Math.min(quota, topicBacklogSize - ensureEntriesPerTopic)
      } else {
        0
      }
      // calculate forward position for a topic, make sure that it is
      // always increased by the configured ensure entry count
      val resultEntry = topic -> (ensureEntriesPerTopic + forwardTopicBy)
      // decrease the overall quota separately
      quota -= (topicBacklogSize - ensureEntriesPerTopic)
      // return already calculated forward position
      resultEntry
    }

    result.toMap
  }
}
