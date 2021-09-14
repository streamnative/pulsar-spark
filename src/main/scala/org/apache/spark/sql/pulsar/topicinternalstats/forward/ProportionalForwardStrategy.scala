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
 * This forward strategy will forward individual topic backlogs based on
 * their size proportional to the size of the overall backlog (considering
 * all topics).
 *
 * If the maximum entries to forward is `100`, topics will be forwarded
 * like this (provided there is no minimum entry number specified:
 * | topic name | backlog size | forward amount           |
 * |------------|--------------|--------------------------|
 * |topic-1     |           60 | 100*(60/(60+50+40)) = 40 |
 * |topic-2     |           50 | 100*(50/(60+50+40)) = 33 |
 * |topic-3     |           40 | 100*(40/(60+50+40)) = 27 |
 *
 * If @param ensureEntriesPerTopic is specified, then every topic will be
 * forwarded by that value in addition to this (taking the backlog size of
 * the topic into account so that bandwidth is not wasted).
 * Given maximum entries is `100`, minimum entries is `10`, topics will be
 * forwarded like this:
 *
 * | topic name | backlog size |             forward amount |
 * |------------|--------------|----------------------------|
 * |topic-1     |           60 | 10+70*(60/(60+50+40)) = 38 |
 * |topic-2     |           50 | 10+70*(50/(60+50+40)) = 33 |
 * |topic-3     |           40 | 10+70*(40/(60+50+40)) = 29 |
 *
 * @param maxEntriesAltogetherToForward Maximum entries in all topics to forward.
 *                                      Individual topics forward values will sum
 *                                      up to this value.
 * @param ensureEntriesPerTopic All topics will be forwarded by this value. The goal
 *                              of this parameter is to ensure that topics with a very
 *                              small backlog are also forwarded with a given minimal
 *                              value. Has a higher precedence than
 *                              @param maxEntriesAltogetherToForward.
 */
class ProportionalForwardStrategy(maxEntriesAltogetherToForward: Long,
                                  ensureEntriesPerTopic: Long) extends ForwardStrategy {
  override def forward(topics: Map[String, TopicState]): Map[String, Long] = {
    // calculate all remaining entries per topic
    val topicBacklogs = topics
      .map{
        case (topicName, topicStat) =>
          val internalStat = topicStat.internalStat
          val ledgerId = topicStat.actualLedgerId
          val entryId = topicStat.actualEntryId
          (topicName, TopicInternalStatsUtils.numOfEntriesAfter(internalStat, ledgerId, entryId))
        }
      .toList

    // this is the size of the complete backlog (the sum of all individual topic
    // backlogs)
    val completeBacklogSize = topicBacklogs
      .map{ case (_, topicBacklogSize) => topicBacklogSize }
      .sum

    // calculate quota based on the ensured entry count
    // this will be distributed between individual topics
    val quota = Math.max(maxEntriesAltogetherToForward - ensureEntriesPerTopic * topics.size, 0)

    topicBacklogs.map {
      case (topicName: String, backLog: Long) =>
        // when calculating the coefficient, do not take the number of additional entries into
        // account (that we will add anyway)
        val topicBacklogCoefficient = if (completeBacklogSize == 0) {
          0.0 // do not forward if there is no backlog
        } else {
          // take the ensured entries into account when calculating
          // backlog coefficient
          val backlogWithoutAdditionalEntries =
            Math.max(backLog - ensureEntriesPerTopic, 0).toFloat
          val completeBacklogWithoutAdditionalEntries =
            (completeBacklogSize - ensureEntriesPerTopic * topics.size).toFloat
          backlogWithoutAdditionalEntries / completeBacklogWithoutAdditionalEntries
        }
        topicName -> (ensureEntriesPerTopic + (quota * topicBacklogCoefficient).toLong)
    }.toMap
  }
}
