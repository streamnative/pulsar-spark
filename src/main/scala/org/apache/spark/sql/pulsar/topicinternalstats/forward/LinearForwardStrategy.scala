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
 * Simple forward strategy, which forwards every topic evenly, not
 * taking actual backlog sizes into account. Might waste bandwidth
 * when the backlog of the topic is smaller than the calculated value
 * for that topic.
 *
 * If the maximum entries to forward is `150`, topics will be forwarded
 * like this (provided there is no minimum entry number specified:
 * | topic name | backlog size | forward amount |
 * |------------|--------------|----------------|
 * | topic-1    |           60 |             50 |
 * | topic-2    |           50 |             50 |
 * | topic-3    |           40 |             50 |
 *
 * @param maxEntriesAltogetherToForward Maximum entries in all topics to
 *                                      forward. Will forward every topic
 *                                      by dividing this with the number of
 *                                      topics.
 */
class LinearForwardStrategy(maxEntriesAltogetherToForward: Long) extends ForwardStrategy {
  override def forward(topics: Map[String, TopicState]): Map[String, Long] =
    topics
      .map{ case (topicName, _) =>
        topicName -> (maxEntriesAltogetherToForward / topics.size) }
}
