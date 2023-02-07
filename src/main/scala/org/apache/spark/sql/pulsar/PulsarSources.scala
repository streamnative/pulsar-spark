/*
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
package org.apache.spark.sql.pulsar

import java.io._
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.impl.{BatchMessageIdImpl, MessageIdImpl, TopicMessageIdImpl}

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, SerializedOffset}
import org.apache.spark.storage.BlockManager

private[pulsar] object PulsarSourceUtils extends Logging {
  import PulsarOptions._

  private[pulsar] val VERSION = 1

  def getSortedExecutorList(blockManager: BlockManager): Array[String] = {
    blockManager.master
      .getPeers(blockManager.blockManagerId)
      .toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compare)
      .map(_.toString)
  }

  def getSortedExecutorList(sc: SparkContext): Array[String] = {
    getSortedExecutorList(sc.env.blockManager)
  }

  def getSortedExecutorList(): Array[String] = {
    getSortedExecutorList(SparkEnv.get.blockManager)
  }

  private def compare(a: ExecutorCacheTaskLocation, b: ExecutorCacheTaskLocation): Boolean = {
    if (a.host == b.host) { a.executorId > b.executorId }
    else { a.host > b.host }
  }

  /**
   * If `failOnDataLoss` is true, this method will throw an `IllegalStateException`. Otherwise,
   * just log a warning.
   */
  def reportDataLossFunc(failOnDataLoss: Boolean): (String) => Unit = { (message: String) =>
    if (failOnDataLoss) {
      throw new IllegalStateException(message + s". $InstructionForFailOnDataLossTrue")
    } else {
      logWarning(message + s". $InstructionForFailOnDataLossFalse")
    }
  }

  // used to check whether starting position and current message we got actually are equal
  // we neglect the potential batchIdx deliberately while seeking to MessageIdImpl for batch entry
  def messageIdRoughEquals(l: MessageId, r: MessageId): Boolean = {
    (l, r) match {
      case (lb: BatchMessageIdImpl, rb: BatchMessageIdImpl) => lb.equals(rb)
      case (lm: MessageIdImpl, rb: BatchMessageIdImpl) =>
        lm.equals(new MessageIdImpl(rb.getLedgerId, rb.getEntryId, rb.getPartitionIndex))
      case (lb: BatchMessageIdImpl, rm: MessageIdImpl) =>
        rm.equals(new MessageIdImpl(lb.getLedgerId, lb.getEntryId, lb.getPartitionIndex))
      case (lm: MessageIdImpl, rm: MessageIdImpl) => lm.equals(rm)
      case _ =>
        throw new IllegalStateException(
          s"comparing messageIds of type [${l.getClass.getName}, ${r.getClass.getName}]")
    }
  }

  def messageExists(mid: MessageId): Boolean = {
    mid match {
      case m: MessageIdImpl => m.getLedgerId != -1 && m.getEntryId != -1
      case t: TopicMessageIdImpl => messageExists(t.getInnerMessageId)
    }
  }

  def enteredEnd(end: MessageId)(current: MessageId): Boolean = {
    val endImpl = end.asInstanceOf[MessageIdImpl]
    val currentImpl = current.asInstanceOf[MessageIdImpl]
    val result = endImpl.getLedgerId == currentImpl.getLedgerId &&
      endImpl.getEntryId == currentImpl.getEntryId

    result
  }

  def isLastMessage(messageId: MessageId): Boolean = {
    messageId match {
      case bmid: BatchMessageIdImpl =>
        bmid.getBatchIndex == bmid.getBatchSize - 1
      case _: MessageIdImpl =>
        true
      case _ =>
        throw new IllegalStateException(
          s"reading a message of type ${messageId.getClass.getName}")
    }
  }

  def mid2Impl(mid: MessageId): MessageIdImpl = {
    mid match {
      case bmid: BatchMessageIdImpl =>
        new MessageIdImpl(bmid.getLedgerId, bmid.getEntryId, bmid.getPartitionIndex)
      case midi: MessageIdImpl => midi
      case t: TopicMessageIdImpl => mid2Impl(t.getInnerMessageId)
      case up: UserProvidedMessageId => mid2Impl(up.mid)
    }
  }

  def getLedgerId(mid: MessageId): Long = {
    mid match {
      case bmid: BatchMessageIdImpl =>
        bmid.getLedgerId
      case midi: MessageIdImpl => midi.getLedgerId
      case t: TopicMessageIdImpl => getLedgerId(t.getInnerMessageId)
      case up: UserProvidedMessageId => up.getLedgerId
    }
  }

  def getEntryId(mid: MessageId): Long = {
    mid match {
      case bmid: BatchMessageIdImpl =>
        bmid.getEntryId
      case midi: MessageIdImpl => midi.getEntryId
      case t: TopicMessageIdImpl => getEntryId(t.getInnerMessageId)
      case up: UserProvidedMessageId => up.getEntryId
    }
  }

  def getPartitionIndex(mid: MessageId): Int = {
    mid match {
      case bmid: BatchMessageIdImpl =>
        bmid.getPartitionIndex
      case midi: MessageIdImpl => midi.getPartitionIndex
      case t: TopicMessageIdImpl => getPartitionIndex(t.getInnerMessageId)
      case up: UserProvidedMessageId => up.getPartitionIndex
    }
  }

  def seekableLatestMid(mid: MessageId): MessageId = {
    if (messageExists(mid)) mid else MessageId.earliest
  }
}

class PulsarSourceInitialOffsetWriter(sparkSession: SparkSession, metadataPath: String)
    extends HDFSMetadataLog[SpecificPulsarOffset](sparkSession, metadataPath) {

  import PulsarSourceUtils._

  override def serialize(metadata: SpecificPulsarOffset, out: OutputStream): Unit = {
    val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
    writer.write("v" + VERSION + "\n")
    writer.write(metadata.json)
    writer.flush()
  }

  override def deserialize(in: InputStream): SpecificPulsarOffset = {
    val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
    // HDFSMetadataLog guarantees that it never creates a partial file.
    assert(content.length != 0)
    if (content(0) == 'v') {
      val indexOfNewLine = content.indexOf("\n")
      if (indexOfNewLine > 0) {

        val version = validateVersion(content.substring(0, indexOfNewLine), VERSION)
        SpecificPulsarOffset(SerializedOffset(content.substring(indexOfNewLine + 1)))
      } else {
        throw new IllegalStateException(
          s"Log file was malformed: failed to detect the log file version line.")
      }
    } else {
      throw new IllegalStateException(
        s"Log file was malformed: failed to detect the log file version line.")
    }
  }

  def getInitialOffset(
      metadataReader: PulsarMetadataReader,
      startingOffsets: PerTopicOffset,
      poolTimeoutMs: Int,
      reportDataLoss: String => Unit): SpecificPulsarOffset = {
    get(0).getOrElse {
      val actualOffsets = SpecificPulsarOffset(
        metadataReader.actualOffsets(startingOffsets, poolTimeoutMs, reportDataLoss))
      add(0, actualOffsets)
      logInfo(s"Initial Offsets: $actualOffsets")
      actualOffsets
    }
  }
}

private[pulsar] class PulsarOffsetRange private (
    private var topic_ : String,
    private var fromOffset_ : MessageId,
    private var untilOffset_ : MessageId,
    private var preferredLoc_ : Option[String])
    extends Externalizable {

  def this() = this(null, null, null, None) // For deserialization only

  def topic: String = topic_

  def fromOffset: MessageId = fromOffset_

  def untilOffset: MessageId = untilOffset_

  def preferredLoc: Option[String] = preferredLoc_

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(topic_)

    val fromBytes = fromOffset_.toByteArray
    if (fromOffset_.isInstanceOf[UserProvidedMessageId]) {
      out.writeBoolean(true)
    } else {
      out.writeBoolean(false)
    }
    out.writeInt(fromBytes.length)
    out.write(fromBytes)

    val untilBytes = untilOffset_.toByteArray
    out.writeInt(untilBytes.length)
    out.write(untilBytes)

    out.writeBoolean(preferredLoc_.isDefined)
    if (preferredLoc_.isDefined) {
      out.writeUTF(preferredLoc_.get)
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    topic_ = in.readUTF()

    val isUserProvided = in.readBoolean()
    val fromBytes = new Array[Byte](in.readInt())
    in.readFully(fromBytes)
    fromOffset_ = if (isUserProvided) {
      UserProvidedMessageId(MessageId.fromByteArray(fromBytes))
    } else {
      MessageId.fromByteArray(fromBytes)
    }

    val toBytes = new Array[Byte](in.readInt())
    in.readFully(toBytes)
    untilOffset_ = MessageId.fromByteArray(toBytes)

    val hasLoc = in.readBoolean()
    if (hasLoc) {
      preferredLoc_ = Some(in.readUTF())
    } else {
      preferredLoc_ = None
    }
  }
}

private[pulsar] object PulsarOffsetRange {
  def apply(
      topic: String,
      fromOffset: MessageId,
      untilOffset: MessageId,
      preferredLoc: Option[String]): PulsarOffsetRange = {
    new PulsarOffsetRange(topic, fromOffset, untilOffset, preferredLoc)
  }
}
