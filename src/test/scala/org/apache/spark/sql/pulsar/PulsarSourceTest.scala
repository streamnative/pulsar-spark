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
package org.apache.spark.sql.pulsar

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.schema.SchemaType

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.util.stackTraceToString
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamTest, StreamingQueryException, StreamingQueryListener}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{Dataset, QueryTest}
import org.apache.spark.util.{SystemClock, Utils}

class PulsarSourceTest extends StreamTest with SharedSparkSession with PulsarTest {

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    // TODO: disable task retry when pulsar reader seek failure is fixed.
    sparkContext.conf.set("spark.task.maxFailures", "5")
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  protected def makeSureGetOffsetCalled = AssertOnQuery { q =>
    // Because PulsarSource's initialPartitionOffsets is set lazily, we need to make sure
    // its "getOffset" is called before pushing any data. Otherwise, because of the race condition,
    // we don't know which data should be fetched when `startingOffsets` is latest.
    q match {
      case m: MicroBatchExecution => m.processAllAvailable()
    }
    true
  }

  /**
   * Add data to Pulsar.
   *
   * `topicAction` can be used to run actions for each topic before inserting data.
   */
  case class AddPulsarData(topics: Set[String], data: Int*)(
    implicit ensureDataInMultiplePartition: Boolean = false,
    concurrent: Boolean = false,
    message: String = "",
    topicAction: (String, Option[MessageId]) => Unit = (_, _) => {})
    extends AddData {

    override def addData(query: Option[StreamExecution]): (SparkDataStream, Offset) = {
      query match {
        // Make sure no Spark job is running when deleting a topic
        case Some(m: MicroBatchExecution) => m.processAllAvailable()
        case _ =>
      }

      val existingTopics = getAllTopicsSize().toMap
      val newTopics = topics.diff(existingTopics.keySet)
      for (newTopic <- newTopics) {
        topicAction(newTopic, None)
      }
      for (existingTopicPartitions <- existingTopics) {
        topicAction(existingTopicPartitions._1, Some(existingTopicPartitions._2))
      }

      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active pulsar source")

      val sources = query.get.logicalPlan.collect {
          case StreamingExecutionRelation(source: PulsarSource, _, _) => source
          case StreamingExecutionRelation(source: PulsarMicroBatchReader, _, _) => source
        }.distinct

      if (sources.isEmpty) {
        throw new Exception(
          "Could not find Pulsar source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the Pulsar source in the StreamExecution logical plan as there" +
            "are multiple Pulsar sources:\n\t" + sources.mkString("\n\t"))
      }
      val pulsarSource = sources.head
      val topic = topics.toSeq(Random.nextInt(topics.size))

      sendMessages(topic, data.map { _.toString }.toArray)
      val sizes = getLatestOffsets(topics).toSeq
      val offset = SpecificPulsarOffset(sizes: _*)
      logInfo(s"Added data, expected offset $offset")
      (pulsarSource, offset)
    }

    override def toString: String =
      s"AddPulsarData(topics = $topics, data = $data, message = $message)"
  }

  /**
   * Add data to Pulsar.
   *
   * `topicAction` can be used to run actions for each topic before inserting data.
   */
  case class AddPulsarTypedData[T: ClassTag](topics: Set[String], tpe: SchemaType, data: Seq[T])(
    implicit ensureDataInMultiplePartition: Boolean = false,
    concurrent: Boolean = false,
    message: String = "",
    topicAction: (String, Option[MessageId]) => Unit = (_, _) => {})
    extends AddData {

    override def addData(query: Option[StreamExecution]): (SparkDataStream, Offset) = {
      query match {
        // Make sure no Spark job is running when deleting a topic
        case Some(m: MicroBatchExecution) => m.processAllAvailable()
        case _ =>
      }

      val existingTopics = getAllTopicsSize().toMap
      val newTopics = topics.diff(existingTopics.keySet)
      for (newTopic <- newTopics) {
        topicAction(newTopic, None)
      }
      for (existingTopicPartitions <- existingTopics) {
        topicAction(existingTopicPartitions._1, Some(existingTopicPartitions._2))
      }

      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active pulsar source")

      val sources = query.get.logicalPlan.collect {
          case StreamingExecutionRelation(source: PulsarSource, _, _) => source
          case StreamingExecutionRelation(source: PulsarMicroBatchReader, _, _) => source
        }.distinct

      if (sources.isEmpty) {
        throw new Exception(
          "Could not find Pulsar source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the Pulsar source in the StreamExecution logical plan as there" +
            "are multiple Pulsar sources:\n\t" + sources.mkString("\n\t"))
      }
      val pulsarSource = sources.head
      val topic = topics.toSeq(Random.nextInt(topics.size))

      val offsets: Seq[MessageId] = sendTypedMessages[T](topic, tpe, data, None)

      val midLast = PulsarSourceUtils.mid2Impl(offsets.last)

      val offset = SpecificPulsarOffset((topic, midLast))
      logInfo(s"Added data, expected offset $offset")
      (pulsarSource, offset)
    }

    override def toString: String =
      s"AddPulsarData(topics = $topics, data = $data, message = $message)"
  }

  private val topicId = new AtomicInteger(0)

  protected def newTopic(): String = TopicName.get(s"topic-${topicId.getAndIncrement()}").toString

  override def testStream(_stream: Dataset[_], outputMode: OutputMode, extraOptions: Map[String,String])(
    actions: StreamAction*): Unit = synchronized {
    import org.apache.spark.sql.streaming.util.StreamManualClock

    // `synchronized` is added to prevent the user from calling multiple `testStream`s concurrently
    // because this method assumes there is only one active query in its `StreamingQueryListener`
    // and it may not work correctly when multiple `testStream`s run concurrently.

    val stream = _stream.toDF()
    val sparkSession = stream.sparkSession // use the session in DF, not the default session
    var pos = 0
    var currentStream: StreamExecution = null
    var lastStream: StreamExecution = null
    val awaiting = new mutable.HashMap[Int, OffsetV2]() // source index -> offset to wait for
    val sink = new MemorySink()
    val resetConfValues = mutable.Map[String, Option[String]]()
    val defaultCheckpointLocation =
      Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath
    var manualClockExpectedTime = -1L

    @volatile
    var streamThreadDeathCause: Throwable = null
    // Set UncaughtExceptionHandler in `onQueryStarted` so that we can ensure catching fatal errors
    // during query initialization.
    val listener = new StreamingQueryListener {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {
        // Note: this assumes there is only one query active in the `testStream` method.
        Thread.currentThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler {
          override def uncaughtException(t: Thread, e: Throwable): Unit = {
            streamThreadDeathCause = e
          }
        })
      }

      override def onQueryProgress(event: QueryProgressEvent): Unit = {}
      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
    }
    sparkSession.streams.addListener(listener)

    // If the test doesn't manually start the stream, we do it automatically at the beginning.
    val startedManually =
      actions.takeWhile(!_.isInstanceOf[StreamMustBeRunning]).exists(_.isInstanceOf[StartStream])
    val startedTest = if (startedManually) actions else StartStream() +: actions

    def testActions =
      actions.zipWithIndex
        .map {
          case (a, i) =>
            if ((pos == i && startedManually) || (pos == (i + 1) && !startedManually)) {
              "=> " + a.toString
            } else {
              "   " + a.toString
            }
        }
        .mkString("\n")

    def currentOffsets =
      if (currentStream != null) currentStream.committedOffsets.toString else "not started"

    def threadState =
      if (currentStream != null && currentStream.queryExecutionThread.isAlive) "alive" else "dead"

    def threadStackTrace =
      if (currentStream != null && currentStream.queryExecutionThread.isAlive) {
        s"Thread stack trace: ${currentStream.queryExecutionThread.getStackTrace.mkString("\n")}"
      } else {
        ""
      }

    def testState = {
      val sinkDebugString = sink match {
        case s: MemorySink => s.toDebugString
      }
      s"""
         |== Progress ==
         |$testActions
         |
         |== Stream ==
         |Output Mode: $outputMode
         |Stream state: $currentOffsets
         |Thread state: $threadState
         |$threadStackTrace
         |${if (streamThreadDeathCause != null) stackTraceToString(streamThreadDeathCause) else ""}
         |
         |== Sink ==
         |$sinkDebugString
         |
         |
         |== Plan ==
         |${if (currentStream != null) currentStream.lastExecution else ""}
         """.stripMargin
    }

    def verify(condition: => Boolean, message: String): Unit = {
      if (!condition) {
        failTest(message)
      }
    }

    def eventually[T](message: String)(func: => T): T = {
      try {
        Eventually.eventually(Timeout(streamingTimeout)) {
          func
        }
      } catch {
        case NonFatal(e) =>
          failTest(message, e)
      }
    }

    def failTest(message: String, cause: Throwable = null) = {

      // Recursively pretty print a exception with truncated stacktrace and internal cause
      def exceptionToString(e: Throwable, prefix: String = ""): String = {
        val base = s"$prefix${e.getMessage}" +
          e.getStackTrace.take(10).mkString(s"\n$prefix", s"\n$prefix\t", "\n")
        if (e.getCause != null) {
          base + s"\n$prefix\tCaused by: " + exceptionToString(e.getCause, s"$prefix\t")
        } else {
          base
        }
      }
      val c = Option(cause).map(exceptionToString(_))
      val m = if (message != null && message.size > 0) Some(message) else None
      fail(s"""
              |${(m ++ c).mkString(": ")}
              |$testState
         """.stripMargin)
    }

    var lastFetchedMemorySinkLastBatchId: Long = -1

    def fetchStreamAnswer(
        currentStream: StreamExecution,
        lastOnly: Boolean = false,
        sinceLastFetchOnly: Boolean = false) = {
      verify(
        !(lastOnly && sinceLastFetchOnly),
        "both lastOnly and sinceLastFetchOnly cannot be true")
      verify(currentStream != null, "stream not running")

      // Block until all data added has been processed for all the source
      awaiting.foreach {
        case (sourceIndex, offset) =>
          failAfter(streamingTimeout) {
            // currentStream.awaitOffset(sourceIndex, offset, streamingTimeout.toMillis)
            // Make sure all processing including no-data-batches have been executed
            if (!currentStream.triggerClock.isInstanceOf[StreamManualClock]) {
              currentStream.processAllAvailable()
            }
          }
      }

      val lastExecution = currentStream.lastExecution
      if (currentStream.isInstanceOf[MicroBatchExecution] && lastExecution != null) {
        // Verify if stateful operators have correct metadata and distribution
        // This can often catch hard to debug errors when developing stateful operators
        lastExecution.executedPlan.collect { case s: StatefulOperator => s }.foreach { s =>
          assert(s.stateInfo.map(_.numPartitions).contains(lastExecution.numStateStores))
          s.requiredChildDistribution.foreach { d =>
            withClue(s"$s specifies incorrect # partitions in requiredChildDistribution $d") {
              assert(d.requiredNumPartitions.isDefined)
              assert(d.requiredNumPartitions.get >= 1)
              if (d != AllTuples) {
                assert(d.requiredNumPartitions.get == s.stateInfo.get.numPartitions)
              }
            }
          }
        }
      }

      val rows = try {
        if (sinceLastFetchOnly) {
          if (sink.latestBatchId.getOrElse(-1L) < lastFetchedMemorySinkLastBatchId) {
            failTest("MemorySink was probably cleared since last fetch. Use CheckAnswer instead.")
          }
          sink.dataSinceBatch(lastFetchedMemorySinkLastBatchId)
        } else {
          if (lastOnly) sink.latestBatchData else sink.allData
        }
      } catch {
        case e: Exception =>
          failTest("Exception while getting data from sink", e)
      }
      lastFetchedMemorySinkLastBatchId = sink.latestBatchId.getOrElse(-1L)
      rows
    }

    def executeAction(action: StreamAction): Unit = {
      logInfo(s"Processing test stream action: $action")
      action match {
        case StartStream(trigger, triggerClock, additionalConfs, checkpointLocation) =>
          verify(currentStream == null || !currentStream.isActive, "stream already running")
          verify(
            triggerClock.isInstanceOf[SystemClock]
              || triggerClock.isInstanceOf[StreamManualClock],
            "Use either SystemClock or StreamManualClock to start the stream"
          )
          if (triggerClock.isInstanceOf[StreamManualClock]) {
            manualClockExpectedTime = triggerClock.asInstanceOf[StreamManualClock].getTimeMillis()
          }
          val metadataRoot = Option(checkpointLocation).getOrElse(defaultCheckpointLocation)

          additionalConfs.foreach(pair => {
            val value =
              if (sparkSession.conf.contains(pair._1)) {
                Some(sparkSession.conf.get(pair._1))
              } else None
            resetConfValues(pair._1) = value
            sparkSession.conf.set(pair._1, pair._2)
          })

          lastStream = currentStream
          currentStream = sparkSession.streams
            .startQuery(
              None,
              Some(metadataRoot),
              stream,
              Map(),
              sink,
              outputMode,
              trigger = trigger,
              triggerClock = triggerClock)
            .asInstanceOf[StreamingQueryWrapper]
            .streamingQuery
          // Wait until the initialization finishes, because some tests need to use `logicalPlan`
          // after starting the query.
          try {
            currentStream.awaitInitialization(streamingTimeout.toMillis)
          } catch {
            case _: StreamingQueryException =>
            // Ignore the exception. `StopStream` or `ExpectFailure` will catch it as well.
          }

        case AdvanceManualClock(timeToAdd) =>
          verify(
            currentStream != null,
            "can not advance manual clock when a stream is not running")
          verify(
            currentStream.triggerClock.isInstanceOf[StreamManualClock],
            s"can not advance clock of type ${currentStream.triggerClock.getClass}")
          val clock = currentStream.triggerClock.asInstanceOf[StreamManualClock]
          assert(manualClockExpectedTime >= 0)

          // Make sure we don't advance ManualClock too early. See SPARK-16002.
          eventually("StreamManualClock has not yet entered the waiting state") {
            assert(clock.isStreamWaitingAt(manualClockExpectedTime))
          }

          clock.advance(timeToAdd)
          manualClockExpectedTime += timeToAdd
          verify(
            clock.getTimeMillis() === manualClockExpectedTime,
            s"Unexpected clock time after updating: " +
              s"expecting $manualClockExpectedTime, current ${clock.getTimeMillis()}"
          )

        case StopStream =>
          verify(currentStream != null, "can not stop a stream that is not running")
          try failAfter(streamingTimeout) {
            currentStream.stop()
            verify(!currentStream.queryExecutionThread.isAlive, s"microbatch thread not stopped")
            verify(!currentStream.isActive, "query.isActive() is false even after stopping")
            verify(
              currentStream.exception.isEmpty,
              s"query.exception() is not empty after clean stop: " +
                currentStream.exception.map(_.toString()).getOrElse(""))
          } catch {
            case e: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
              failTest(
                "Timed out while stopping and waiting for microbatchthread to terminate.",
                e)
            case thr: Throwable =>
              def causedByInterruptedException(e: Throwable): Boolean = {
                if (e.isInstanceOf[InterruptedException]) {
                  true
                } else {
                  if (e.getCause == null) {
                    return false
                  }
                  causedByInterruptedException(e.getCause)
                }
              }
              if (currentStream.exception.isDefined) {
                if (!causedByInterruptedException(currentStream.exception.get)) {
                  failTest("Error while stopping stream", thr)
                }
              }
          } finally {
            lastStream = currentStream
            currentStream = null
          }

        case ef: ExpectFailure[_] =>
          verify(currentStream != null, "can not expect failure when stream is not running")
          try failAfter(streamingTimeout) {
            val thrownException = intercept[StreamingQueryException] {
              currentStream.awaitTermination()
            }
            eventually("microbatch thread not stopped after termination with failure") {
              assert(!currentStream.queryExecutionThread.isAlive)
            }
            verify(
              currentStream.exception === Some(thrownException),
              s"incorrect exception returned by query.exception()")

            val exception = currentStream.exception.get
            verify(
              exception.cause.getClass === ef.causeClass,
              "incorrect cause in exception returned by query.exception()\n" +
                s"\tExpected: ${ef.causeClass}\n\tReturned: ${exception.cause.getClass}"
            )
            if (ef.isFatalError) {
              // This is a fatal error, `streamThreadDeathCause` should be set to this error in
              // UncaughtExceptionHandler.
              verify(
                streamThreadDeathCause != null &&
                  streamThreadDeathCause.getClass === ef.causeClass,
                "UncaughtExceptionHandler didn't receive the correct error\n" +
                  s"\tExpected: ${ef.causeClass}\n\tReturned: $streamThreadDeathCause"
              )
              streamThreadDeathCause = null
            }
            ef.assertFailure(exception.getCause)
          } catch {
            case _: InterruptedException =>
            case e: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
              failTest("Timed out while waiting for failure", e)
            case t: Throwable =>
              failTest("Error while checking stream failure", t)
          } finally {
            lastStream = currentStream
            currentStream = null
          }

        case a: AssertOnQuery =>
          verify(
            currentStream != null || lastStream != null,
            "cannot assert when no stream has been started")
          val streamToAssert = Option(currentStream).getOrElse(lastStream)
          try {
            verify(a.condition(streamToAssert), s"Assert on query failed: ${a.message}")
          } catch {
            case NonFatal(e) =>
              failTest(s"Assert on query failed: ${a.message}", e)
          }

        case a: Assert =>
          val streamToAssert = Option(currentStream).getOrElse(lastStream)
          verify({ a.run(); true }, s"Assert failed: ${a.message}")

        case a: AddData =>
          try {

            // If the query is running with manual clock, then wait for the stream execution
            // thread to start waiting for the clock to increment. This is needed so that we
            // are adding data when there is no trigger that is active. This would ensure that
            // the data gets deterministically added to the next batch triggered after the manual
            // clock is incremented in following AdvanceManualClock. This avoid race conditions
            // between the test thread and the stream execution thread in tests using manual
            // clock.
            if (currentStream != null &&
              currentStream.triggerClock.isInstanceOf[StreamManualClock]) {
              val clock = currentStream.triggerClock.asInstanceOf[StreamManualClock]
              eventually("Error while synchronizing with manual clock before adding data") {
                if (currentStream.isActive) {
                  assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
                }
              }
              if (!currentStream.isActive) {
                failTest("Query terminated while synchronizing with manual clock")
              }
            }
            // Add data
            val queryToUse = Option(currentStream).orElse(Option(lastStream))
            val (source, offset) = a.addData(queryToUse)

            def findSourceIndex(plan: LogicalPlan): Option[Int] = {
              plan
                .collect {
                  case r: StreamingExecutionRelation => r.source
                  case r: StreamingDataSourceV2Relation => r.stream
                }
                .zipWithIndex
                .find(_._1 == source)
                .map(_._2)
            }

            // Try to find the index of the source to which data was added. Either get the index
            // from the current active query or the original input logical plan.
            val sourceIndex =
            queryToUse
              .flatMap { query =>
                findSourceIndex(query.logicalPlan)
              }
              .orElse {
                findSourceIndex(stream.logicalPlan)
              }
              .orElse {
                queryToUse.flatMap { q =>
                  findSourceIndex(q.lastExecution.logical)
                }
              }
              .getOrElse {
                throw new IllegalArgumentException(
                  "Could not find index of the source to which data was added")
              }

            // Store the expected offset of added data to wait for it later
            awaiting.put(sourceIndex, offset)
          } catch {
            case NonFatal(e) =>
              failTest("Error adding data", e)
          }

        case e: ExternalAction =>
          e.runAction()

        case CheckAnswerRows(expectedAnswer, lastOnly, isSorted) =>
          val sparkAnswer = fetchStreamAnswer(currentStream, lastOnly)
          QueryTest.sameRows(expectedAnswer, sparkAnswer, isSorted).foreach { error =>
            failTest(error)
          }

        case CheckAnswerRowsContains(expectedAnswer, lastOnly) =>
          val sparkAnswer = currentStream match {
            case null => fetchStreamAnswer(lastStream, lastOnly)
            case s => fetchStreamAnswer(s, lastOnly)
          }
          QueryTest.includesRows(expectedAnswer, sparkAnswer).foreach { error =>
            failTest(error)
          }

        case CheckAnswerRowsByFunc(globalCheckFunction, lastOnly) =>
          val sparkAnswer = currentStream match {
            case null => fetchStreamAnswer(lastStream, lastOnly)
            case s => fetchStreamAnswer(s, lastOnly)
          }
          try {
            globalCheckFunction(sparkAnswer)
          } catch {
            case e: Throwable => failTest(e.toString)
          }

        case CheckNewAnswerRows(expectedAnswer) =>
          val sparkAnswer = fetchStreamAnswer(currentStream, sinceLastFetchOnly = true)
          QueryTest.sameRows(expectedAnswer, sparkAnswer).foreach { error =>
            failTest(error)
          }
      }
    }

    try {
      startedTest.foreach {
        case StreamProgressLockedActions(actns, _) =>
          // Perform actions while holding the stream from progressing
          assert(
            currentStream != null,
            s"Cannot perform stream-progress-locked actions $actns when query is not active")
          assert(
            currentStream.isInstanceOf[MicroBatchExecution],
            s"Cannot perform stream-progress-locked actions on non-microbatch queries")
          currentStream.asInstanceOf[MicroBatchExecution].withProgressLocked {
            actns.foreach(executeAction)
          }
          pos += 1

        case action: StreamAction =>
          executeAction(action)
          pos += 1
      }
      if (streamThreadDeathCause != null) {
        failTest("Stream Thread Died", streamThreadDeathCause)
      }
    } catch {
      case _: InterruptedException if streamThreadDeathCause != null =>
        failTest("Stream Thread Died", streamThreadDeathCause)
      case e: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
        failTest("Timed out waiting for stream", e)
    } finally {
      if (currentStream != null && currentStream.queryExecutionThread.isAlive) {
        currentStream.stop()
      }

      // Rollback prev configuration values
      resetConfValues.foreach {
        case (key, Some(value)) => sparkSession.conf.set(key, value)
        case (key, None) => sparkSession.conf.unset(key)
      }
      sparkSession.streams.removeListener(listener)
    }
  }

}