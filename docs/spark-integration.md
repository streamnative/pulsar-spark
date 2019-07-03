# Structured Streaming + Pulsar Integration Guide

Structured Streaming integration for Spark 2.4.0+ with Pulsar 2.3.1+ to read data from and write data to Pulsar.

## Build Pulsar-Spark Connector Jar By Yourself

In order to use Pulsar in Spark, you need to build the fat jar of pulsar-spark connector.
Clone the pulsar-spark repo, changing the `scala.version` and `scala.binary.version` 
(_scala version should be consistent with the scala version of Spark you use_) in `pom.xml` 
and build the fat jar using following command.

```
$ mvn -am -pl connectors/spark-all/ clean install -DskipTests
```

Once it finishes, there is a `-all` fat jar generated under your local maven repo as well as `connectors/spark-all/target`.

## Linking
For Scala/Java applications using SBT/Maven project definitions, link your application with the following artifact:

    groupId = io.streamnative.connectors
    artifactId = pulsar-spark-connector-all_{{SCALA_BINARY_VERSION}}
    version = {{PULSAR_SPARK_VERSION}}

For Python applications, you need to add this above library and its dependencies when deploying your
application. See the [Deploying](#deploying) subsection below.

For experimenting on `spark-shell`, you need to add this above library and its dependencies too when invoking `spark-shell`. Also, see the [Deploying](#deploying) subsection below.

## Reading Data from Pulsar

### Creating a Pulsar Source for Streaming Queries

```scala
// Subscribe to 1 topic
val df = spark
  .readStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("topic", "topic1")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to multiple topics
val df = spark
  .readStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("topics", "topic1,topic2")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to a pattern
val df = spark
  .readStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("topicsPattern", "topic.*")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

```
_Note: to use other language bindings for Spark Structured Streaming, 
refer to [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) 
for more examples. We'll only show the scala code here as an example._

### Creating a Pulsar Source for Batch Queries 
If you have a use case that is better suited to batch processing,
you can create a Dataset/DataFrame for a defined range of offsets.

```scala

// Subscribe to 1 topic defaults to the earliest and latest offsets
val df = spark
  .read
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("topic", "topic1")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to multiple topics, specifying explicit Pulsar offsets
import org.apache.spark.sql.pulsar.JsonUtils._
val startingOffsets = topicOffsets(Map("topic1" -> messageId1, "topic2" -> messageId2))
val endingOffsets = topicOffsets(...)
val df = spark
  .read
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("topics", "topic1,topic2")
  .option("startingOffsets", startingOffsets)
  .option("endingOffsets", endingOffsets)
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to a pattern, at the earliest and latest offsets
val df = spark
  .read
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("topicsPattern", "topic.*")
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]
```

Each row in the source has the following schema:
<table class="table">
<tr><th>Column</th><th>Type</th></tr>
<tr>
  <td>key</td>
  <td>Binary</td>
</tr>
<tr>
  <td>value</td>
  <td>Binary</td>
</tr>
<tr>
  <td>topic</td>
  <td>String</td>
</tr>
<tr>
  <td>messageId</td>
  <td>Binary</td>
</tr>
<tr>
  <td>publishTime</td>
  <td>Timestamp</td>
</tr>
<tr>
  <td>eventTime</td>
  <td>TimeStamp</td>
</tr>
</table>

The following options must be set for the Pulsar source
for both batch and streaming queries.

<table class="table">
<tr><th>Option</th><th>value</th><th>meaning</th></tr>
<tr>
  <td>topic</td>
  <td>A topic name string</td>
  <td>Specific Topic to consume.
  Only one of "topic", "topics" or "topicsPattern"
  options can be specified for Pulsar source.</td>
</tr>
<tr>
  <td>topics</td>
  <td>A comma-separated list of topics</td>
  <td>The topic list to consume.
  Only one of "topic", "topics" or "topicsPattern"
  options can be specified for Pulsar source.</td>
</tr>
<tr>
  <td>topicsPattern</td>
  <td>Java regex string</td>
  <td>The pattern used to subscribe to topic(s).
  Only one of "topic", "topics" or "topicsPattern"
  options can be specified for Pulsar source.</td>
</tr>
<tr>
  <td>service.url</td>
  <td>The service url of your Pulsar cluster</td>
  <td>The Pulsar "serviceUrl" configuration.</td>
</tr>
<tr>
  <td>admin.url</td>
  <td>The service http url of your Pulsar cluster</td>
  <td>The Pulsar "serviceHttpUrl" configuration.</td>
</tr>
</table>

The following configurations are optional:

<table class="table">
<tr><th>Option</th><th>value</th><th>default</th><th>query type</th><th>meaning</th></tr>
<tr>
  <td>startingOffsets</td>
  <td>"earliest", "latest" (streaming only), or json string
  """ {"topic-1":[8,11,16,101,24,1,32,1],"topic-5":[8,15,16,105,24,5,32,5]} """
  </td>
  <td>"latest" for streaming, "earliest" for batch</td>
  <td>streaming and batch</td>
  <td>The start point when a query is started, either "earliest" which is from the earliest offsets,
  "latest" which is just from the latest offsets, or a json string specifying a starting offset for
  each Topic. For specific message as off set, 
  you could use `org.apache.spark.sql.pulsar.JsonUtils.topicOffsets(Map[String, MessageId])` to generate
  json string. 
  Note: For batch queries, latest (either implicitly or by using MessageId.latest ([8,-1,-1,-1,-1,-1,-1,-1,-1,127,16,-1,-1,-1,-1,-1,-1,-1,-1,127]) in json) is not allowed.
  For streaming queries, this only applies when a new query is started, and that resuming will
  always pick up from where the query left off. Newly discovered partitions during a query will start at
  earliest.</td>
</tr>
<tr>
  <td>endingOffsets</td>
  <td>latest or json string
  {"topic-1":[8,12,16,102,24,2,32,2],"topic-5":[8,16,16,106,24,6,32,6]}
  </td>
  <td>latest</td>
  <td>batch query</td>
  <td>The end point when a batch query is ended, either "latest" which is just referred to the
  latest, or a json string specifying an ending offset for each topic.  
  In json, MessageId.earliest ([8,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,16,-1,-1,-1,-1,-1,-1,-1,-1,-1,1]) as an offset is not allowed.</td>
</tr>
<tr>
  <td>failOnDataLoss</td>
  <td>true or false</td>
  <td>true</td>
  <td>streaming query</td>
  <td>Whether to fail the query when it's possible that data is lost (e.g., topics are deleted, or
  message are deleted because of retention policy). This may be a false alarm. You can disable it when it doesn't work
  as you expected. Batch queries will always fail if it fails to read any data from the provided
  offsets due to lost data.</td>
</tr>
</table>

## Writing Data to Pulsar

Here, we describe the support for writing Streaming Queries and Batch Queries to Apache Pulsar. Take note that 
Apache Pulsar only supports at least once write semantics. Consequently, when writing---either Streaming Queries
or Batch Queries---to Pulsar, some records may be duplicated; this can happen, for example, if Pulsar needs
to retry a message that was not acknowledged by a Broker, even though that Broker received and wrote the message record.
Structured Streaming cannot prevent such duplicates from occurring due to these Pulsar write semantics. However, 
if writing the query is successful, then you can assume that the query output was written at least once. A possible
solution to remove duplicates when reading the written data could be to introduce a primary (unique) key 
that can be used to perform de-duplication when reading.

The Dataframe being written to Pulsar should have the following columns in schema:
<table class="table">
<tr><th>Column</th><th>Type</th></tr>
<tr>
  <td>key (optional)</td>
  <td>string or binary</td>
</tr>
<tr>
  <td>value (required)</td>
  <td>string or binary</td>
</tr>
<tr>
  <td>topic (*optional)</td>
  <td>string</td>
</tr>
</table>
\* The topic column is required if the "topic" options is not specified.<br>

The value column is the only required option. If a key column is not specified then 
a ```null``` valued key column will be automatically added (see Pulsar semantics on 
how ```null``` valued key values are handled). If a topic column exists then its value
is used as the topic when writing the given row to Pulsar, unless the "topic" configuration
option is set i.e., the "topic" configuration option overrides the topic column.

The following options must be set for the Pulsar sink
for both batch and streaming queries.

<table class="table">
<tr><th>Option</th><th>value</th><th>meaning</th></tr>
<tr>
  <td>service.url</td>
  <td>The service url of your Pulsar cluster</td>
  <td>The Pulsar "serviceUrl" configuration.</td>
</tr>
</table>

The following configurations are optional:

<table class="table">
<tr><th>Option</th><th>value</th><th>default</th><th>query type</th><th>meaning</th></tr>
<tr>
  <td>topic</td>
  <td>string</td>
  <td>none</td>
  <td>streaming and batch</td>
  <td>Sets the topic that all rows will be written to in Pulsar. This option overrides any
  topic column that may exist in the data.</td>
</tr>
</table>

### Creating a Pulsar Sink for Streaming Queries

```scala

// Write key-value data from a DataFrame to a specific Pulsar topic specified in an option
val ds = df
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("topic", "topic1")
  .start()

// Write key-value data from a DataFrame to Pulsar using a topic specified in the data
val ds = df
  .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .start()
```


### Writing the output of Batch Queries to Pulsar

```scala

// Write key-value data from a DataFrame to a specific Pulsar topic specified in an option
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .write
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("topic", "topic1")
  .save()

// Write key-value data from a DataFrame to Pulsar using a topic specified in the data
df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
  .write
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .save()
```


## Pulsar Specific Configurations

Pulsar's client/producer/consumer configurations can be set via `DataStreamReader.option` 
with `pulsar.client.`/`pulsar.producer.`/`pulsar.consumer.` prefix, e.g, 
`stream.option("pulsar.consumer.ackTimeoutMillis", "10000")`. For possible Pulsar parameters, see 
[Pulsar client libraries](https://pulsar.apache.org/docs/en/client-libraries/).


## Deploying

As with any Spark applications, `spark-submit` is used to launch your application. `pulsar-spark-connector-all_{{SCALA_BINARY_VERSION}}`
and its dependencies can be directly added to `spark-submit` using `--packages`, such as,

    ./bin/spark-submit --packages io.streamnative.connectors:pulsar-spark-connector-all_{{SCALA_BINARY_VERSION}}:{{PULSAR_SPARK_VERSION}} ...

For experimenting on `spark-shell`, you can also use `--packages` to add `pulsar-spark-connector-all_{{SCALA_BINARY_VERSION}}` and its dependencies directly,

    ./bin/spark-shell --packages io.streamnative.connectors:pulsar-spark-connector-all_{{SCALA_BINARY_VERSION}}:{{PULSAR_SPARK_VERSION}} ...
    
A little more information: `--packages` option will search the local maven repo, then maven central and any additional remote
repositories given by `--repositories`. The format for the coordinates should be groupId:artifactId:version.

See [Application Submission Guide](https://spark.apache.org/docs/latest/submitting-applications.html) for more details about submitting
applications with external dependencies.
