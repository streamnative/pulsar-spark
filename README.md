# pulsar-spark

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Version](https://img.shields.io/github/release/streamnative/pulsar-spark.svg)](https://github.com/streamnative/pulsar-spark/releases)

Unified data processing with [Apache Pulsar](https://pulsar.apache.org) and [Apache Spark](https://spark.apache.org).

## Prerequisites

- Java 8 or later
- Spark 2.4.0 or later
- Pulsar 2.4.0 or later

## Preparations
### Build Spark Pulsar Connector

1. Checkout the source code.

```bash
git clone https://github.com/streamnative/pulsar-spark.git
cd pulsar-spark
```

2. Install Docker.

> Pulsar-spark connector is using [Testcontainers](https://www.testcontainers.org/) for
> integration tests. In order to run the integration tests, make sure you
> have installed [Docker](https://docs.docker.com/docker-for-mac/install/).

3. Set a Scala version.
> Change `scala.version` and `scala.binary.version` in `pom.xml`.
> #### Note
> Scala version should be consistent with the Scala version of Spark you use.

4. Build the project.

```bash
mvn clean install -DskipTests
```

5. Run the tests.

```bash
mvn clean install
```
Once the installation is finished, there is a fat jar generated under both local maven repo and `target` directory.

### Link
* For Scala/Java applications using SBT/Maven project definitions, link your application with the following artifact:

```
    groupId = io.streamnative.connectors
    artifactId = pulsar-spark-connector_{{SCALA_BINARY_VERSION}}
    version = {{PULSAR_SPARK_VERSION}}
```

* For Python applications, you need to add this above library and its dependencies when deploying your
application. For more information, see [Deploy](#deploy) subsection below.

For experimenting on `spark-shell`, you need to add this above library and its dependencies too when invoking `spark-shell`. Also, see the [Deploying](#deploying) subsection below.

### Deploy

As with any Spark applications, `spark-submit` is used to launch your application. `pulsar-spark-connector_{{SCALA_BINARY_VERSION}}`
and its dependencies can be directly added to `spark-submit` using `--packages`, such as,

    ./bin/spark-submit --packages io.streamnative.connectors:pulsar-spark-connector_{{SCALA_BINARY_VERSION}}:{{PULSAR_SPARK_VERSION}} ...

For experimenting on `spark-shell`, you can also use `--packages` to add `pulsar-spark-connector_{{SCALA_BINARY_VERSION}}` and its dependencies directly,

    ./bin/spark-shell --packages io.streamnative.connectors:pulsar-spark-connector_{{SCALA_BINARY_VERSION}}:{{PULSAR_SPARK_VERSION}} ...
    
When locating an artifact or library, `--packages` option checks the following repositories in order:
1. Local maven repository
2. Maven central repository
3. Other repositories specified by `--repositories`

The format for the coordinates should be `groupId:artifactId:version`.

For more information about **submitting applications with external dependencies**, see [Application Submission Guide](https://spark.apache.org/docs/latest/submitting-applications.html). 

## Usage

### Read data from Pulsar

#### Create a Pulsar source for streaming queries
The following examples are in Scala.
```scala
// Subscribe to 1 topic
val df = spark
  .readStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("topic", "topic1")
  .load()
df.selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to multiple topics
val df = spark
  .readStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("topics", "topic1,topic2")
  .load()
df.selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to a pattern
val df = spark
  .readStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("topicsPattern", "topic.*")
  .load()
df.selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

```
> #### Tip
> For more information on how to use other language bindings for Spark Structured Streaming, 
> see [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html).

#### Create a Pulsar source for batch queries 
If you have a use case that is better suited to batch processing,
you can create a Dataset/DataFrame for a defined range of offsets.

The following examples are in Scala.
```scala

// Subscribe to 1 topic defaults to the earliest and latest offsets
val df = spark
  .read
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("topic", "topic1")
  .load()
df.selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
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
df.selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
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
df.selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]
```

The following options must be set for the Pulsar source
for both batch and streaming queries.

<table class="table">
<tr><th>Option</th><th>Value</th><th>Description</th></tr>
<tr>
  <td>`topic`</td>
  <td>A topic name string</td>
  <td>The topic to be consumed.
  Only one of `topic`, `topics` or `topicsPattern`
  options can be specified for Pulsar source.</td>
</tr>
<tr>
  <td>`topics`</td>
  <td>A comma-separated list of topics</td>
  <td>The topic list to be consumed.
  Only one of `topic`, `topics` or `topicsPattern`
  options can be specified for Pulsar source.</td>
</tr>
<tr>
  <td>`topicsPattern`</td>
  <td>A Java regex string</td>
  <td>The pattern used to subscribe to topic(s).
  Only one of `topic`, `topics` or `topicsPattern`
  options can be specified for Pulsar source.</td>
</tr>
<tr>
  <td>`service.url`</td>
  <td>A service URL of your Pulsar cluster</td>
  <td>The Pulsar `serviceUrl` configuration.</td>
</tr>
<tr>
  <td>`admin.url`</td>
  <td>A service HTTP URL of your Pulsar cluster</td>
  <td>The Pulsar `serviceHttpUrl` configuration.</td>
</tr>
</table>

The following configurations are optional.

<table class="table">

<tr><th>Option</th><th>Value</th><th>Default</th><th>Query Type</th><th>Description</th></tr>

<tr>

  <td>`startingOffsets`</td>

  <td>The following are valid values:<br>

  * "earliest"(streaming and batch queries)<br>
  
  * "latest" (streaming query)<br>
  
  * A JSON string<br>
  
    **Example**<br>

    """ {"topic-1":[8,11,16,101,24,1,32,1],"topic-5":[8,15,16,105,24,5,32,5]} """
  </td>

  <td>

   * "earliest"（batch query)<br>
  
   *  "latest"（streaming query)</td>
  
  <td>Streaming and batch queries</td>

  <td>

  `startingOffsets` option controls where a consumer reads data from.

  * "earliest": lacks a valid offset, the consumer reads all the data in the partition, starting from the very beginning.<br>

*  "latest": lacks a valid offset, the consumer reads from the newest records written after the consumer starts running.<br>
  
* A JSON string: specifies a starting offset for each Topic. <br>
You can use `org.apache.spark.sql.pulsar.JsonUtils.topicOffsets(Map[String, MessageId])` to convert a message offset to a JSON string. <br>

**Note**: <br>

* For batch query, "latest" is not allowed, either implicitly specified or use `MessageId.latest ([8,-1,-1,-1,-1,-1,-1,-1,-1,127,16,-1,-1,-1,-1,-1,-1,-1,-1,127])` in JSON.<br>

* For streaming query, "latest" only applies when a new query is started, and the resuming will
  always pick up from where the query left off. Newly discovered partitions during a query will start at
  "earliest".</td>

</tr>

<tr>

  <td>`endingOffsets`</td>

  <td>The following are valid values:<br>

  * "latest" (batch query)<br>
  
  * A JSON string<br>
  
   **Example**<br> 
   
   {"topic-1":[8,12,16,102,24,2,32,2],"topic-5":[8,16,16,106,24,6,32,6]}

  </td>

  <td>"latest"</td>

  <td>Batch query</td>

  <td>
  
  `endingOffsets` option controls where a consumer stops reading data.
  
  * "latest": the consumer stops reading data at the latest record.
 
 * A JSON string: specifies an ending offset for each topic.<br>

    **Note**: <br> 
    
    `MessageId.earliest ([8,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,16,-1,-1,-1,-1,-1,-1,-1,-1,-1,1])` is not allowed.</td>

</tr>

<tr>

  <td>`failOnDataLoss`</td>

  <td>The following are valid values:<br>

  * true
  
  * false
  
  </td>

  <td>true</td>

  <td>Streaming query</td>

  <td>
  
  `failOnDataLoss` option controls whether to fail a query when data is lost (for example, topics are deleted, or
  messages are deleted because of retention policy).<br>

  This may cause a false alarm. You can set it to `false` when it doesn't work as you expected. <br>
  
  A batch query always fails if it fails to read any data from the provided offsets due to data loss.</td>

</tr>

</table>

#### Schema of Pulsar source
* For topics without schema or with primitive schema in Pulsar, messages' payload
is loaded to a `value` column with the corresponding type with Pulsar schema.

* For topics with Avro or JSON schema, their field names and field types are kept in the result rows.

Besides, each row in the source has the following metadata fields as well.
<table class="table">
<tr><th>Column</th><th>Type</th></tr>
<tr>
  <td>`__key`</td>
  <td>Binary</td>
</tr>
<tr>
  <td>`__topic`</td>
  <td>String</td>
</tr>
<tr>
  <td>`__messageId`</td>
  <td>Binary</td>
</tr>
<tr>
  <td>`__publishTime`</td>
  <td>Timestamp</td>
</tr>
<tr>
  <td>`__eventTime`</td>
  <td>Timestamp</td>
</tr>
</table>

** Example**

The topic of AVRO schema _s_ in Pulsar is as below:
```scala
  case class Foo(i: Int, f: Float, bar: Bar)
  case class Bar(b: Boolean, s: String)
  val s = Schema.AVRO(Foo.getClass)
```
has the following schema as a DataFrame/DataSet in Spark:
```
root
 |-- i: integer (nullable = false)
 |-- f: float (nullable = false)
 |-- bar: struct (nullable = true)
 |    |-- b: boolean (nullable = false)
 |    |-- s: string (nullable = true)
 |-- __key: binary (nullable = true)
 |-- __topic: string (nullable = true)
 |-- __messageId: binary (nullable = true)
 |-- __publishTime: timestamp (nullable = true)
 |-- __eventTime: timestamp (nullable = true)
 ```
 
 For Pulsar topic with `Schema.DOUBLE`, it's schema as a DataFrame is:
 ```
 root
 |-- value: double (nullable = false)
 |-- __key: binary (nullable = true)
 |-- __topic: string (nullable = true)
 |-- __messageId: binary (nullable = true)
 |-- __publishTime: timestamp (nullable = true)
 |-- __eventTime: timestamp (nullable = true)
 ```

### Write data to Pulsar

The DataFrame being written to Pulsar can have arbitrary schema, since each record in DataFrame are transformed as one message send to Pulsar, it's fields are divided into two groups: fields named `__key` and `__eventTime` are encoded as Pulsar message's metadata; other fields are grouped and encoded using AVRO and put in `value()`:
```scala
producer.newMessage().key(__key).value(avro_encoded_fields).eventTime(__eventTime)
```

#### Create a Pulsar sink for streaming queries
The following examples are in Scala.
```scala

// Write key-value data from a DataFrame to a specific Pulsar topic specified in an option
val ds = df
  .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("topic", "topic1")
  .start()

// Write key-value data from a DataFrame to Pulsar using a topic specified in the data
val ds = df
  .selectExpr("__topic", "CAST(__key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .start()
```

#### Write the output of batch queries to Pulsar
The following examples are in Scala.
```scala

// Write key-value data from a DataFrame to a specific Pulsar topic specified in an option
df.selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
  .write
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("topic", "topic1")
  .save()

// Write key-value data from a DataFrame to Pulsar using a topic specified in the data
df.selectExpr("__topic", "CAST(__key AS STRING)", "CAST(value AS STRING)")
  .write
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .save()
```

#### Limitations

Currently, we provide at-least-once semantic. Consequently, when writing either streaming queries
or batch Queries to Pulsar, some records may be duplicated.
If writing the query is successful, then you can assume that the query output was written at least once. A possible
solution to remove duplicates when reading the written data could be to introduce a primary (unique) key 
that can be used to perform de-duplication when reading.


### Pulsar specific configurations

Pulsar's client/producer/consumer configurations can be set via `DataStreamReader.option` 
with `pulsar.client.`/`pulsar.producer.`/`pulsar.consumer.` prefix, e.g, 
`stream.option("pulsar.consumer.ackTimeoutMillis", "10000")`. For possible Pulsar parameters, check docs at 
[Pulsar client libraries](https://pulsar.apache.org/docs/en/client-libraries/).
