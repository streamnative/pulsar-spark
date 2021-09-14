# pulsar-spark

[![Build Status](https://jenkins.snio.xyz/buildStatus/icon?job=StreamNative%2Fpulsar-spark%2Fmaster)](https://jenkins.snio.xyz/job/StreamNative/job/pulsar-spark/job/master/)
[![Version](https://img.shields.io/github/release/streamnative/pulsar-spark/all.svg)](https://github.com/streamnative/pulsar-spark/releases)
[![Bintray](https://img.shields.io/badge/dynamic/json.svg?label=latest&query=name&style=flat-square&url=https%3A%2F%2Fapi.bintray.com%2Fpackages%2Fstreamnative%2Fmaven%2Fio.streamnative.pulsar-spark%2Fversions%2F_latest)](https://dl.bintray.com/streamnative/maven/io/streamnative/connectors/pulsar-spark-connector_2.11/)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fstreamnative%2Fpulsar-spark.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fstreamnative%2Fpulsar-spark?ref=badge_shield)

Unified data processing with [Apache Pulsar](https://pulsar.apache.org) and [Apache Spark](https://spark.apache.org).

## Prerequisites

- Java 8 or later
- Spark 2.4.0 or later
- Pulsar 2.4.0 or later

## Preparations

### Link

#### Client library  
For Scala/Java applications using SBT/Maven project definitions, link your application with the following artifact:

```
    groupId = io.streamnative.connectors
    artifactId = pulsar-spark-connector_{{SCALA_BINARY_VERSION}}
    version = {{PULSAR_SPARK_VERSION}}
```
Currently, the artifact is available in [Bintray Maven repository of StreamNative]( https://dl.bintray.com/streamnative/maven).
For Maven project, you can add the repository to your `pom.xml` as follows:
```xml
  <repositories>
    <repository>
      <id>central</id>
      <layout>default</layout>
      <url>https://repo1.maven.org/maven2</url>
    </repository>
    <repository>
      <id>bintray-streamnative-maven</id>
      <name>bintray</name>
      <url>https://dl.bintray.com/streamnative/maven</url>
    </repository>
  </repositories>
```

### Deploy

#### Client library  
As with any Spark applications, `spark-submit` is used to launch your application.     
`pulsar-spark-connector_{{SCALA_BINARY_VERSION}}`
and its dependencies can be directly added to `spark-submit` using `--packages`.  

Example

```
$ ./bin/spark-submit 
  --packages io.streamnative.connectors:pulsar-spark-connector_{{SCALA_BINARY_VERSION}}:{{PULSAR_SPARK_VERSION}}
  --repositories https://dl.bintray.com/streamnative/maven
  ...
```

#### CLI  
For experimenting on `spark-shell` (or `pyspark` for Python), you can also use `--packages` to add `pulsar-spark-connector_{{SCALA_BINARY_VERSION}}` and its dependencies directly.

Example

```
$ ./bin/spark-shell 
  --packages io.streamnative.connectors:pulsar-spark-connector_{{SCALA_BINARY_VERSION}}:{{PULSAR_SPARK_VERSION}}
  --repositories https://dl.bintray.com/streamnative/maven
  ...
```

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

  `startingOffsets` option controls where a reader reads data from.

  * "earliest": lacks a valid offset, the reader reads all the data in the partition, starting from the very beginning.<br>

*  "latest": lacks a valid offset, the reader reads from the newest records written after the reader starts running.<br>

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

  `endingOffsets` option controls where a reader stops reading data.

  * "latest": the reader stops reading data at the latest record.

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
<tr>
<td>`maxRetries`</td>
<td>Maximum number of retries to attempt before failing.</td>
<td>0</td>
<td>Streaming and batch queries</td>
<td>Currently this *only* works for `pulsar-admin` API calls.
The retry time is calculated using the following formula:

`nextWaitTime = previousWaitTime * retryMultiplier * 
random value from [1.0 - randomizationFactor, 1.0 + randomizationFactor]`

For example, the following sequence of wait times might be generated 
by this logic (with parameters `maxRetries=10`, `initialInterval=500`,
`randomizationFactor=0.5`, `retryMultiplier=1.5`: 
500 ms, 841 ms, 1587 ms, 2849 ms, 5765 ms.
</tr>

<tr>
<td>`retryMultiplier`</td>
<td>Real number such as `1.25`.</td>
<td>1.5</td>
<td>Streaming and batch queries</td>
<td>Parameter used in conjunction with the `maxRetries` parameter.</td>
</tr>

<tr>
<td>`initialInterval`</td>
<td>Initial wait time in ms.</td>
<td>500</td>
<td>Streaming and batch queries</td>
<td>Parameter used in conjunction with the `maxRetries` parameter.</td>
</tr>

<tr>
<td>`randomizationFactor`</td>
<td>Real number between 0.0 and 1.0 such as `0.75`.</td>
<td>0.5</td>
<td>Streaming and batch queries</td>
<td>Parameter used in conjunction with the `maxRetries` parameter.</td>
</tr>

</table>

#### Authentication
Should the Pulsar cluster require authentication, credentials can be set in the following way.

The following examples are in Scala.
```scala
// Secure connection with authentication, using the same credentials on the
// Pulsar client and admin interface (if not given explicitly, the client configuration
// is used for admin as well).
val df = spark
  .readStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationToken")
  .option("pulsar.client.authParams","token:<valid client JWT token>")
  .option("topicsPattern", "sensitiveTopic")
  .load()
df.selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]
// Secure connection with authentication, using different credentials for
// Pulsar client and admin interfaces.
val df = spark
  .readStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .option("pulsar.admin.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationToken")
  .option("pulsar.admin.authParams","token:<valid admin JWT token>")
  .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationToken")
  .option("pulsar.client.authParams","token:<valid client JWT token>")
  .option("topicsPattern", "sensitiveTopic")
  .load()
df.selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Secure connection with client TLS enabled.
// Note that the certificate file has to be present at the specified
// path on every machine of the cluster!
val df = spark
  .readStream
  .format("pulsar")
  .option("service.url", "pulsar+ssl://localhost:6651")
  .option("admin.url", "http://localhost:8080")
  .option("pulsar.admin.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationToken")
  .option("pulsar.admin.authParams","token:<valid admin JWT token>")
  .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationToken")
  .option("pulsar.client.authParams","token:<valid client JWT token>")
  .option("pulsar.client.tlsTrustCertsFilePath","/path/to/tls/cert/cert.pem")
  .option("pulsar.client.tlsAllowInsecureConnection","false")
  .option("pulsar.client.tlsHostnameVerificationenable","true")
  .option("topicsPattern", "sensitiveTopic")
  .load()
df.selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]
```

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

The DataFrame written to Pulsar can have arbitrary schema, since each record in DataFrame is transformed as one message sent to Pulsar, fields of DataFrame are divided into two groups: `__key` and `__eventTime` fields are encoded as metadata of Pulsar message; other fields are grouped and encoded using AVRO and put in `value()`:
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
  .option("admin.url", "http://localhost:8080")
  .option("topic", "topic1")
  .start()

// Write key-value data from a DataFrame to Pulsar using a topic specified in the data
val ds = df
  .selectExpr("__topic", "CAST(__key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
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
  .option("admin.url", "http://localhost:8080")
  .option("topic", "topic1")
  .save()

// Write key-value data from a DataFrame to Pulsar using a topic specified in the data
df.selectExpr("__topic", "CAST(__key AS STRING)", "CAST(value AS STRING)")
  .write
  .format("pulsar")
  .option("service.url", "pulsar://localhost:6650")
  .option("admin.url", "http://localhost:8080")
  .save()
```

#### Limitations

Currently, we provide at-least-once semantic. Consequently, when writing either streaming queries or batch queries to Pulsar, some records may be duplicated.
A possible solution to remove duplicates when reading the written data could be to introduce a primary (unique) key that can be used to perform de-duplication when reading.


### Pulsar specific configurations

Client/producer/reader configurations of Pulsar can be set via `DataStreamReader.option`
with `pulsar.client.`/`pulsar.producer.`/`pulsar.reader.` prefix, e.g,
`stream.option("pulsar.reader.receiverQueueSize", "1000000")`. 
Since the connector needs to access the Pulsar Admin interface as well, separate 
configuration of the admin client can be set via the same method with the
`pulsar.admin` prefix. For example: `stream.option("pulsar.admin.authParams","token:<token>")`.
This can be useful if a different authentication plugin or
token need to be used. If this is not given explicitly, the client
parameters (with `pulsar.client` prefix) will be used for accessing the admin
interface as well.
For possible Pulsar parameters, check docs at
[Pulsar client libraries](https://pulsar.apache.org/docs/en/client-libraries/).

## Build Spark Pulsar Connector
If you want to build a Spark-Pulsar connector reading data from Pulsar and writing results to Pulsar, follow the steps below.

1. Checkout the source code.

```bash
$ git clone https://github.com/streamnative/pulsar-spark.git
$ cd pulsar-spark
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
$ mvn clean install -DskipTests
```

If you get the following error during compilation, try running Maven with Java 8:  
```
[ERROR] [Error] : Source option 6 is no longer supported. Use 7 or later.
[ERROR] [Error] : Target option 6 is no longer supported. Use 7 or later.
```

5. Run the tests.

```bash
$ mvn clean install
```

Note: by configuring `scalatest-maven-plugin` in the [usual ways](https://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin), individual tests can be executed, if that is needed:

```bash
mvn -Dsuites=org.apache.spark.sql.pulsar.CachedPulsarClientSuite clean install
```

This might be handy if test execution is slower, or you get a `java.io.IOException: Too many open files` exception during full suite run.

Once the installation is finished, there is a fat jar generated under both local maven repo and `target` directory.


## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fstreamnative%2Fpulsar-spark.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fstreamnative%2Fpulsar-spark?ref=badge_large)
