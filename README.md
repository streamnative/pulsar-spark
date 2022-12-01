# pulsar-spark

[![Version](https://img.shields.io/github/release/streamnative/pulsar-spark/all.svg)](https://github.com/streamnative/pulsar-spark/releases)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fstreamnative%2Fpulsar-spark.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fstreamnative%2Fpulsar-spark?ref=badge_shield)
![contribution](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)

Unified data processing with [Apache Pulsar](https://pulsar.apache.org) and [Apache Spark](https://spark.apache.org).

## Prerequisites

- Java 8 or later
- Spark 3.2.0 or later
- Pulsar 2.10.0 or later

## Preparations

### Link

For Scala/Java applications using SBT/Maven project definitions, link your application with the following artifact:

```
groupId = io.streamnative.connectors
artifactId = pulsar-spark-connector_{{SCALA_BINARY_VERSION}}
version = {{PULSAR_SPARK_VERSION}}
```

### Deploy

#### Client library

As with any Spark applications, `spark-submit` is used to launch your application.     
`pulsar-spark-connector_{{SCALA_BINARY_VERSION}}`
and its dependencies can be directly added to `spark-submit` using `--packages`.  

Example

```shell
$ ./bin/spark-submit 
  --packages io.streamnative.connectors:pulsar-spark-connector_{{SCALA_BINARY_VERSION}}:{{PULSAR_SPARK_VERSION}}
  ...
```

#### CLI

For experimenting on `spark-shell` (or `pyspark` for Python), you can also use `--packages` to add `pulsar-spark-connector_{{SCALA_BINARY_VERSION}}` and its dependencies directly.

Example

```shell
$ ./bin/spark-shell 
  --packages io.streamnative.connectors:pulsar-spark-connector_{{SCALA_BINARY_VERSION}}:{{PULSAR_SPARK_VERSION}}
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

> **Note**
>
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
val endingOffsets = topicOffsets(Map("topic1" -> MessageId.latest, "topic2" -> MessageId.latest))
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

| Option          | Value                                     | Description                                                                                                                                 |
|-----------------|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `topic`         | A topic name string                       | The topic to be consumed. Only one of `topic`, `topics` or `topicsPattern` options can be specified for the Pulsar source.                  |
| `topics`        | A comma-separated list of topics          | The topic list to be consumed. Only one of `topic`, `topics` or `topicsPattern` options can be specified for the Pulsar source.             |
| `topicsPattern` | A Java regex string                       | The pattern used to subscribe to topic(s). Only one of `topic`, `topics` or `topicsPattern` options can be specified for the Pulsar source. |
| `service.url`   | A service URL of your Pulsar cluster      | The Pulsar `serviceUrl` configuration for creating the `PulsarClient` instance.                                                             |
| `admin.url`     | A service HTTP URL of your Pulsar cluster | The Pulsar `serviceHttpUrl` configuration for creating the `PulsarAdmin` instance.                                                          |

The following configurations are optional.

<table class="table">
<tr><th>Option</th><th>Value</th><th>Default</th><th>Applied Query Type</th><th>Description</th></tr>
<tr>
  <td><code>startingOffsets</code></td>
  <td>
    <p>The following are valid values:</p>
    <ul>
      <li><code>earliest</code>: (streaming and batch queries)</li>
      <li><code>latest</code>: (only streaming query)</li>
      <li>
        <p>JSON string</p>
        <p><strong>Example</strong></p>
        <p><code>{&quot;topic-1&quot;:[8,11,16,101,24,1,32,1],&quot;topic-5&quot;:[8,15,16,105,24,5,32,5]}</code></p>
      </li>
    </ul>
  </td>
  <td>
    <ul>
      <li>Batch query: <code>earliest</code></li>
      <li>Streaming query: <code>latest</code></li>
    </ul>
  </td>
  <td>Streaming and batch queries</td>
  <td>
    <p><code>startingOffsets</code> option controls where a reader reads data from.</p>
    <ul>
      <li>earliest: lacks a valid offset, the reader reads all the data in the partition, starting from the very beginning.</li>
      <li>latest: lacks a valid offset, the reader reads from the newest records written after the reader starts running.</li>
      <li>A JSON string: specifies a starting offset for each Topic.</li>
    </ul>
    <p>You can use <code>org.apache.spark.sql.pulsar.JsonUtils.topicOffsets(Map[String, MessageId])</code> to convert a message offset to a JSON string. </p>
    <p><strong>Note</strong></p>
    <ul>
      <li>For batch query, <code>latest</code> is not allowed, either implicitly specified or use <code>MessageId.latest</code> in JSON.</li>
      <li>For streaming query, <code>latest</code> only applies when a new query is started, and the resuming will
always pick up from where the query left off. Newly discovered partitions during a query will start at  &quot;earliest&quot;.</li>
    </ul>
  </td>
</tr>
<tr>
  <td><code>endingOffsets</code></td>
  <td>
    <p>The following are valid values:</p>
    <ul>
      <li><code>latest</code> (only batch query)</li>
      <li>
        <p>JSON string</p>
        <p><strong>Example</strong></p>
        <p><code>{&quot;topic-1&quot;:[8,12,16,102,24,2,32,2],&quot;topic-5&quot;:[8,16,16,106,24,6,32,6]}</code></p>
      </li>
    </ul>
  </td>
  <td><code>latest</code></td>
  <td>Batch query</td>
  <td>
    <p><code>endingOffsets</code> option controls where a reader stops reading data.</p>
    <ul>
      <li><code>latest</code>: the reader stops reading data at the latest record.</li>
      <li>A JSON string: specifies an ending offset for each topic.</li>
    </ul>
    <p><strong>Note</strong></p>
    <p><code>MessageId.earliest</code> is not allowed.</p>
  </td>
</tr>
<tr>
  <td><code>failOnDataLoss</code></td>
  <td>
    <p>The following are valid values:</p>
    <ul>
      <li><code>true</code></li>
      <li><code>false</code></li>
    </ul>
  </td>
  <td><code>true</code></td>
  <td>Streaming query</td>
  <td>
    <p><code>failOnDataLoss</code> option controls whether to fail a query when data is lost (for example, topics are deleted, or messages are deleted because of retention policy).</p>
    <p>This may cause a false alarm. You can set it to <code>false</code> when it doesn&#39;t work as you expected.</p>
    <p>A batch query always fails if it fails to read any data from the provided offsets due to data loss.</p>
  </td>
</tr>
<tr>
  <td><code>allowDifferentTopicSchemas</code></td>
  <td>Boolean value</td>
  <td><code>false</code></td>
  <td>Streaming query</td>
  <td>
    <p>If multiple topics with different schemas are read, using this parameter automatic schema-based topic value deserialization can be turned off. In that way, topics with different schemas can be read in the same pipeline - which is then responsible for deserializing the raw values based on some schema. Since only the raw values are returned when this is <code>true</code>. Pulsar topic schema(s) are not taken into account during operation.</p>
  </td>
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

- For topics without schema or with primitive schema in Pulsar, messages' payload
is loaded to a `value` column with the corresponding type with Pulsar schema.
- For topics with Avro or JSON schema, their field names and field types are kept in the result rows.
- If the `topicsPattern` matches for topics which have different schemas, then setting
`allowDifferentTopicSchemas` to `true` will allow the connector to read this content in a
raw form. In this case it is the responsibility of the pipeline to apply the schema
on this content, which is loaded to the `value` column. 

Besides, each row in the source has the following metadata fields as well.

| Column                | Type                  |
|-----------------------|-----------------------|
| `__key`               | Binary                |
| `__topic`             | String                |
| `__messageId`         | Binary                |
| `__publishTime`       | Timestamp             |
| `__eventTime`         | Timestamp             |
| `__messageProperties` | `Map<String, String>` |

**Example**

The topic of AVRO schema in Pulsar is as below:

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
 |-- __messageProperties: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
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
 |-- __messageProperties: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
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

The client, producer and reader configuration of Pulsar can be set via `DataStreamReader.option`
with `pulsar.client.`, `pulsar.producer.` and `pulsar.reader.` prefix.

E.g, `stream.option("pulsar.reader.receiverQueueSize", "1000000")`. 

Since the connector needs to access the Pulsar Admin interface as well, separate 
configuration of the admin client can be set via the same method with the
`pulsar.admin` prefix.

For example: `stream.option("pulsar.admin.authParams","token:<token>")`.

This can be useful if a different authentication plugin or token need to be used.
If this is not given explicitly, the client parameters (with `pulsar.client` prefix) will be used for accessing the admin
interface as well.

For possible Pulsar parameters, check docs at [Pulsar client libraries](https://pulsar.apache.org/docs/en/client-libraries/).

## Build Spark Pulsar Connector

If you want to build a Spark-Pulsar connector reading data from Pulsar and writing results to Pulsar, follow the steps below.

1. Checkout the source code.
   ```bash
   $ git clone https://github.com/streamnative/pulsar-spark.git
   $ cd pulsar-spark
   ```
2. Install Docker. Pulsar-spark connector is using [Testcontainers](https://www.testcontainers.org/) for
   integration tests. In order to run the integration tests, make sure you
   have installed [Docker](https://docs.docker.com/docker-for-mac/install/).
3. Set a Scala version. Change `scala.version` and `scala.binary.version` in `pom.xml`.
   > **Note**
   >
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
