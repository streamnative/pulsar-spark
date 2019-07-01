# pulsar-spark

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Version](https://img.shields.io/github/release/streamnative/psegment.svg)](https://github.com/streamnative/psegment/releases)

Unified data processing with [Apache Pulsar](https://pulsar.apache.org) and [Apache Spark](https://spark.apache.org).

## Prerequisites

- Java 8+

## Building Spark Pulsar Connectors

Checkout the source code:

```bash
git clone https://github.com/streamnative/pulsar-spark.git
cd pulsar-spark
```

> pulsar-spark is using [testcontainers](https://www.testcontainers.org/) for
> integration tests. In order to run the integration tests, make sure you
> have installed [docker](https://docs.docker.com/docker-for-mac/install/)

Build the project.

```bash
mvn clean install -DskipTests
```

Run the tests.

```bash
mvn clean install
```

## Setting up your IDE

Similar as [Apache Pulsar](http://pulsar.apache.org), *pulsar-spark* is using
[lombok](https://projectlombok.org/). You have to ensure your IDE setup
required plugins. Intellij is recommended.

### Intellij

To configure annotation processing in IntelliJ:

1. Open Annotation Processors Settings dialog box by going to
   `Settings -> Build, Execution, Deployment -> Compiler -> Annotation Processors`.

2. Select the following buttons:
   1. “Enable annotation processing”
   2. “Obtain processors from project classpath”
   3. “Store generated sources relative to: Module content root”

3. Set the generated source directories to be equal to the Maven directories:
   1. Set “Production sources directory:” to “target/generated-sources/annotations”.
   2. Set “Test sources directory:” to “target/generated-test-sources/test-annotations”.

4. Click “OK”.

### Eclipse

Follow the instructions [here](https://howtodoinjava.com/automation/lombok-eclipse-installation-examples/)
to configure your Eclipse setup.

## Spark Pulsar Integration

See docs at [here](docs/spark-integration.md).
