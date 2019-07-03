# pulsar-spark

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Version](https://img.shields.io/github/release/streamnative/pulsar-spark.svg)](https://github.com/streamnative/pulsar-spark/releases)

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

## Spark Pulsar Integration

See docs at [here](docs/spark-integration.md).
