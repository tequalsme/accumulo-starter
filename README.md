# accumulo-starter

A sample application that illustrates using [Apache Accumulo](http://accumulo.apache.org/) to ingest and query the [Enron Email Dataset](https://www.cs.cmu.edu/~enron/).

## Notes

This sample app was tested with the following:

* Accumulo 1.4.2
* Hadoop 0.20.2
* Zookeeper 3.3.5
* Java 1.7.0_15
* Maven 3.0.4

## Prerequisites

1. Accumulo, Hadoop, and ZooKeeper must be installed and running.
2. To compile the query module, [mango-core](https://github.com/calrissian/mango) (currently 1.0.3-SNAPSHOT) must be available in your local Maven repository:

```bash
$ git clone git@github.com:calrissian/mango.git
$ cd mango/mango-core && mvn clean install
```

## Download Enron Data and Load into HDFS

[Download enron dataset](https://www.cs.cmu.edu/~enron/) and untar.

Place into HDFS, e.g.:

```bash
$ hadoop fs -put enron_mail_20110402 /enron
```

## Compile Starter Project

Clone and compile:

```bash
$ git clone git@github.com:tequalsme/accumulo-starter.git
$ cd accumulo-starter
$ mvn clean package
```

## Ingest the Data

Untar the compiled ingest assembly:

```bash
$ cd ingest/target/
$ tar xf accumulo-starter-ingest-*-dist.tar.gz 
$ cd accumulo-starter-ingest-*
```

Edit the `conf/ingest.xml` file specifying your Accumulo connection parameters.

Execute the `bin/ingest.sh` script, specifying the HDFS path to be loaded into Accumulo. (When starting out, choose a small directory for testing purposes, for example "/enron/maildir/slinger-r")

```bash
$ ./bin/ingest.sh <path_to_ingest>
```

## Query the Data

Create a profile in your local Maven settings.xml specifying your Accumulo connection parameters:

```xml
  <profiles>
    <profile>
      <id>test</id>
      <properties>
        <accumulo.instance>...</accumulo.instance>
        <accumulo.zookeepers>...</accumulo.zookeepers>
        <accumulo.username>...</accumulo.username>
        <accumulo.password>...</accumulo.password>
      </properties>
    </profile>
  </profiles>
```

Then launch the query webapp using the maven-jetty-plugin and your newly created Maven profile:

```bash
$ cd query-webapp/
$ mvn jetty:run -Ptest
```

This will start an Jetty webapp running on port 8080. You can enter Ctrl-C at any time to stop the web server.

Open the UI at http://localhost:8080/ and issue a query based on the data you have ingested.

Alternatively, you can issue queries via the REST url: http://localhost:8080/accumulo-starter/query/query

For example (using curl):

```bash
$ curl "http://localhost:8080/accumulo-starter/query/query?term=enron&limit=100"
```
