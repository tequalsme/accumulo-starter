accumulo-starter
================

A sample application that illustrates using Apache Accumulo to ingest and query the Enron Email Dataset.


Setup
-----

download enron dataset from https://www.cs.cmu.edu/~enron/

untar

place into hdfs:
$ hadoop fs -put enron_mail_20110402 /enron


Ingest
------

mvn package
cd ingest/target/
tar xf accumulo-starter-ingest-*-dist.tar.gz 
cd accumulo-starter-ingest-*/
./bin/ingest.sh <path>

Choose a small directory for testing, such as "/enron/maildir/slinger-r"


Query
-----

