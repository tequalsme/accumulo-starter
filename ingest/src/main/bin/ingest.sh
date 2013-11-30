#!/bin/sh

if [ $# -ne 1 ]; then
    echo "Usage: $0 inputdir"
    echo "  - ingests data from the given HDFS directory"
    exit 1
fi

BIN_DIR=`dirname $0`
for f in $BIN_DIR/../lib/*.jar; do
    CLASSPATH=${CLASSPATH}:$f  
done
export HADOOP_CLASSPATH=$CLASSPATH

# Transform the classpath into a comma-separated list also
LIBJARS=`echo $CLASSPATH | sed 's/^://' | sed 's/:/,/g'`

JAR=$BIN_DIR/../lib/${project.build.finalName}.jar
CONF=$BIN_DIR/../conf/ingest.xml

hadoop jar $JAR com.timreardon.accumulo.starter.ingest.IngestJob \
  -libjars $LIBJARS -conf $CONF ${1}
