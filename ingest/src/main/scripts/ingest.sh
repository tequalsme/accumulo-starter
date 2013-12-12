#!/bin/sh

if [ $# -ne 1 ]; then
    echo "Usage: $0 inputdir"
    echo "  - ingests data from the given HDFS directory"
    exit 1
fi

if [ ! -d "$ACCUMULO_HOME" ]; then
    echo "ERROR: ACCUMULO_HOME must be set!" && exit 1
fi
if [ ! -d "$HADOOP_HOME" ]; then
    echo "ERROR: HADOOP_HOME must be set!" && exit 1
fi

SCRIPT_DIR=`dirname $0`
CLASSPATH=$SCRIPT_DIR/../conf
for f in $SCRIPT_DIR/../lib/*.jar; do
    CLASSPATH=${CLASSPATH}:$f
done
for f in $ACCUMULO_HOME/lib/*.jar; do
    CLASSPATH=$f:${CLASSPATH}
done
for f in $ACCUMULO_HOME/lib/ext/*.jar; do
    CLASSPATH=$f:${CLASSPATH}
done
export HADOOP_CLASSPATH=$CLASSPATH

# Transform the classpath into a comma-separated list also
LIBJARS=`echo $CLASSPATH | sed 's/^://' | sed 's/:/,/g'`

JAR=$SCRIPT_DIR/../lib/${project.build.finalName}.jar
CONF=$SCRIPT_DIR/../conf/ingest.xml

$HADOOP_HOME/bin/hadoop jar $JAR com.timreardon.accumulo.starter.ingest.IngestJob \
  -libjars $LIBJARS -conf $CONF ${1}
