#!/usr/bin/env bash
set -u
set -eo pipefail
#export JAVA_HOME=/usr/lib/jvm/java-6-sun
#export HADOOP_HOME=/home/jhclark/workspace/hadoop-0.20.1
#export HADOOP_JAR=/home/jhclark/workspace/hadoop-0.20.1/hadoop-0.20.1-core.jar
#export HADOOP_CLASSPATH=/home/jhclark/scala-2.9.1.final/lib/scala-library.jar
export CLASSPATH=/home/jhclark/scala-2.9.1.final/lib/scala-library.jar

export ALL_HADOOP=""
for jar in $HADOOP_HOME/lib/*.jar; do
    export ALL_HADOOP="$ALL_HADOOP:$jar"
done
for jar in $HADOOP_HOME/*.jar; do
    export ALL_HADOOP="$ALL_HADOOP:$jar"
done

rm -rf example/out
#hadoop BloatedWordCount example/in example/out
#hadoop jar scadoop.jar example/in example/out
#java -cp $ALL_HADOOP:$CLASSPATH:scadoop.jar scadoop.BloatedWordCount example/in example/out
java -cp $ALL_HADOOP:$CLASSPATH:scadoop.jar scadoop.examples.WordCountApp example/in example/out
