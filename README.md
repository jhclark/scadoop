Scadoop
=======
By Jonathan Clark

One day, the obscene verbosity of writing MapReduce applications using the Hadoop Java API caused me to fly into a fit of blinding rage. Ergo, Scadoop.

Introduction
============

We draw heavily from David Hall's smr (link) and the Python dumbo (link) module.

What Scadoop does differently:

* We use implicit string to text conversion
* You only need to declare the writable types of your mapper reducer once. ever.
* You can specify closures for your mapper and reducer. They will be serialized over the wire without the need for mangling into configuration strings.
* Uses the Java API directly making it faster than dumbo while requiring far less code than writing directly in Java.
* For I/O bound jobs, the Java API (and therefore Scadoop) should be faster than even C++ with Hadoop Pipes.

Let's have a look classic wordcount example written in Scadoop. If you have a lot of time on your hands, you can also have a look at the original Java Hadoop API version [http://wiki.apache.org/hadoop/WordCount].

```scala
@serializable object WordCountApp extends ScadoopApp {
  val inDir = args(0)
  val outDir = args(1)

  val prefix = "#" // magically gets passed to all mappers/reducers via closure serialization
  val one = new IntWritable(1) // don't recreate this every time

  def mapper(records: Iterator[(LongWritable,Text)], c: MapContext): Iterator[(Text,IntWritable)]
    = for( (xxx, line) <- records; tok <- line.split(' ').toIterator) yield (new Text(prefix+tok), one)
  def reducer(records: Iterator[(Text,Iterator[IntWritable])], c: ReduceContext): Iterator[(Text,IntWritable)]
    = for( (key, values) <- records) yield (key, new IntWritable(values.map(_.get).sum))

  val pipeline = Pipeline.add("Word Count Job", mapper, reducer, combiner=Some(reducer _))
  exit(pipeline.runWithExitCode(inDir, outDir, ""))
}
```

Building
========

```bash
set HADOOP_HOME=/the/path
set SCALA_HOME=/the/path
ant
```

Running the Example
===================

```bash
./run_example.sh
```
