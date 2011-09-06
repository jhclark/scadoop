package scadoop.examples

import System._
import scadoop._
import scadoop.Implicits._
import org.apache.hadoop.io._

// * We use implicit string to text conversion
// * Your ScadoopApp must be serializable to make over-the-wire magic happen
// * You only need to declare the writable types of your mapper reducer once. ever.
// * Scadoop provides implicit conversions from IntWriable -> Int, Text -> String, etc.
@serializable object WordCountApp extends ScadoopApp {
  val inDir = args(0)
  val outDir = args(1)
  val tmpDir = "" // we only need this for pipelines of length > 1

  val prefix = "#" // magically gets passed to all mappers/reducers via closure serialization
  val one = new IntWritable(1) // don't recreate this every time

  def mapper(records: Iterator[(LongWritable,Text)], c: MapContext): Iterator[(Text,IntWritable)]
    = for( (xxx, line) <- records; tok <- line.split(' ').toIterator) yield (new Text(prefix+tok), one)
  def reducer(records: Iterator[(Text,Iterator[IntWritable])], c: ReduceContext): Iterator[(Text,IntWritable)]
    = for( (key, values) <- records) yield (key, new IntWritable(values.map(_.get).sum))

  val pipeline = Pipeline.add("Word Count Job", mapper, reducer, combiner=Some(reducer _))
  exit(pipeline.runWithExitCode(inDir, outDir, tmpDir))
}
