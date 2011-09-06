package scadoop

import collection._
import System._
import util.Random

import org.apache.hadoop.conf._
import org.apache.hadoop.filecache._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util._

import java.net._

object Implicits {
  implicit def text2str(text: Text) = text.toString
  implicit def text2richstr(text: Text) = new immutable.WrappedString(text.toString)
  implicit def intwritable2int(n: IntWritable) = n.get
}

// must be serializable to transmit options, etc to mappers/reducers
// uses DelayedInit similar to Scala's App trait to let user type code directly in app's constructor
// however, this constructor code is run in run() instead of main() to conform to the Hadoop tool pattern
@serializable trait ScadoopApp extends Configured with Tool with DelayedInit {
  private var _args: Array[String] = _
  protected def args: Array[String] = _args
  private val code = new mutable.ListBuffer[() => Unit]

  override def delayedInit(body: => Unit) = code += (() => body)
  override def run(args: Array[String]): Int = {
    this._args = args
    for(cmd <- code) cmd()
    0 // ignored since client code should call exit
  }
  def main(args: Array[String]): Unit = { // child code should call exit()
    ToolRunner.run(new Configuration, this, args)
  }
}

object Pipeline {

  // enforces key-value types match via scala's type system
  // notice that KIn and VIn are bound at the class level here to enforce that IO types are compatible
  def add[KIn, VIn, KX, VX, KOut:Manifest, VOut:Manifest](
    name: String,
    mapper: (Iterator[(KIn,VIn)],MapContext) => Iterator[(KX,VX)],
    reducer: (Iterator[(KX,Iterator[VX])],ReduceContext) => Iterator[(KOut, VOut)],
    combiner: Option[(Iterator[(KX,Iterator[VX])],ReduceContext) => Iterator[(KX, VX)]]
  ): Pipeline[KOut,VOut]
      = addPriv(List(), name, mapper, reducer, combiner)

  private def addPriv[KIn, VIn, KX, VX, KOut:Manifest, VOut:Manifest](
      prevJobs: List[Job],
      name: String,
      mapper: (Iterator[(KIn,VIn)],MapContext) => Iterator[(KX,VX)],
      reducer: (Iterator[(KX,Iterator[VX])],ReduceContext) => Iterator[(KOut, VOut)],
      combiner: Option[(Iterator[(KX,Iterator[VX])],ReduceContext) => Iterator[(KX, VX)]]
    ): Pipeline[KOut,VOut] = {
    
    val job = new Job(new Configuration, name)
    job.setJarByClass(mapper.getClass)

    val mapperBin = "mapper.bin"
    IOUtil.write(mapper, mapperBin)
    job.getConfiguration.set(SimpleMapper.SERIALIZED_NAME, mapperBin)
    DistributedCache.addCacheFile(new URI(mapperBin), job.getConfiguration)
    
    job.setMapperClass(classOf[SimpleMapper[KIn,VIn,KX,VX]])

// REDUCER SPECIFIC
    job.setReducerClass(classOf[SimpleReducer[KX,VX,KOut,VOut]])
    val reducerBin = "reducer.bin"
    IOUtil.write(reducer, reducerBin)
    job.getConfiguration.set(SimpleReducer.SERIALIZED_NAME, reducerBin)
    DistributedCache.addCacheFile(new URI(reducerBin), job.getConfiguration)

    combiner match {
      case Some(c) => {
        job.setCombinerClass(classOf[SimpleCombiner[KX,VX]])
        val combinerBin = "combiner.bin"
        IOUtil.write(c, combinerBin)
        job.getConfiguration.set(SimpleCombiner.SERIALIZED_NAME, combinerBin)
        DistributedCache.addCacheFile(new URI(combinerBin), job.getConfiguration)
      }
      case None => ;
    }

    job.setOutputKeyClass(manifest[KOut].erasure)
    job.setOutputValueClass(manifest[VOut].erasure)
// MAPPER ONLY
//        job.setOutputKeyClass(manifest[KX].erasure)
//        job.setOutputValueClass(manifest[VX].erasure)
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_,_]])

    new Pipeline[KOut,VOut](prevJobs :+ job)
  }
}

class Pipeline[KIn,VIn] private[scadoop] (jobs: List[Job]) {

  // TODO: Convenience add methods without reducer, etc.
  def add[KX, VX, KOut:Manifest, VOut:Manifest](
    name: String,
    mapper: (Iterator[(KIn,VIn)],MapContext) => Iterator[(KX,VX)],
    reducer: (Iterator[(KX,Iterator[VX])],ReduceContext) => Iterator[(KOut, VOut)],
    combiner: Option[(Iterator[(KX,Iterator[VX])],ReduceContext) => Iterator[(KX, VX)]]
  ): Pipeline[KOut, VOut]
      = Pipeline.addPriv(jobs, name, mapper, reducer, combiner)

  // NOTE: All HDFS paths
  def run(inDir: String, outDir: String, tmpDir: String): Boolean = {

    var prevDir = new Path(inDir)
    for( (job,i) <- jobs.zipWithIndex) {
      // XXX: This mutates an otherwise immutable job
      // TODO: Ensure path doesn't exist
      val curOutDir = i match {
        case _ if(i < jobs.size-1) => new Path(tmpDir, "scadoop-%s".format(Random.alphanumeric.take(10)))
        case _ => new Path(outDir)
      }
      FileInputFormat.addInputPath(job, prevDir)
      FileOutputFormat.setOutputPath(job, curOutDir)

      if(!job.waitForCompletion(true)) {
        err.println("%sERROR: JOB FAILED%s".format(Console.RED, Console.RESET))
        return false
      }
      prevDir = curOutDir
    }
    return true
  }

  def runWithExitCode(inDir: String, outDir: String, tmpDir: String): Int = {
    run(inDir, outDir, tmpDir) match {
      case true => 0
      case false => 1
    }
  }
}

object IOUtil {
  import java.io._

  def write(obj: AnyRef, filename: String) = {
    import java.io._
    val out = new ObjectOutputStream(new FileOutputStream(filename))
    out.writeObject(obj)
    out.close
  }

  // TODO: Determine most generic type parameters here
  def read(inStream: InputStream): AnyRef = {
    import java.io._
    val in = new ObjectInputStream(inStream)
    val obj = in.readObject
    in.close
    obj
  }
}

class Context(hContext: TaskInputOutputContext[_,_,_,_]) {
  import collection.JavaConversions._
  def getCounter(groupName: String, counterName: String): Counter
    = hContext.getCounter(groupName, counterName)
  def getTaskAttemptID: TaskAttemptID = hContext.getTaskAttemptID
  def setStatus(msg: String) = hContext.setStatus(msg)
  def getStatus: String = hContext.getStatus
  def getConfiguration: Configuration = hContext.getConfiguration
  def conf = getConfiguration
  def getJobID: JobID = hContext.getJobID
  def getJobName: String = hContext.getJobName
  def getNumReduceTasks: Int = hContext.getNumReduceTasks
  def getWorkingDirectory: Path = hContext.getWorkingDirectory
  def progress: Unit = hContext.progress

  // XXX: There might be collisions
  private def mapify(paths: Array[Path]): Map[String, Path]
    = if(paths == null) Map.empty
      else paths.map(path => (path.getName, path) ).toMap
  lazy val getDistributedCacheFiles: Map[String, Path] = {
    // hack for bug in local runner
    if(DistributedCache.getLocalCacheFiles(conf) == null) {
      DistributedCache.setLocalFiles(conf, conf.get("mapred.cache.files"));
    }
    mapify(DistributedCache.getLocalCacheFiles(getConfiguration))
  }
  lazy val getDistributedCacheArchives: Map[String, Path] = {
    // hack for bug in local runner
    if(DistributedCache.getLocalCacheArchives(conf) == null) {
      DistributedCache.setLocalArchives(conf, conf.get("mapred.cache.archives"));
    }
    mapify(DistributedCache.getLocalCacheArchives(getConfiguration))
  }
}

class MapContext(hContext: org.apache.hadoop.mapreduce.MapContext[_,_,_,_]) extends Context(hContext) {
  def getInputSplit: InputSplit = hContext.getInputSplit
}

class ReduceContext(hContext: org.apache.hadoop.mapreduce.ReduceContext[_,_,_,_]) extends Context(hContext);


object SimpleMapper {
  val SERIALIZED_NAME = "scadoop.mapper.file"
}

class SimpleMapper[KIn,VIn,KOut,VOut] extends Mapper[KIn,VIn,KOut,VOut] {
  override def run(hContext: Mapper[KIn,VIn,KOut,VOut]#Context): Unit = {
    super.setup(hContext)

    val conf = hContext.getConfiguration
    val serializedMapper = conf.get(SimpleMapper.SERIALIZED_NAME)
    val context = new scadoop.MapContext(hContext)
    val mapperPath = context.getDistributedCacheFiles.get(serializedMapper).get
    //throw new RuntimeException("Serialized mapper function 'mapper.bin' not found in distributed cache.")
    val inStream = mapperPath.getFileSystem(conf).open(mapperPath)
    val delegate = IOUtil.read(inStream).
      asInstanceOf[Function2[Iterator[(KIn,VIn)],MapContext,Iterator[(KOut,VOut)]]]

    def next() = hContext.nextKeyValue() match {
      case true => Some( (hContext.getCurrentKey, hContext.getCurrentValue) )
      case false => None
    }

    // create a *lazy* iterator (all iterators are lazy) over input records
    val it = Iterator.continually(next).takeWhile(_ != None).map(_.get)
    for( (outKey, outValue) <- delegate(it, context)) {
      hContext.write(outKey, outValue)
    }
    super.cleanup(hContext)
  }
}

object SimpleReducer {
  val SERIALIZED_NAME = "scadoop.reducer.file"
}

// TODO: Type Parameters
class SimpleReducer[KIn,VIn,KOut,VOut] extends Reducer[KIn,VIn,KOut,VOut] {

  def serializedName = SimpleReducer.SERIALIZED_NAME

  override def run(hContext: Reducer[KIn,VIn,KOut,VOut]#Context): Unit = {
    super.setup(hContext)
    val conf = hContext.getConfiguration
    val serializedReducer = conf.get(serializedName)
    val context = new scadoop.ReduceContext(hContext)
    val reducerPath = context.getDistributedCacheFiles.get(serializedReducer).get
    //throw new RuntimeException("Serialized reducer function 'reduder.bin' not found in distributed cache.")
    val inStream = reducerPath.getFileSystem(conf).open(reducerPath)
    val delegate = IOUtil.read(inStream).
      asInstanceOf[Function2[Iterator[(KIn,Iterator[VIn])], ReduceContext,Iterator[(KOut,VOut)]]]

    import collection.JavaConversions._
    def next() = hContext.nextKeyValue() match {
      case true => Some( (hContext.getCurrentKey, asScalaIterator(hContext.getValues.iterator)) )
      case false => None
    }

    // create a *lazy* iterator (with view) over input records
    val it = Iterator.continually(next).takeWhile(_ != None).map(_.get)
    for( (outKey, outValue) <- delegate(it, context)) {
      hContext.write(outKey, outValue)
    }
    super.cleanup(hContext)
  }
}

object SimpleCombiner {
  val SERIALIZED_NAME = "scadoop.combiner.file"
}

class SimpleCombiner[K,V] extends SimpleReducer[K,V,K,V] {
  override def serializedName = SimpleCombiner.SERIALIZED_NAME
}
