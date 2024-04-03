/**
 * /!\ Warning /!\
 *
 * This file is generated using zio-spark-codegen, you should not edit
 * this file directly.
 */

package zio.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{InputFormat, JobConf}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.spark.{
  SimpleFutureAction,
  SparkConf,
  SparkContext => UnderlyingSparkContext,
  SparkStatusTracker,
  TaskContext
}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.{RDD => UnderlyingRDD}
import org.apache.spark.resource._
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.util._

import zio._
import zio.spark.rdd.RDD

import scala.reflect.ClassTag

@SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs"))
final case class SparkContext(underlying: UnderlyingSparkContext) { self =>
  // scalafix:off
  implicit private def lift[U](x: UnderlyingRDD[U]): RDD[U]                      = RDD(x)
  implicit private def liftMap[K, V](map: scala.collection.Map[K, V]): Map[K, V] = map.toMap
  // scalafix:on

  /** Applies an action to the underlying SparkContext. */
  def action[U](f: UnderlyingSparkContext => U)(implicit trace: Trace): Task[U] = ZIO.attempt(get(f))

  /** Applies an action to the underlying SparkContext. */
  def get[U](f: UnderlyingSparkContext => U): U = f(underlying)

  // Handmade functions specific to zio-spark
  // template:on
  /**
   * Returns an immutable map of RDDs that have marked themselves as
   * persistent via cache() call.
   *
   * @note
   *   This does not necessarily mean the caching or computation was
   *   successful.
   */
  def getPersistentRDDs: Task[Map[Int, RDD[_]]] =
    ZIO.attempt(
      self.underlying.getPersistentRDDs.view.mapValues(rdd => RDD(rdd)).toMap
    )

  // Generated functions coming from spark

  def appName: String = get(_.appName)

  def applicationAttemptId: Option[String] = get(_.applicationAttemptId)

  /**
   * A unique identifier for the Spark application. Its format depends
   * on the scheduler implementation. (i.e. in case of local spark app
   * something like 'local-1433865536131' in case of YARN something like
   * 'application_1433865536131_34483' in case of MESOS something like
   * 'driver-20170926223339-0001' )
   */
  def applicationId: String = get(_.applicationId)

  /**
   * Default min number of partitions for Hadoop RDDs when not given by
   * user Notice that we use math.min so the "defaultMinPartitions"
   * cannot be higher than 2. The reasons for this are discussed in
   * https://github.com/mesos/spark/pull/718
   */
  def defaultMinPartitions: Int = get(_.defaultMinPartitions)

  /**
   * Default level of parallelism to use when not given by user (e.g.
   * parallelize and makeRDD).
   */
  def defaultParallelism: Int = get(_.defaultParallelism)

  def deployMode: String = get(_.deployMode)

  /** Get an RDD that has no partitions or elements. */
  def emptyRDD[T: ClassTag]: RDD[T] = get(_.emptyRDD[T])

  def files: Seq[String] = get(_.files)

  /**
   * Return a copy of this SparkContext's configuration. The
   * configuration ''cannot'' be changed at runtime.
   */
  def getConf: SparkConf = get(_.getConf)

  /**
   * A default Hadoop Configuration for the Hadoop code (e.g. file
   * systems) that we reuse.
   *
   * @note
   *   As it will be reused in all Hadoop RDDs, it's better not to
   *   modify it unless you plan to set some global configurations for
   *   all Hadoop RDDs.
   */
  def hadoopConfiguration: Configuration = get(_.hadoopConfiguration)

  /**
   * Get an RDD for a Hadoop file with an arbitrary InputFormat
   *
   * @note
   *   Because Hadoop's RecordReader class re-uses the same Writable
   *   object for each record, directly caching the returned RDD or
   *   directly passing it to an aggregation or shuffle operation will
   *   create many references to the same object. If you plan to
   *   directly cache, sort, or aggregate Hadoop writable objects, you
   *   should first copy them using a `map` function.
   * @param path
   *   directory to the input data files, the path can be comma
   *   separated paths as a list of inputs
   * @param inputFormatClass
   *   storage format of the data to be read
   * @param keyClass
   *   `Class` of the key associated with the `inputFormatClass`
   *   parameter
   * @param valueClass
   *   `Class` of the value associated with the `inputFormatClass`
   *   parameter
   * @param minPartitions
   *   suggested minimum number of partitions for the resulting RDD
   * @return
   *   RDD of tuples of key and corresponding value
   */
  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions
  ): RDD[(K, V)] = get(_.hadoopFile[K, V](path, inputFormatClass, keyClass, valueClass, minPartitions))

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out
   * the classes of keys, values and the InputFormat so that users don't
   * need to pass them directly. Instead, callers can just write, for
   * example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minPartitions)
   * }}}
   *
   * @note
   *   Because Hadoop's RecordReader class re-uses the same Writable
   *   object for each record, directly caching the returned RDD or
   *   directly passing it to an aggregation or shuffle operation will
   *   create many references to the same object. If you plan to
   *   directly cache, sort, or aggregate Hadoop writable objects, you
   *   should first copy them using a `map` function.
   * @param path
   *   directory to the input data files, the path can be comma
   *   separated paths as a list of inputs
   * @param minPartitions
   *   suggested minimum number of partitions for the resulting RDD
   * @return
   *   RDD of tuples of key and corresponding value
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String, minPartitions: Int)(implicit
      km: ClassTag[K],
      vm: ClassTag[V],
      fm: ClassTag[F]
  ): RDD[(K, V)] = get(_.hadoopFile[K, V, F](path, minPartitions))

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out
   * the classes of keys, values and the InputFormat so that users don't
   * need to pass them directly. Instead, callers can just write, for
   * example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * }}}
   *
   * @note
   *   Because Hadoop's RecordReader class re-uses the same Writable
   *   object for each record, directly caching the returned RDD or
   *   directly passing it to an aggregation or shuffle operation will
   *   create many references to the same object. If you plan to
   *   directly cache, sort, or aggregate Hadoop writable objects, you
   *   should first copy them using a `map` function.
   * @param path
   *   directory to the input data files, the path can be comma
   *   separated paths as a list of inputs
   * @return
   *   RDD of tuples of key and corresponding value
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
      path: String
  )(implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = get(_.hadoopFile[K, V, F](path))

  def isLocal: Boolean = get(_.isLocal)

  def jars: Seq[String] = get(_.jars)

  /**
   * Distribute a local Scala collection to form an RDD.
   *
   * This method is identical to `parallelize`.
   * @param seq
   *   Scala collection to distribute
   * @param numSlices
   *   number of partitions to divide the collection into
   * @return
   *   RDD representing distributed collection
   */
  def makeRDD[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = get(_.makeRDD[T](seq, numSlices))

  /**
   * Distribute a local Scala collection to form an RDD, with one or
   * more location preferences (hostnames of Spark nodes) for each
   * object. Create a new partition for each collection item.
   * @param seq
   *   list of tuples of data and location preferences (hostnames of
   *   Spark nodes)
   * @return
   *   RDD representing data partitioned according to location
   *   preferences
   */
  def makeRDD[T: ClassTag](seq: Seq[(T, Seq[String])]): RDD[T] = get(_.makeRDD[T](seq))

  def master: String = get(_.master)

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API
   * InputFormat and extra configuration options to pass to the input
   * format.
   *
   * @param conf
   *   Configuration for setting up the dataset. Note: This will be put
   *   into a Broadcast. Therefore if you plan to reuse this conf to
   *   create multiple RDDs, you need to make sure you won't modify the
   *   conf. A safe approach is always creating a new conf for a new
   *   RDD.
   * @param fClass
   *   storage format of the data to be read
   * @param kClass
   *   `Class` of the key associated with the `fClass` parameter
   * @param vClass
   *   `Class` of the value associated with the `fClass` parameter
   *
   * @note
   *   Because Hadoop's RecordReader class re-uses the same Writable
   *   object for each record, directly caching the returned RDD or
   *   directly passing it to an aggregation or shuffle operation will
   *   create many references to the same object. If you plan to
   *   directly cache, sort, or aggregate Hadoop writable objects, you
   *   should first copy them using a `map` function.
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      conf: Configuration = hadoopConfiguration,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V]
  ): RDD[(K, V)] = get(_.newAPIHadoopRDD[K, V, F](conf, fClass, kClass, vClass))

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects,
   * with NullWritable keys and BytesWritable values that contain a
   * serialized partition. This is still an experimental storage format
   * and may not be supported exactly as is in future Spark releases. It
   * will also be pretty slow if you use the default serializer (Java
   * serialization), though the nice thing about it is that there's very
   * little effort required to save arbitrary objects.
   *
   * @param path
   *   directory to the input data files, the path can be comma
   *   separated paths as a list of inputs
   * @param minPartitions
   *   suggested minimum number of partitions for the resulting RDD
   * @return
   *   RDD representing deserialized data from the file(s)
   */
  def objectFile[T: ClassTag](path: String, minPartitions: Int = defaultMinPartitions): RDD[T] =
    get(_.objectFile[T](path, minPartitions))

  /**
   * Creates a new RDD[Long] containing elements from `start` to
   * `end`(exclusive), increased by `step` every element.
   *
   * @note
   *   if we need to cache this RDD, we should make sure each partition
   *   does not exceed limit.
   *
   * @param start
   *   the start value.
   * @param end
   *   the end value.
   * @param step
   *   the incremental step
   * @param numSlices
   *   number of partitions to divide the collection into
   * @return
   *   RDD representing distributed range
   */
  def range(start: Long, end: Long, step: Long = 1, numSlices: Int = defaultParallelism): RDD[Long] =
    get(_.range(start, end, step, numSlices))

  def statusTracker: SparkStatusTracker = get(_.statusTracker)

  def uiWebUrl: Option[String] = get(_.uiWebUrl)

  /** Build the union of a list of RDDs. */
  def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = get(_.union[T](rdds.map(_.underlying)))

  /**
   * Build the union of a list of RDDs passed as variable-length
   * arguments.
   */
  def union[T: ClassTag](first: RDD[T], rest: RDD[T]*): RDD[T] =
    get(_.union[T](first.underlying, rest.map(_.underlying): _*))

  /** The version of Spark on which this application is running. */
  def version: String = get(_.version)

  /**
   * Read a directory of text files from HDFS, a local file system
   * (available on all nodes), or any Hadoop-supported file system URI.
   * Each file is read as a single record and returned in a key-value
   * pair, where the key is the path of each file, the value is the
   * content of each file. The text files must be encoded as UTF-8.
   *
   * <p> For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do `val rdd = sparkContext.wholeTextFile("hdfs://a-hdfs-path")`,
   *
   * <p> then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note
   *   Small files are preferred, large file is also allowable, but may
   *   cause bad performance.
   * @note
   *   On some filesystems, `.../path/&#42;` can be a more efficient way
   *   to read all files in a directory rather than `.../path/` or
   *   `.../path`
   * @note
   *   Partitioning is determined by data locality. This may result in
   *   too few partitions by default.
   *
   * @param path
   *   Directory to the input data files, the path can be comma
   *   separated paths as the list of inputs.
   * @param minPartitions
   *   A suggestion value of the minimal splitting number for input
   *   data.
   * @return
   *   RDD representing tuples of file path and the corresponding file
   *   content
   */
  def wholeTextFiles(path: String, minPartitions: Int = defaultMinPartitions): RDD[(String, String)] =
    get(_.wholeTextFiles(path, minPartitions))

  // ===============

  /**
   * :: Experimental :: Add an archive to be downloaded and unpacked
   * with this Spark job on every node.
   *
   * If an archive is added during execution, it will not be available
   * until the next TaskSet starts.
   *
   * @param path
   *   can be either a local file, a file in HDFS (or other
   *   Hadoop-supported filesystems), or an HTTP, HTTPS or FTP URI. To
   *   access the file in Spark jobs, use
   *   `SparkFiles.get(paths-to-files)` to find its download/unpacked
   *   location. The given path should be one of .zip, .tar, .tar.gz,
   *   .tgz and .jar.
   *
   * @note
   *   A path can be added only once. Subsequent additions of the same
   *   path are ignored.
   *
   * @since 3.1.0
   */
  def addArchive(path: => String)(implicit trace: Trace): Task[Unit] = action(_.addArchive(path))

  /**
   * Add a file to be downloaded with this Spark job on every node.
   *
   * If a file is added during execution, it will not be available until
   * the next TaskSet starts.
   *
   * @param path
   *   can be either a local file, a file in HDFS (or other
   *   Hadoop-supported filesystems), or an HTTP, HTTPS or FTP URI. To
   *   access the file in Spark jobs, use `SparkFiles.get(fileName)` to
   *   find its download location.
   *
   * @note
   *   A path can be added only once. Subsequent additions of the same
   *   path are ignored.
   */
  def addFile(path: => String)(implicit trace: Trace): Task[Unit] = action(_.addFile(path))

  /**
   * Add a file to be downloaded with this Spark job on every node.
   *
   * If a file is added during execution, it will not be available until
   * the next TaskSet starts.
   *
   * @param path
   *   can be either a local file, a file in HDFS (or other
   *   Hadoop-supported filesystems), or an HTTP, HTTPS or FTP URI. To
   *   access the file in Spark jobs, use `SparkFiles.get(fileName)` to
   *   find its download location.
   * @param recursive
   *   if true, a directory can be given in `path`. Currently
   *   directories are only supported for Hadoop-supported filesystems.
   *
   * @note
   *   A path can be added only once. Subsequent additions of the same
   *   path are ignored.
   */
  def addFile(path: => String, recursive: => Boolean)(implicit trace: Trace): Task[Unit] =
    action(_.addFile(path, recursive))

  /**
   * Adds a JAR dependency for all tasks to be executed on this
   * `SparkContext` in the future.
   *
   * If a jar is added during execution, it will not be available until
   * the next TaskSet starts.
   *
   * @param path
   *   can be either a local file, a file in HDFS (or other
   *   Hadoop-supported filesystems), an HTTP, HTTPS or FTP URI, or
   *   local:/path for a file on every worker node.
   *
   * @note
   *   A path can be added only once. Subsequent additions of the same
   *   path are ignored.
   */
  def addJar(path: => String)(implicit trace: Trace): Task[Unit] = action(_.addJar(path))

  /**
   * Add a tag to be assigned to all the jobs started by this thread.
   *
   * @param tag
   *   The tag to be added. Cannot contain ',' (comma) character.
   *
   * @since 3.5.0
   */
  def addJobTag(tag: => String)(implicit trace: Trace): Task[Unit] = action(_.addJobTag(tag))

  def archives(implicit trace: Trace): Task[Seq[String]] = action(_.archives)

  /**
   * Get an RDD for a Hadoop-readable dataset as PortableDataStream for
   * each file (useful for binary data)
   *
   * For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do `val rdd = sparkContext.binaryFiles("hdfs://a-hdfs-path")`,
   *
   * then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note
   *   Small files are preferred; very large files may cause bad
   *   performance.
   * @note
   *   On some filesystems, `.../path/&#42;` can be a more efficient way
   *   to read all files in a directory rather than `.../path/` or
   *   `.../path`
   * @note
   *   Partitioning is determined by data locality. This may result in
   *   too few partitions by default.
   *
   * @param path
   *   Directory to the input data files, the path can be comma
   *   separated paths as the list of inputs.
   * @param minPartitions
   *   A suggestion value of the minimal splitting number for input
   *   data.
   * @return
   *   RDD representing tuples of file path and corresponding file
   *   content
   */
  def binaryFiles(path: => String, minPartitions: => Int = defaultMinPartitions)(implicit
      trace: Trace
  ): Task[RDD[(String, PortableDataStream)]] = action(_.binaryFiles(path, minPartitions))

  /**
   * Load data from a flat binary file, assuming the length of each
   * record is constant.
   *
   * @note
   *   We ensure that the byte array for each record in the resulting
   *   RDD has the provided record length.
   *
   * @param path
   *   Directory to the input data files, the path can be comma
   *   separated paths as the list of inputs.
   * @param recordLength
   *   The length at which to split the records
   * @param conf
   *   Configuration for setting up the dataset.
   *
   * @return
   *   An RDD of data with values, represented as byte arrays
   */
  def binaryRecords(path: => String, recordLength: => Int, conf: => Configuration = hadoopConfiguration)(implicit
      trace: Trace
  ): Task[RDD[Seq[Byte]]] = action(_.binaryRecords(path, recordLength, conf).map(_.toSeq))

  /**
   * Broadcast a read-only variable to the cluster, returning a
   * [[org.apache.spark.broadcast.Broadcast]] object for reading it in
   * distributed functions. The variable will be sent to each executor
   * only once.
   *
   * @param value
   *   value to broadcast to the Spark nodes
   * @return
   *   `Broadcast` object, a read-only variable cached on each machine
   */
  def broadcast[T: ClassTag](value: => T)(implicit trace: Trace): Task[Broadcast[T]] = action(_.broadcast[T](value))

  /** Cancel all jobs that have been scheduled or are running. */
  def cancelAllJobs(implicit trace: Trace): Task[Unit] = action(_.cancelAllJobs())

  /**
   * Cancel a given job if it's scheduled or running.
   *
   * @param jobId
   *   the job ID to cancel
   * @param reason
   *   optional reason for cancellation
   * @note
   *   Throws `InterruptedException` if the cancel message cannot be
   *   sent
   */
  def cancelJob(jobId: => Int, reason: => String)(implicit trace: Trace): Task[Unit] =
    action(_.cancelJob(jobId, reason))

  /**
   * Cancel a given job if it's scheduled or running.
   *
   * @param jobId
   *   the job ID to cancel
   * @note
   *   Throws `InterruptedException` if the cancel message cannot be
   *   sent
   */
  def cancelJob(jobId: => Int)(implicit trace: Trace): Task[Unit] = action(_.cancelJob(jobId))

  /**
   * Cancel active jobs for the specified group. See
   * `org.apache.spark.SparkContext.setJobGroup` for more information.
   */
  def cancelJobGroup(groupId: => String)(implicit trace: Trace): Task[Unit] = action(_.cancelJobGroup(groupId))

  /**
   * Cancel active jobs that have the specified tag. See
   * `org.apache.spark.SparkContext.addJobTag`.
   *
   * @param tag
   *   The tag to be cancelled. Cannot contain ',' (comma) character.
   *
   * @since 3.5.0
   */
  def cancelJobsWithTag(tag: => String)(implicit trace: Trace): Task[Unit] = action(_.cancelJobsWithTag(tag))

  /**
   * Cancel a given stage and all jobs associated with it.
   *
   * @param stageId
   *   the stage ID to cancel
   * @param reason
   *   reason for cancellation
   * @note
   *   Throws `InterruptedException` if the cancel message cannot be
   *   sent
   */
  def cancelStage(stageId: => Int, reason: => String)(implicit trace: Trace): Task[Unit] =
    action(_.cancelStage(stageId, reason))

  /**
   * Cancel a given stage and all jobs associated with it.
   *
   * @param stageId
   *   the stage ID to cancel
   * @note
   *   Throws `InterruptedException` if the cancel message cannot be
   *   sent
   */
  def cancelStage(stageId: => Int)(implicit trace: Trace): Task[Unit] = action(_.cancelStage(stageId))

  /**
   * Clear the thread-local property for overriding the call sites of
   * actions and RDDs.
   */
  def clearCallSite(implicit trace: Trace): Task[Unit] = action(_.clearCallSite())

  /** Clear the current thread's job group ID and its description. */
  def clearJobGroup(implicit trace: Trace): Task[Unit] = action(_.clearJobGroup())

  /**
   * Clear the current thread's job tags.
   *
   * @since 3.5.0
   */
  def clearJobTags(implicit trace: Trace): Task[Unit] = action(_.clearJobTags())

  /**
   * Create and register a `CollectionAccumulator`, which starts with
   * empty list and accumulates inputs by adding them into the list.
   */
  def collectionAccumulator[T](implicit trace: Trace): Task[CollectionAccumulator[T]] =
    action(_.collectionAccumulator[T])

  /**
   * Create and register a `CollectionAccumulator`, which starts with
   * empty list and accumulates inputs by adding them into the list.
   */
  def collectionAccumulator[T](name: => String)(implicit trace: Trace): Task[CollectionAccumulator[T]] =
    action(_.collectionAccumulator[T](name))

  /**
   * Create and register a double accumulator, which starts with 0 and
   * accumulates inputs by `add`.
   */
  def doubleAccumulator(implicit trace: Trace): Task[DoubleAccumulator] = action(_.doubleAccumulator)

  /**
   * Create and register a double accumulator, which starts with 0 and
   * accumulates inputs by `add`.
   */
  def doubleAccumulator(name: => String)(implicit trace: Trace): Task[DoubleAccumulator] =
    action(_.doubleAccumulator(name))

  def getCheckpointDir(implicit trace: Trace): Task[Option[String]] = action(_.getCheckpointDir)

  /**
   * Return a map from the block manager to the max memory available for
   * caching and the remaining memory available for caching.
   */
  def getExecutorMemoryStatus(implicit trace: Trace): Task[Map[String, (Long, Long)]] =
    action(_.getExecutorMemoryStatus)

  /**
   * Get the tags that are currently set to be assigned to all the jobs
   * started by this thread.
   *
   * @since 3.5.0
   */
  def getJobTags(implicit trace: Trace): Task[Set[String]] = action(_.getJobTags())

  /**
   * Get a local property set in this thread, or null if it is missing.
   * See `org.apache.spark.SparkContext.setLocalProperty`.
   */
  def getLocalProperty(key: => String)(implicit trace: Trace): Task[String] = action(_.getLocalProperty(key))

  /** Return current scheduling mode */
  def getSchedulingMode(implicit trace: Trace): Task[SchedulingMode] = action(_.getSchedulingMode)

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf
   * given its InputFormat and other necessary info (e.g. file name for
   * a filesystem-based dataset, table name for HyperTable), using the
   * older MapReduce API (`org.apache.hadoop.mapred`).
   *
   * @param conf
   *   JobConf for setting up the dataset. Note: This will be put into a
   *   Broadcast. Therefore if you plan to reuse this conf to create
   *   multiple RDDs, you need to make sure you won't modify the conf. A
   *   safe approach is always creating a new conf for a new RDD.
   * @param inputFormatClass
   *   storage format of the data to be read
   * @param keyClass
   *   `Class` of the key associated with the `inputFormatClass`
   *   parameter
   * @param valueClass
   *   `Class` of the value associated with the `inputFormatClass`
   *   parameter
   * @param minPartitions
   *   Minimum number of Hadoop Splits to generate.
   * @return
   *   RDD of tuples of key and corresponding value
   *
   * @note
   *   Because Hadoop's RecordReader class re-uses the same Writable
   *   object for each record, directly caching the returned RDD or
   *   directly passing it to an aggregation or shuffle operation will
   *   create many references to the same object. If you plan to
   *   directly cache, sort, or aggregate Hadoop writable objects, you
   *   should first copy them using a `map` function.
   */
  def hadoopRDD[K, V](
      conf: => JobConf,
      inputFormatClass: => Class[_ <: InputFormat[K, V]],
      keyClass: => Class[K],
      valueClass: => Class[V],
      minPartitions: => Int = defaultMinPartitions
  )(implicit trace: Trace): Task[RDD[(K, V)]] =
    action(_.hadoopRDD[K, V](conf, inputFormatClass, keyClass, valueClass, minPartitions))

  /** @return true if context is stopped or in the midst of stopping. */
  def isStopped(implicit trace: Trace): Task[Boolean] = action(_.isStopped)

  /**
   * Kill and reschedule the given task attempt. Task ids can be
   * obtained from the Spark UI or through SparkListener.onTaskStart.
   *
   * @param taskId
   *   the task ID to kill. This id uniquely identifies the task
   *   attempt.
   * @param interruptThread
   *   whether to interrupt the thread running the task.
   * @param reason
   *   the reason for killing the task, which should be a short string.
   *   If a task is killed multiple times with different reasons, only
   *   one reason will be reported.
   *
   * @return
   *   Whether the task was successfully killed.
   */
  def killTaskAttempt(
      taskId: => Long,
      interruptThread: => Boolean = true,
      reason: => String = "killed via SparkContext.killTaskAttempt"
  )(implicit trace: Trace): Task[Boolean] = action(_.killTaskAttempt(taskId, interruptThread, reason))

  /**
   * :: Experimental :: Returns a list of archive paths that are added
   * to resources.
   *
   * @since 3.1.0
   */
  def listArchives(implicit trace: Trace): Task[Seq[String]] = action(_.listArchives())

  /** Returns a list of file paths that are added to resources. */
  def listFiles(implicit trace: Trace): Task[Seq[String]] = action(_.listFiles())

  /** Returns a list of jar files that are added to resources. */
  def listJars(implicit trace: Trace): Task[Seq[String]] = action(_.listJars())

  /**
   * Create and register a long accumulator, which starts with 0 and
   * accumulates inputs by `add`.
   */
  def longAccumulator(implicit trace: Trace): Task[LongAccumulator] = action(_.longAccumulator)

  /**
   * Create and register a long accumulator, which starts with 0 and
   * accumulates inputs by `add`.
   */
  def longAccumulator(name: => String)(implicit trace: Trace): Task[LongAccumulator] = action(_.longAccumulator(name))

  /**
   * Smarter version of `newApiHadoopFile` that uses class tags to
   * figure out the classes of keys, values and the
   * `org.apache.hadoop.mapreduce.InputFormat` (new MapReduce API) so
   * that user don't need to pass them directly. Instead, callers can
   * just write, for example:
   * ```
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * ```
   *
   * @note
   *   Because Hadoop's RecordReader class re-uses the same Writable
   *   object for each record, directly caching the returned RDD or
   *   directly passing it to an aggregation or shuffle operation will
   *   create many references to the same object. If you plan to
   *   directly cache, sort, or aggregate Hadoop writable objects, you
   *   should first copy them using a `map` function.
   * @param path
   *   directory to the input data files, the path can be comma
   *   separated paths as a list of inputs
   * @return
   *   RDD of tuples of key and corresponding value
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      path: => String
  )(implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F], trace: Trace): Task[RDD[(K, V)]] =
    action(_.newAPIHadoopFile[K, V, F](path))

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API
   * InputFormat and extra configuration options to pass to the input
   * format.
   *
   * @note
   *   Because Hadoop's RecordReader class re-uses the same Writable
   *   object for each record, directly caching the returned RDD or
   *   directly passing it to an aggregation or shuffle operation will
   *   create many references to the same object. If you plan to
   *   directly cache, sort, or aggregate Hadoop writable objects, you
   *   should first copy them using a `map` function.
   * @param path
   *   directory to the input data files, the path can be comma
   *   separated paths as a list of inputs
   * @param fClass
   *   storage format of the data to be read
   * @param kClass
   *   `Class` of the key associated with the `fClass` parameter
   * @param vClass
   *   `Class` of the value associated with the `fClass` parameter
   * @param conf
   *   Hadoop configuration
   * @return
   *   RDD of tuples of key and corresponding value
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      path: => String,
      fClass: => Class[F],
      kClass: => Class[K],
      vClass: => Class[V],
      conf: => Configuration = hadoopConfiguration
  )(implicit trace: Trace): Task[RDD[(K, V)]] = action(_.newAPIHadoopFile[K, V, F](path, fClass, kClass, vClass, conf))

  // Methods for creating RDDs
  /**
   * Distribute a local Scala collection to form an RDD.
   *
   * @note
   *   Parallelize acts lazily. If `seq` is a mutable collection and is
   *   altered after the call to parallelize and before the first action
   *   on the RDD, the resultant RDD will reflect the modified
   *   collection. Pass a copy of the argument to avoid this.
   * @note
   *   avoid using `parallelize(Seq())` to create an empty `RDD`.
   *   Consider `emptyRDD` for an RDD with no partitions, or
   *   `parallelize(Seq[T]())` for an RDD of `T` with empty partitions.
   * @param seq
   *   Scala collection to distribute
   * @param numSlices
   *   number of partitions to divide the collection into
   * @return
   *   RDD representing distributed collection
   */
  def parallelize[T: ClassTag](seq: => Seq[T], numSlices: => Int = defaultParallelism)(implicit
      trace: Trace
  ): Task[RDD[T]] = action(_.parallelize[T](seq, numSlices))

  // Methods for creating shared variables
  /**
   * Register the given accumulator.
   *
   * @note
   *   Accumulators must be registered before use, or it will throw
   *   exception.
   */
  def register(acc: => AccumulatorV2[_, _])(implicit trace: Trace): Task[Unit] = action(_.register(acc))

  /**
   * Register the given accumulator with given name.
   *
   * @note
   *   Accumulators must be registered before use, or it will throw
   *   exception.
   */
  def register(acc: => AccumulatorV2[_, _], name: => String)(implicit trace: Trace): Task[Unit] =
    action(_.register(acc, name))

  /**
   * Remove a tag previously added to be assigned to all the jobs
   * started by this thread. Noop if such a tag was not added earlier.
   *
   * @param tag
   *   The tag to be removed. Cannot contain ',' (comma) character.
   *
   * @since 3.5.0
   */
  def removeJobTag(tag: => String)(implicit trace: Trace): Task[Unit] = action(_.removeJobTag(tag))

  def resources(implicit trace: Trace): Task[Map[String, ResourceInformation]] = action(_.resources)

  /**
   * Run a function on a given set of partitions in an RDD and pass the
   * results to the given handler function. This is the main entry point
   * for all actions in Spark.
   *
   * @param rdd
   *   target RDD to run tasks on
   * @param func
   *   a function to run on each partition of the RDD
   * @param partitions
   *   set of partitions to run on; some jobs may not want to compute on
   *   all partitions of the target RDD, e.g. for operations like
   *   `first()`
   * @param resultHandler
   *   callback to pass each result to
   */
  def runJob[T, U: ClassTag](
      rdd: => RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: => Seq[Int],
      resultHandler: (Int, U) => Unit
  )(implicit trace: Trace): Task[Unit] = action(_.runJob[T, U](rdd.underlying, func, partitions, resultHandler))

  /**
   * Run a function on a given set of partitions in an RDD and return
   * the results as an array. The function that is run against each
   * partition additionally takes `TaskContext` argument.
   *
   * @param rdd
   *   target RDD to run tasks on
   * @param func
   *   a function to run on each partition of the RDD
   * @param partitions
   *   set of partitions to run on; some jobs may not want to compute on
   *   all partitions of the target RDD, e.g. for operations like
   *   `first()`
   * @return
   *   in-memory collection with a result of the job (each collection
   *   element will contain a result from one partition)
   */
  def runJob[T, U: ClassTag](rdd: => RDD[T], func: (TaskContext, Iterator[T]) => U, partitions: => Seq[Int])(implicit
      trace: Trace
  ): Task[Seq[U]] = action(_.runJob[T, U](rdd.underlying, func, partitions).toSeq)

  /**
   * Run a function on a given set of partitions in an RDD and return
   * the results as an array.
   *
   * @param rdd
   *   target RDD to run tasks on
   * @param func
   *   a function to run on each partition of the RDD
   * @param partitions
   *   set of partitions to run on; some jobs may not want to compute on
   *   all partitions of the target RDD, e.g. for operations like
   *   `first()`
   * @return
   *   in-memory collection with a result of the job (each collection
   *   element will contain a result from one partition)
   */
  def runJob[T, U: ClassTag](rdd: => RDD[T], func: Iterator[T] => U, partitions: => Seq[Int])(implicit
      trace: Trace
  ): Task[Seq[U]] = action(_.runJob[T, U](rdd.underlying, func, partitions).toSeq)

  /**
   * Run a job on all partitions in an RDD and return the results in an
   * array. The function that is run against each partition additionally
   * takes `TaskContext` argument.
   *
   * @param rdd
   *   target RDD to run tasks on
   * @param func
   *   a function to run on each partition of the RDD
   * @return
   *   in-memory collection with a result of the job (each collection
   *   element will contain a result from one partition)
   */
  def runJob[T, U: ClassTag](rdd: => RDD[T], func: (TaskContext, Iterator[T]) => U)(implicit
      trace: Trace
  ): Task[Seq[U]] = action(_.runJob[T, U](rdd.underlying, func).toSeq)

  /**
   * Run a job on all partitions in an RDD and return the results in an
   * array.
   *
   * @param rdd
   *   target RDD to run tasks on
   * @param func
   *   a function to run on each partition of the RDD
   * @return
   *   in-memory collection with a result of the job (each collection
   *   element will contain a result from one partition)
   */
  def runJob[T, U: ClassTag](rdd: => RDD[T], func: Iterator[T] => U)(implicit trace: Trace): Task[Seq[U]] =
    action(_.runJob[T, U](rdd.underlying, func).toSeq)

  /**
   * Run a job on all partitions in an RDD and pass the results to a
   * handler function. The function that is run against each partition
   * additionally takes `TaskContext` argument.
   *
   * @param rdd
   *   target RDD to run tasks on
   * @param processPartition
   *   a function to run on each partition of the RDD
   * @param resultHandler
   *   callback to pass each result to
   */
  def runJob[T, U: ClassTag](
      rdd: => RDD[T],
      processPartition: (TaskContext, Iterator[T]) => U,
      resultHandler: (Int, U) => Unit
  )(implicit trace: Trace): Task[Unit] = action(_.runJob[T, U](rdd.underlying, processPartition, resultHandler))

  /**
   * Run a job on all partitions in an RDD and pass the results to a
   * handler function.
   *
   * @param rdd
   *   target RDD to run tasks on
   * @param processPartition
   *   a function to run on each partition of the RDD
   * @param resultHandler
   *   callback to pass each result to
   */
  def runJob[T, U: ClassTag](rdd: => RDD[T], processPartition: Iterator[T] => U, resultHandler: (Int, U) => Unit)(
      implicit trace: Trace
  ): Task[Unit] = action(_.runJob[T, U](rdd.underlying, processPartition, resultHandler))

  /**
   * Set the thread-local property for overriding the call sites of
   * actions and RDDs.
   */
  def setCallSite(shortCallSite: => String)(implicit trace: Trace): Task[Unit] = action(_.setCallSite(shortCallSite))

  /**
   * Set the directory under which RDDs are going to be checkpointed.
   * @param directory
   *   path to the directory where checkpoint files will be stored (must
   *   be HDFS path if running in cluster)
   */
  def setCheckpointDir(directory: => String)(implicit trace: Trace): Task[Unit] = action(_.setCheckpointDir(directory))

  /**
   * Set the behavior of job cancellation from jobs started in this
   * thread.
   *
   * @param interruptOnCancel
   *   If true, then job cancellation will result in
   *   `Thread.interrupt()` being called on the job's executor threads.
   *   This is useful to help ensure that the tasks are actually stopped
   *   in a timely manner, but is off by default due to HDFS-1208, where
   *   HDFS may respond to Thread.interrupt() by marking nodes as dead.
   *
   * @since 3.5.0
   */
  def setInterruptOnCancel(interruptOnCancel: => Boolean)(implicit trace: Trace): Task[Unit] =
    action(_.setInterruptOnCancel(interruptOnCancel))

  /** Set a human readable description of the current job. */
  def setJobDescription(value: => String)(implicit trace: Trace): Task[Unit] = action(_.setJobDescription(value))

  /**
   * Assigns a group ID to all the jobs started by this thread until the
   * group ID is set to a different value or cleared.
   *
   * Often, a unit of execution in an application consists of multiple
   * Spark actions or jobs. Application programmers can use this method
   * to group all those jobs together and give a group description. Once
   * set, the Spark web UI will associate such jobs with this group.
   *
   * The application can also use
   * `org.apache.spark.SparkContext.cancelJobGroup` to cancel all
   * running jobs in this group. For example,
   * {{{
   * // In the main thread:
   * sc.setJobGroup("some_job_to_cancel", "some job description")
   * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
   *
   * // In a separate thread:
   * sc.cancelJobGroup("some_job_to_cancel")
   * }}}
   *
   * @param interruptOnCancel
   *   If true, then job cancellation will result in
   *   `Thread.interrupt()` being called on the job's executor threads.
   *   This is useful to help ensure that the tasks are actually stopped
   *   in a timely manner, but is off by default due to HDFS-1208, where
   *   HDFS may respond to Thread.interrupt() by marking nodes as dead.
   */
  def setJobGroup(groupId: => String, description: => String, interruptOnCancel: => Boolean = false)(implicit
      trace: Trace
  ): Task[Unit] = action(_.setJobGroup(groupId, description, interruptOnCancel))

  /**
   * Set a local property that affects jobs submitted from this thread,
   * such as the Spark fair scheduler pool. User-defined properties may
   * also be set here. These properties are propagated through to worker
   * tasks and can be accessed there via
   * [[org.apache.spark.TaskContext#getLocalProperty]].
   *
   * These properties are inherited by child threads spawned from this
   * thread. This may have unexpected consequences when working with
   * thread pools. The standard java implementation of thread pools have
   * worker threads spawn other worker threads. As a result, local
   * properties may propagate unpredictably.
   */
  def setLocalProperty(key: => String, value: => String)(implicit trace: Trace): Task[Unit] =
    action(_.setLocalProperty(key, value))

  /* ------------------------------------------------------------------------------------- * Initialization. This code
   * initializes the context in a manner that is exception-safe. | All internal fields holding state are initialized
   * here, and any error prompts the | stop() method to be called. |
   * ------------------------------------------------------------------------------------- */
  /**
   * Control our logLevel. This overrides any user-defined log settings.
   * @param logLevel
   *   The desired log level as a string. Valid log levels include: ALL,
   *   DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
   */
  def setLogLevel(logLevel: => String)(implicit trace: Trace): Task[Unit] = action(_.setLogLevel(logLevel))

  /** Shut down the SparkContext. */
  def stop(implicit trace: Trace): Task[Unit] = action(_.stop())

  /**
   * Shut down the SparkContext with exit code that will passed to
   * scheduler backend. In client mode, client side may call
   * `SparkContext.stop()` to clean up but exit with code not equal to
   * 0. This behavior cause resource scheduler such as
   * `ApplicationMaster` exit with success status but client side exited
   * with failed status. Spark can call this method to stop SparkContext
   * and pass client side correct exit code to scheduler backend. Then
   * scheduler backend should send the exit code to corresponding
   * resource scheduler to keep consistent.
   *
   * @param exitCode
   *   Specified exit code that will passed to scheduler backend in
   *   client mode.
   */
  def stop(exitCode: => Int)(implicit trace: Trace): Task[Unit] = action(_.stop(exitCode))

  /**
   * Submit a job for execution and return a FutureJob holding the
   * result.
   *
   * @param rdd
   *   target RDD to run tasks on
   * @param processPartition
   *   a function to run on each partition of the RDD
   * @param partitions
   *   set of partitions to run on; some jobs may not want to compute on
   *   all partitions of the target RDD, e.g. for operations like
   *   `first()`
   * @param resultHandler
   *   callback to pass each result to
   * @param resultFunc
   *   function to be executed when the result is ready
   */
  def submitJob[T, U, R](
      rdd: => RDD[T],
      processPartition: Iterator[T] => U,
      partitions: => Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R
  )(implicit trace: Trace): Task[SimpleFutureAction[R]] =
    action(_.submitJob[T, U, R](rdd.underlying, processPartition, partitions, resultHandler, resultFunc))

  /**
   * Read a text file from HDFS, a local file system (available on all
   * nodes), or any Hadoop-supported file system URI, and return it as
   * an RDD of Strings. The text files must be encoded as UTF-8.
   *
   * @param path
   *   path to the text file on a supported file system
   * @param minPartitions
   *   suggested minimum number of partitions for the resulting RDD
   * @return
   *   RDD of lines of the text file
   */
  def textFile(path: => String, minPartitions: => Int = defaultMinPartitions)(implicit
      trace: Trace
  ): Task[RDD[String]] = action(_.textFile(path, minPartitions))

  // ===============

  // Methods with handmade implementations
  //
  // [[org.apache.spark.SparkContext.getPersistentRDDs]]

  // ===============

  // Ignored methods
  //
  // [[org.apache.spark.SparkContext.sequenceFile]]
}
