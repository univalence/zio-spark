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

  def getPersistentRDDs: Task[Map[Int, RDD[_]]] =
    ZIO.attempt(
      self.underlying.getPersistentRDDs.view.mapValues(rdd => RDD(rdd)).toMap
    )

  // Generated functions coming from spark

  def appName: String = get(_.appName)

  def applicationAttemptId: Option[String] = get(_.applicationAttemptId)

  def applicationId: String = get(_.applicationId)

  def defaultMinPartitions: Int = get(_.defaultMinPartitions)

  def defaultParallelism: Int = get(_.defaultParallelism)

  def deployMode: String = get(_.deployMode)

  def emptyRDD[T: ClassTag]: RDD[T] = get(_.emptyRDD[T])

  def files: Seq[String] = get(_.files)

  def getConf: SparkConf = get(_.getConf)

  def hadoopConfiguration: Configuration = get(_.hadoopConfiguration)

  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions
  ): RDD[(K, V)] = get(_.hadoopFile[K, V](path, inputFormatClass, keyClass, valueClass, minPartitions))

  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String, minPartitions: Int)(implicit
      km: ClassTag[K],
      vm: ClassTag[V],
      fm: ClassTag[F]
  ): RDD[(K, V)] = get(_.hadoopFile[K, V, F](path, minPartitions))

  def hadoopFile[K, V, F <: InputFormat[K, V]](
      path: String
  )(implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = get(_.hadoopFile[K, V, F](path))

  def isLocal: Boolean = get(_.isLocal)

  def jars: Seq[String] = get(_.jars)

  def makeRDD[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = get(_.makeRDD[T](seq, numSlices))

  def makeRDD[T: ClassTag](seq: Seq[(T, Seq[String])]): RDD[T] = get(_.makeRDD[T](seq))

  def master: String = get(_.master)

  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      conf: Configuration = hadoopConfiguration,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V]
  ): RDD[(K, V)] = get(_.newAPIHadoopRDD[K, V, F](conf, fClass, kClass, vClass))

  def objectFile[T: ClassTag](path: String, minPartitions: Int = defaultMinPartitions): RDD[T] =
    get(_.objectFile[T](path, minPartitions))

  def range(start: Long, end: Long, step: Long = 1, numSlices: Int = defaultParallelism): RDD[Long] =
    get(_.range(start, end, step, numSlices))

  def statusTracker: SparkStatusTracker = get(_.statusTracker)

  def uiWebUrl: Option[String] = get(_.uiWebUrl)

  def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = get(_.union[T](rdds.map(_.underlying)))

  def union[T: ClassTag](first: RDD[T], rest: RDD[T]*): RDD[T] =
    get(_.union[T](first.underlying, rest.map(_.underlying): _*))

  def version: String = get(_.version)

  def wholeTextFiles(path: String, minPartitions: Int = defaultMinPartitions): RDD[(String, String)] =
    get(_.wholeTextFiles(path, minPartitions))

  // ===============

  def addArchive(path: => String)(implicit trace: Trace): Task[Unit] = action(_.addArchive(path))

  def addFile(path: => String)(implicit trace: Trace): Task[Unit] = action(_.addFile(path))

  def addFile(path: => String, recursive: => Boolean)(implicit trace: Trace): Task[Unit] =
    action(_.addFile(path, recursive))

  def addJar(path: => String)(implicit trace: Trace): Task[Unit] = action(_.addJar(path))

  def addJobTag(tag: => String)(implicit trace: Trace): Task[Unit] = action(_.addJobTag(tag))

  def archives(implicit trace: Trace): Task[Seq[String]] = action(_.archives)

  def binaryFiles(path: => String, minPartitions: => Int = defaultMinPartitions)(implicit
      trace: Trace
  ): Task[RDD[(String, PortableDataStream)]] = action(_.binaryFiles(path, minPartitions))

  def binaryRecords(path: => String, recordLength: => Int, conf: => Configuration = hadoopConfiguration)(implicit
      trace: Trace
  ): Task[RDD[Seq[Byte]]] = action(_.binaryRecords(path, recordLength, conf).map(_.toSeq))

  def broadcast[T: ClassTag](value: => T)(implicit trace: Trace): Task[Broadcast[T]] = action(_.broadcast[T](value))

  def cancelAllJobs(implicit trace: Trace): Task[Unit] = action(_.cancelAllJobs())

  def cancelJob(jobId: => Int, reason: => String)(implicit trace: Trace): Task[Unit] =
    action(_.cancelJob(jobId, reason))

  def cancelJob(jobId: => Int)(implicit trace: Trace): Task[Unit] = action(_.cancelJob(jobId))

  def cancelJobGroup(groupId: => String)(implicit trace: Trace): Task[Unit] = action(_.cancelJobGroup(groupId))

  def cancelJobsWithTag(tag: => String)(implicit trace: Trace): Task[Unit] = action(_.cancelJobsWithTag(tag))

  def cancelStage(stageId: => Int, reason: => String)(implicit trace: Trace): Task[Unit] =
    action(_.cancelStage(stageId, reason))

  def cancelStage(stageId: => Int)(implicit trace: Trace): Task[Unit] = action(_.cancelStage(stageId))

  def clearCallSite(implicit trace: Trace): Task[Unit] = action(_.clearCallSite())

  def clearJobGroup(implicit trace: Trace): Task[Unit] = action(_.clearJobGroup())

  def clearJobTags(implicit trace: Trace): Task[Unit] = action(_.clearJobTags())

  def collectionAccumulator[T](implicit trace: Trace): Task[CollectionAccumulator[T]] =
    action(_.collectionAccumulator[T])

  def collectionAccumulator[T](name: => String)(implicit trace: Trace): Task[CollectionAccumulator[T]] =
    action(_.collectionAccumulator[T](name))

  def doubleAccumulator(implicit trace: Trace): Task[DoubleAccumulator] = action(_.doubleAccumulator)

  def doubleAccumulator(name: => String)(implicit trace: Trace): Task[DoubleAccumulator] =
    action(_.doubleAccumulator(name))

  def getCheckpointDir(implicit trace: Trace): Task[Option[String]] = action(_.getCheckpointDir)

  def getExecutorMemoryStatus(implicit trace: Trace): Task[Map[String, (Long, Long)]] =
    action(_.getExecutorMemoryStatus)

  def getJobTags(implicit trace: Trace): Task[Set[String]] = action(_.getJobTags())

  def getLocalProperty(key: => String)(implicit trace: Trace): Task[String] = action(_.getLocalProperty(key))

  def getSchedulingMode(implicit trace: Trace): Task[SchedulingMode] = action(_.getSchedulingMode)

  def hadoopRDD[K, V](
      conf: => JobConf,
      inputFormatClass: => Class[_ <: InputFormat[K, V]],
      keyClass: => Class[K],
      valueClass: => Class[V],
      minPartitions: => Int = defaultMinPartitions
  )(implicit trace: Trace): Task[RDD[(K, V)]] =
    action(_.hadoopRDD[K, V](conf, inputFormatClass, keyClass, valueClass, minPartitions))

  def isStopped(implicit trace: Trace): Task[Boolean] = action(_.isStopped)

  def killTaskAttempt(
      taskId: => Long,
      interruptThread: => Boolean = true,
      reason: => String = "killed via SparkContext.killTaskAttempt"
  )(implicit trace: Trace): Task[Boolean] = action(_.killTaskAttempt(taskId, interruptThread, reason))

  def listArchives(implicit trace: Trace): Task[Seq[String]] = action(_.listArchives())

  def listFiles(implicit trace: Trace): Task[Seq[String]] = action(_.listFiles())

  def listJars(implicit trace: Trace): Task[Seq[String]] = action(_.listJars())

  def longAccumulator(implicit trace: Trace): Task[LongAccumulator] = action(_.longAccumulator)

  def longAccumulator(name: => String)(implicit trace: Trace): Task[LongAccumulator] = action(_.longAccumulator(name))

  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      path: => String
  )(implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F], trace: Trace): Task[RDD[(K, V)]] =
    action(_.newAPIHadoopFile[K, V, F](path))

  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      path: => String,
      fClass: => Class[F],
      kClass: => Class[K],
      vClass: => Class[V],
      conf: => Configuration = hadoopConfiguration
  )(implicit trace: Trace): Task[RDD[(K, V)]] = action(_.newAPIHadoopFile[K, V, F](path, fClass, kClass, vClass, conf))

  def parallelize[T: ClassTag](seq: => Seq[T], numSlices: => Int = defaultParallelism)(implicit
      trace: Trace
  ): Task[RDD[T]] = action(_.parallelize[T](seq, numSlices))

  def register(acc: => AccumulatorV2[_, _])(implicit trace: Trace): Task[Unit] = action(_.register(acc))

  def register(acc: => AccumulatorV2[_, _], name: => String)(implicit trace: Trace): Task[Unit] =
    action(_.register(acc, name))

  def removeJobTag(tag: => String)(implicit trace: Trace): Task[Unit] = action(_.removeJobTag(tag))

  def resources(implicit trace: Trace): Task[Map[String, ResourceInformation]] = action(_.resources)

  def runJob[T, U: ClassTag](
      rdd: => RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: => Seq[Int],
      resultHandler: (Int, U) => Unit
  )(implicit trace: Trace): Task[Unit] = action(_.runJob[T, U](rdd.underlying, func, partitions, resultHandler))

  def runJob[T, U: ClassTag](rdd: => RDD[T], func: (TaskContext, Iterator[T]) => U, partitions: => Seq[Int])(implicit
      trace: Trace
  ): Task[Seq[U]] = action(_.runJob[T, U](rdd.underlying, func, partitions).toSeq)

  def runJob[T, U: ClassTag](rdd: => RDD[T], func: Iterator[T] => U, partitions: => Seq[Int])(implicit
      trace: Trace
  ): Task[Seq[U]] = action(_.runJob[T, U](rdd.underlying, func, partitions).toSeq)

  def runJob[T, U: ClassTag](rdd: => RDD[T], func: (TaskContext, Iterator[T]) => U)(implicit
      trace: Trace
  ): Task[Seq[U]] = action(_.runJob[T, U](rdd.underlying, func).toSeq)

  def runJob[T, U: ClassTag](rdd: => RDD[T], func: Iterator[T] => U)(implicit trace: Trace): Task[Seq[U]] =
    action(_.runJob[T, U](rdd.underlying, func).toSeq)

  def runJob[T, U: ClassTag](
      rdd: => RDD[T],
      processPartition: (TaskContext, Iterator[T]) => U,
      resultHandler: (Int, U) => Unit
  )(implicit trace: Trace): Task[Unit] = action(_.runJob[T, U](rdd.underlying, processPartition, resultHandler))

  def runJob[T, U: ClassTag](rdd: => RDD[T], processPartition: Iterator[T] => U, resultHandler: (Int, U) => Unit)(
      implicit trace: Trace
  ): Task[Unit] = action(_.runJob[T, U](rdd.underlying, processPartition, resultHandler))

  def setCallSite(shortCallSite: => String)(implicit trace: Trace): Task[Unit] = action(_.setCallSite(shortCallSite))

  def setCheckpointDir(directory: => String)(implicit trace: Trace): Task[Unit] = action(_.setCheckpointDir(directory))

  def setInterruptOnCancel(interruptOnCancel: => Boolean)(implicit trace: Trace): Task[Unit] =
    action(_.setInterruptOnCancel(interruptOnCancel))

  def setJobDescription(value: => String)(implicit trace: Trace): Task[Unit] = action(_.setJobDescription(value))

  def setJobGroup(groupId: => String, description: => String, interruptOnCancel: => Boolean = false)(implicit
      trace: Trace
  ): Task[Unit] = action(_.setJobGroup(groupId, description, interruptOnCancel))

  def setLocalProperty(key: => String, value: => String)(implicit trace: Trace): Task[Unit] =
    action(_.setLocalProperty(key, value))

  def setLogLevel(logLevel: => String)(implicit trace: Trace): Task[Unit] = action(_.setLogLevel(logLevel))

  def stop(implicit trace: Trace): Task[Unit] = action(_.stop())

  def stop(exitCode: => Int)(implicit trace: Trace): Task[Unit] = action(_.stop(exitCode))

  def submitJob[T, U, R](
      rdd: => RDD[T],
      processPartition: Iterator[T] => U,
      partitions: => Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R
  )(implicit trace: Trace): Task[SimpleFutureAction[R]] =
    action(_.submitJob[T, U, R](rdd.underlying, processPartition, partitions, resultHandler, resultFunc))

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
